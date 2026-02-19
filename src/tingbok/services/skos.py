"""SKOS lookup service — wraps upstream AGROVOC, DBpedia, Wikidata sources.

Cache format is compatible with inventory-md's skos.py cache so that the
/var/cache/tingbok/skos/ directory can be pre-seeded from an existing
inventory-md SKOS cache.
"""

import hashlib
import json
import logging
import time
from pathlib import Path

import httpx

logger = logging.getLogger(__name__)

CACHE_TTL_SECONDS = 60 * 60 * 24 * 60  # 60 days — matches inventory-md
DEFAULT_TIMEOUT = 10.0

_REST_ENDPOINTS: dict[str, str] = {
    "agrovoc": "https://agrovoc.fao.org/browse/rest/v1",
    "dbpedia": "https://lookup.dbpedia.org/api",
    "wikidata": "https://www.wikidata.org",
}


# ---------------------------------------------------------------------------
# Cache utilities (format-compatible with inventory-md skos.py)
# ---------------------------------------------------------------------------


def _get_cache_path(cache_dir: Path, key: str) -> Path:
    """Return the cache file path for a lookup key."""
    key_hash = hashlib.sha256(key.encode()).hexdigest()[:16]
    safe_key = "".join(c if c.isalnum() else "_" for c in key[:50])
    return cache_dir / f"{safe_key}_{key_hash}.json"


def _get_not_found_cache_path(cache_dir: Path) -> Path:
    return cache_dir / "_not_found.json"


def _load_from_cache(cache_path: Path, ttl: int = CACHE_TTL_SECONDS) -> dict | None:
    """Load cached data if it exists and has not expired."""
    if not cache_path.exists():
        return None
    try:
        with open(cache_path, encoding="utf-8") as f:
            data: dict = json.load(f)
        if time.time() - data.get("_cached_at", 0) > ttl:
            return None
        return data
    except (json.JSONDecodeError, OSError) as e:
        logger.debug("Cache read failed for %s: %s", cache_path, e)
        return None


def _save_to_cache(cache_path: Path, data: dict) -> None:
    """Save data to a cache file, stamping _cached_at."""
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {**data, "_cached_at": time.time()}
    try:
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except OSError as e:
        logger.warning("Cache write failed for %s: %s", cache_path, e)


def _is_in_not_found_cache(cache_dir: Path, key: str, ttl: int = CACHE_TTL_SECONDS) -> bool:
    """Return True if *key* is present (and not expired) in the not-found cache."""
    cache_path = _get_not_found_cache_path(cache_dir)
    if not cache_path.exists():
        return False
    try:
        with open(cache_path, encoding="utf-8") as f:
            data: dict = json.load(f)
        entry = data.get("entries", {}).get(key)
        if entry is None:
            return False
        return time.time() - entry.get("cached_at", 0) <= ttl
    except (json.JSONDecodeError, OSError) as e:
        logger.debug("Not-found cache read failed: %s", e)
        return False


def _add_to_not_found_cache(cache_dir: Path, key: str) -> None:
    """Add *key* to the consolidated not-found cache file."""
    cache_path = _get_not_found_cache_path(cache_dir)
    cache_dir.mkdir(parents=True, exist_ok=True)
    data: dict = {"entries": {}}
    if cache_path.exists():
        try:
            with open(cache_path, encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError):
            data = {"entries": {}}
    data.setdefault("entries", {})[key] = {"cached_at": time.time()}
    try:
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except OSError as e:
        logger.warning("Not-found cache write failed: %s", e)


def _broader_to_uris(broader: list) -> list[str]:
    """Normalise the broader list to a plain list of URI strings.

    Cache files from inventory-md store broader as ``list[dict]`` with
    ``{"uri": ..., "label": ...}``; older entries may be plain strings.
    """
    result: list[str] = []
    for item in broader:
        if isinstance(item, str) and item:
            result.append(item)
        elif isinstance(item, dict):
            uri = item.get("uri", "")
            if uri:
                result.append(uri)
    return result


# ---------------------------------------------------------------------------
# Public service functions
# ---------------------------------------------------------------------------


def lookup_concept(label: str, lang: str, source: str, cache_dir: Path) -> dict | None:
    """Look up a concept by label, serving from cache when possible.

    Returns a concept dict (compatible with inventory-md cache format) or
    ``None`` if the concept was not found.  Upstream queries are only made
    when neither the positive cache nor the not-found cache has an entry.

    Args:
        label:     Human-readable label to search for (e.g. ``"potatoes"``).
        lang:      BCP-47 language code (e.g. ``"en"``, ``"nb"``).
        source:    Taxonomy source: ``"agrovoc"``, ``"dbpedia"``, or ``"wikidata"``.
        cache_dir: Path to the SKOS cache directory.

    Returns:
        Concept dict or ``None``.
    """
    cache_key = f"concept:{source}:{lang}:{label.lower()}"
    cache_path = _get_cache_path(cache_dir, cache_key)

    # 1. Positive cache
    cached = _load_from_cache(cache_path)
    if cached is not None and cached.get("uri"):
        return cached

    # 2. Not-found cache
    if _is_in_not_found_cache(cache_dir, cache_key):
        return None

    # 3. Upstream query
    concept, query_failed = _upstream_lookup(label, lang, source)

    if not query_failed:
        if concept:
            _save_to_cache(cache_path, concept)
        else:
            _add_to_not_found_cache(cache_dir, cache_key)

    return concept


def get_labels(uri: str, languages: list[str], source: str, cache_dir: Path) -> dict[str, str]:
    """Fetch labels for a concept URI in the requested languages.

    Serves from cache when available; falls back to upstream REST APIs.

    Args:
        uri:       Full SKOS concept URI.
        languages: List of BCP-47 language codes to return.
        source:    Taxonomy source: ``"agrovoc"``, ``"dbpedia"``, or ``"wikidata"``.
        cache_dir: Path to the SKOS cache directory.

    Returns:
        Dict mapping language codes to label strings.
    """
    if not uri or not languages:
        return {}

    uri_hash = hashlib.md5(uri.encode()).hexdigest()[:16]  # noqa: S324 — non-crypto use
    cache_key = f"labels:{source}:{uri_hash}"
    cache_path = _get_cache_path(cache_dir, cache_key)

    cached = _load_from_cache(cache_path)
    if cached is not None:
        cached_labels: dict[str, str] = cached.get("labels", {})
        return {lang: cached_labels[lang] for lang in languages if lang in cached_labels}

    all_labels = _upstream_get_labels(uri, source, languages)
    if all_labels:
        _save_to_cache(cache_path, {"uri": uri, "source": source, "labels": all_labels})

    return {lang: all_labels[lang] for lang in languages if lang in all_labels}


# ---------------------------------------------------------------------------
# Upstream REST lookups
# ---------------------------------------------------------------------------


def _upstream_lookup(label: str, lang: str, source: str) -> tuple[dict | None, bool]:
    """Dispatch to the appropriate upstream REST source.

    Returns ``(concept_or_None, query_failed)``.  ``query_failed=True``
    indicates a transient error (network timeout, 5xx); the result must not
    be added to the not-found cache.
    """
    if source == "agrovoc":
        return _lookup_agrovoc(label, lang)
    if source == "dbpedia":
        return _lookup_dbpedia(label, lang)
    if source == "wikidata":
        return _lookup_wikidata(label, lang)
    logger.warning("Unknown SKOS source: %s", source)
    return None, True


def _lookup_agrovoc(label: str, lang: str) -> tuple[dict | None, bool]:
    """Look up a concept in AGROVOC via the Skosmos REST API."""
    rest_base = _REST_ENDPOINTS["agrovoc"]
    url = f"{rest_base}/search/"
    params = {"query": label, "lang": lang}
    try:
        with httpx.Client(timeout=DEFAULT_TIMEOUT) as client:
            response = client.get(url, params=params)
            response.raise_for_status()
            data: dict = response.json()
    except httpx.TimeoutException as e:
        logger.warning("AGROVOC search timed out for '%s': %s", label, e)
        return None, True
    except httpx.HTTPError as e:
        logger.warning("AGROVOC search failed for '%s': %s", label, e)
        return None, True

    results: list[dict] = data.get("results", [])
    if not results:
        return None, False

    label_lower = label.lower()
    best_match = None
    for result in results:
        pref = result.get("prefLabel", "").lower()
        alts = result.get("altLabel", [])
        alt_list = [a.lower() for a in alts] if isinstance(alts, list) else []
        if pref == label_lower or label_lower in alt_list:
            best_match = result
            break
    if best_match is None:
        best_match = results[0]

    concept_uri: str = best_match.get("uri", "")
    if not concept_uri:
        return None, False

    broader = _get_broader_agrovoc(concept_uri, rest_base, lang)
    return {
        "uri": concept_uri,
        "prefLabel": best_match.get("prefLabel", label),
        "source": "agrovoc",
        "broader": broader,
    }, False


def _get_broader_agrovoc(concept_uri: str, rest_base: str, lang: str) -> list[dict]:
    """Retrieve broader concepts for an AGROVOC URI via REST."""
    url = f"{rest_base}/data/"
    params = {"uri": concept_uri}
    try:
        with httpx.Client(timeout=DEFAULT_TIMEOUT) as client:
            response = client.get(url, params=params)
            response.raise_for_status()
            concept_data: dict = response.json()
    except httpx.HTTPError as e:
        logger.warning("AGROVOC data fetch failed for %s: %s", concept_uri, e)
        return []

    broader: list[dict] = []
    graph: list[dict] = concept_data.get("graph", [])
    for item in graph:
        if item.get("uri") != concept_uri:
            continue
        broader_refs = item.get("broader", [])
        if isinstance(broader_refs, str):
            broader_refs = [broader_refs]
        elif isinstance(broader_refs, list):
            broader_refs = [b.get("uri") if isinstance(b, dict) else b for b in broader_refs]
        for broader_uri in broader_refs:
            if not broader_uri:
                continue
            label_val = ""
            for node in graph:
                if node.get("uri") != broader_uri:
                    continue
                pref_labels = node.get("prefLabel", [])
                if isinstance(pref_labels, str):
                    label_val = pref_labels
                elif isinstance(pref_labels, list):
                    for pl in pref_labels:
                        if isinstance(pl, dict) and pl.get("lang") == lang:
                            label_val = pl.get("value", "")
                            break
                    if not label_val and pref_labels:
                        first = pref_labels[0]
                        label_val = first.get("value", "") if isinstance(first, dict) else str(first)
                break
            broader.append({"uri": broader_uri, "label": label_val})
    return broader


def _lookup_dbpedia(label: str, lang: str) -> tuple[dict | None, bool]:
    """Look up a concept in DBpedia via the Lookup REST API."""
    url = f"{_REST_ENDPOINTS['dbpedia']}/search"
    params: dict = {"query": label, "format": "JSON", "maxResults": "5"}
    if lang:
        params["language"] = lang
    try:
        with httpx.Client(timeout=DEFAULT_TIMEOUT) as client:
            response = client.get(url, params=params)
            response.raise_for_status()
            data: dict = response.json()
    except httpx.TimeoutException as e:
        logger.warning("DBpedia lookup timed out for '%s': %s", label, e)
        return None, True
    except httpx.HTTPError as e:
        logger.warning("DBpedia lookup failed for '%s': %s", label, e)
        return None, True

    results: list[dict] = data.get("docs", [])
    if not results:
        return None, False

    label_lower = label.lower()
    best = None
    for result in results:
        raw_labels = result.get("label", [])
        labels = raw_labels if isinstance(raw_labels, list) else [raw_labels]
        if any(lbl.lower() == label_lower for lbl in labels):
            best = result
            break
    if best is None:
        best = results[0]

    resource = best.get("resource", [])
    uri = resource[0] if isinstance(resource, list) and resource else (resource if isinstance(resource, str) else "")
    if not uri:
        return None, False

    raw_labels = best.get("label", [])
    pref_label = raw_labels[0] if isinstance(raw_labels, list) and raw_labels else str(raw_labels)
    comment = best.get("comment", [])
    description: str | None = comment[0] if isinstance(comment, list) and comment else (comment or None)

    return {
        "uri": uri,
        "prefLabel": pref_label,
        "source": "dbpedia",
        "broader": [],
        "description": description,
    }, False


def _lookup_wikidata(label: str, lang: str) -> tuple[dict | None, bool]:
    """Look up a concept in Wikidata via the MediaWiki Action API."""
    url = "https://www.wikidata.org/w/api.php"
    params: dict = {
        "action": "wbsearchentities",
        "search": label,
        "language": lang,
        "type": "item",
        "format": "json",
        "limit": "5",
    }
    try:
        with httpx.Client(timeout=DEFAULT_TIMEOUT) as client:
            response = client.get(url, params=params)
            response.raise_for_status()
            data: dict = response.json()
    except httpx.TimeoutException as e:
        logger.warning("Wikidata lookup timed out for '%s': %s", label, e)
        return None, True
    except httpx.HTTPError as e:
        logger.warning("Wikidata lookup failed for '%s': %s", label, e)
        return None, True

    results: list[dict] = data.get("search", [])
    if not results:
        return None, False

    label_lower = label.lower()
    best = next((r for r in results if r.get("label", "").lower() == label_lower), results[0])

    qid: str = best.get("id", "")
    if not qid:
        return None, False
    uri = f"http://www.wikidata.org/entity/{qid}"

    return {
        "uri": uri,
        "prefLabel": best.get("label", label),
        "source": "wikidata",
        "broader": [],
        "description": best.get("description"),
    }, False


def _upstream_get_labels(uri: str, source: str, languages: list[str]) -> dict[str, str]:
    """Fetch multilingual labels for a URI from the appropriate upstream source."""
    if source == "agrovoc":
        return _get_agrovoc_labels(uri, languages)
    if source == "dbpedia":
        return _get_dbpedia_labels(uri, languages)
    if source == "wikidata":
        return _get_wikidata_labels(uri, languages)
    return {}


def _get_agrovoc_labels(uri: str, languages: list[str]) -> dict[str, str]:
    """Fetch multilingual labels for an AGROVOC URI via REST."""
    rest_base = _REST_ENDPOINTS["agrovoc"]
    url = f"{rest_base}/data/"
    params = {"uri": uri}
    try:
        with httpx.Client(timeout=DEFAULT_TIMEOUT) as client:
            response = client.get(url, params=params)
            response.raise_for_status()
            data: dict = response.json()
    except httpx.HTTPError as e:
        logger.warning("AGROVOC data fetch failed for %s: %s", uri, e)
        return {}

    labels: dict[str, str] = {}
    for item in data.get("graph", []):
        if item.get("uri") != uri:
            continue
        for pl in item.get("prefLabel", []):
            if isinstance(pl, dict):
                lang = pl.get("lang", "")
                value = pl.get("value", "")
                if lang in languages and value:
                    labels[lang] = value
    return labels


def _get_dbpedia_labels(uri: str, languages: list[str]) -> dict[str, str]:
    """Fetch multilingual labels for a DBpedia URI via the Data REST API."""
    data_uri = uri.replace("http://dbpedia.org/resource/", "https://dbpedia.org/data/") + ".json"
    try:
        with httpx.Client(timeout=DEFAULT_TIMEOUT, follow_redirects=True) as client:
            response = client.get(data_uri)
            response.raise_for_status()
            data: dict = response.json()
    except httpx.HTTPError as e:
        logger.warning("DBpedia data fetch failed for %s: %s", uri, e)
        return {}

    labels: dict[str, str] = {}
    resource_data = data.get(uri, {})
    for entry in resource_data.get("http://www.w3.org/2000/01/rdf-schema#label", []):
        lang = entry.get("lang", "")
        value = entry.get("value", "")
        if lang in languages and value:
            labels[lang] = value
    return labels


def _get_wikidata_labels(uri: str, languages: list[str]) -> dict[str, str]:
    """Fetch multilingual labels for a Wikidata item via the Wikibase REST API."""
    qid = uri.rstrip("/").split("/")[-1]
    if not qid.startswith("Q"):
        return {}
    url = f"https://www.wikidata.org/w/rest.php/wikibase/v0/entities/items/{qid}/labels"
    try:
        with httpx.Client(timeout=DEFAULT_TIMEOUT) as client:
            response = client.get(url)
            response.raise_for_status()
            data: dict = response.json()
    except httpx.HTTPError as e:
        logger.warning("Wikidata labels fetch failed for %s: %s", uri, e)
        return {}
    return {lang: data[lang] for lang in languages if lang in data}
