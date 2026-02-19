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


def _parse_json(response: httpx.Response, context: str = "") -> dict | None:
    """Parse a JSON response, returning None on empty or malformed bodies."""
    try:
        return response.json()
    except ValueError as e:
        logger.debug("Non-JSON response%s: %s", f" for {context}" if context else "", e)
        return None


CACHE_TTL_SECONDS = 60 * 60 * 24 * 60  # 60 days — matches inventory-md
DEFAULT_TIMEOUT = 10.0

_REST_ENDPOINTS: dict[str, str] = {
    "agrovoc": "https://agrovoc.fao.org/browse/rest/v1",
    "dbpedia": "https://lookup.dbpedia.org/api",
    "wikidata": "https://www.wikidata.org",
}

# ---------------------------------------------------------------------------
# Root mapping — synchronised with inventory-md AGROVOC_ROOT_MAPPING
# ---------------------------------------------------------------------------

AGROVOC_ROOT_MAPPING: dict[str, str] = {
    # Products hierarchy → Food
    "products": "food",
    "plant products": "food",
    "animal products": "food",
    "processed products": "food",
    "aquatic products": "food",
    # Keep others as-is but with better labels
    "equipment": "tools",
    "materials": "materials",
    "chemicals": "chemicals",
    "organisms": "organisms",
}

#: Per-source root mapping table.  Add entries for other sources as needed.
ROOT_MAPPING: dict[str, dict[str, str]] = {
    "agrovoc": AGROVOC_ROOT_MAPPING,
    "dbpedia": {},
    "wikidata": {},
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


def _normalize_label(label: str) -> str:
    """Normalise a concept label into a path component (lowercase, underscores)."""
    return label.lower().replace(" ", "_").replace("-", "_")


# ---------------------------------------------------------------------------
# Public service functions
# ---------------------------------------------------------------------------


def lookup_concept(label: str, lang: str, source: str, cache_dir: Path) -> dict | None:
    """Look up a concept by label, serving from cache when possible.

    Returns a concept dict (compatible with inventory-md cache format) or
    ``None`` if the concept was not found.

    Args:
        label:     Human-readable label to search for (e.g. ``"potatoes"``).
        lang:      BCP-47 language code (e.g. ``"en"``, ``"nb"``).
        source:    Taxonomy source: ``"agrovoc"``, ``"dbpedia"``, or ``"wikidata"``.
        cache_dir: Path to the SKOS cache directory.
    """
    cache_key = f"concept:{source}:{lang}:{label.lower()}"
    cache_path = _get_cache_path(cache_dir, cache_key)

    cached = _load_from_cache(cache_path)
    if cached is not None and cached.get("uri"):
        return cached

    if _is_in_not_found_cache(cache_dir, cache_key):
        return None

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


def get_labels_batch(uris: list[str], languages: list[str], source: str, cache_dir: Path) -> dict[str, dict[str, str]]:
    """Fetch labels for multiple URIs in the requested languages.

    Each URI is served from cache independently; uncached URIs trigger one
    upstream REST call each.

    Args:
        uris:      List of SKOS concept URIs.
        languages: List of BCP-47 language codes to return.
        source:    Taxonomy source: ``"agrovoc"``, ``"dbpedia"``, or ``"wikidata"``.
        cache_dir: Path to the SKOS cache directory.

    Returns:
        Dict mapping each URI to a ``{lang: label}`` dict.
    """
    return {uri: get_labels(uri, languages, source, cache_dir) for uri in uris}


def build_hierarchy_paths(
    label: str,
    lang: str,
    source: str,
    cache_dir: Path,
    *,
    _current_path: list[str] | None = None,
    _current_uris: list[str] | None = None,
    _visited_uris: frozenset[str] | None = None,
    _depth: int = 0,
    _max_depth: int = 15,
) -> tuple[list[str], bool, dict[str, str]]:
    """Recursively build full hierarchy paths from a concept to its root(s).

    Follows the same algorithm as inventory-md's ``build_skos_hierarchy_paths``:
    starting from *label*, each broader concept is looked up (cache then
    upstream) and the path is extended until a root is reached.  Root labels
    are normalised via :data:`ROOT_MAPPING`.

    Args:
        label:     Concept label to look up (e.g. ``"potatoes"``).
        lang:      BCP-47 language code.
        source:    Taxonomy source (``"agrovoc"``, ``"dbpedia"``, ``"wikidata"``).
        cache_dir: Path to the SKOS cache directory.

    Returns:
        Tuple of (paths, found, uri_map) where

        * *paths* — list of hierarchy path strings
          (e.g. ``["food/vegetables/potatoes"]``),
        * *found* — whether the concept was found at all,
        * *uri_map* — ``{concept_id: uri}`` for each non-root path segment.
    """
    if _current_path is None:
        _current_path = []
    if _current_uris is None:
        _current_uris = []
    if _visited_uris is None:
        _visited_uris = frozenset()

    if _depth >= _max_depth:
        logger.warning("Max hierarchy depth reached for label '%s'", label)
        return [], False, {}

    concept = lookup_concept(label, lang, source, cache_dir)
    if concept is None:
        return [], False, {}

    uri: str = concept.get("uri") or ""
    if uri and uri in _visited_uris:
        # Cycle detected — stop recursion here
        return [], True, {}
    new_visited = (_visited_uris | {uri}) if uri else _visited_uris

    pref_label: str = concept.get("prefLabel") or label
    normalized = _normalize_label(pref_label)

    # Prepend current label (building path bottom-up: root first when complete)
    new_path = [normalized] + _current_path
    new_uris = [uri] + _current_uris

    broader: list = concept.get("broader", [])

    if not broader:
        # This is a root concept — apply root mapping and emit the path
        root_label = pref_label.lower()
        root_map = ROOT_MAPPING.get(source, {})
        root_was_mapped = root_label in root_map
        if root_was_mapped:
            new_path[0] = root_map[root_label]

        full_path = "/".join(new_path)

        # Build URI map, skipping mapped (synthetic) roots
        start_idx = 1 if root_was_mapped else 0
        uri_map: dict[str, str] = {}
        for i in range(start_idx, len(new_path)):
            concept_id = "/".join(new_path[: i + 1])
            if concept_id not in uri_map and new_uris[i]:
                uri_map[concept_id] = new_uris[i]

        return [full_path], True, uri_map

    # Recurse for each broader concept
    all_paths: list[str] = []
    all_uri_maps: dict[str, str] = {}

    for broader_item in broader:
        if isinstance(broader_item, dict):
            broader_label = broader_item.get("label", "")
        else:
            broader_label = str(broader_item)
        if not broader_label:
            continue

        sub_paths, sub_found, sub_uri_map = build_hierarchy_paths(
            broader_label,
            lang,
            source,
            cache_dir,
            _current_path=new_path,
            _current_uris=new_uris,
            _visited_uris=new_visited,
            _depth=_depth + 1,
            _max_depth=_max_depth,
        )
        if sub_found:
            all_paths.extend(sub_paths)
            all_uri_maps.update(sub_uri_map)

    if not all_paths:
        # No broader concept resolved — emit a partial path from what we have
        full_path = "/".join(new_path)
        uri_map = {}
        for i in range(len(new_path)):
            concept_id = "/".join(new_path[: i + 1])
            if concept_id not in uri_map and new_uris[i]:
                uri_map[concept_id] = new_uris[i]
        return [full_path], bool(uri), uri_map

    return all_paths, True, all_uri_maps


def cache_stats(cache_dir: Path) -> dict[str, int | str]:
    """Return statistics about the SKOS cache.

    Args:
        cache_dir: Path to the SKOS cache directory.

    Returns:
        Dict with ``concept_count``, ``labels_count``, ``not_found_count``,
        and ``cache_dir`` entries.
    """
    if not cache_dir.exists():
        return {
            "concept_count": 0,
            "labels_count": 0,
            "not_found_count": 0,
            "cache_dir": str(cache_dir),
        }

    concept_count = 0
    labels_count = 0
    not_found_count = 0

    not_found_path = _get_not_found_cache_path(cache_dir)

    for cache_file in cache_dir.glob("*.json"):
        if cache_file == not_found_path:
            # Count not-found entries inside the consolidated file
            try:
                with open(cache_file, encoding="utf-8") as f:
                    data = json.load(f)
                not_found_count = len(data.get("entries", {}))
            except (json.JSONDecodeError, OSError):
                pass
            continue

        stem = cache_file.stem  # e.g. "concept_agrovoc_en_potato_abc123"
        if stem.startswith("concept_"):
            concept_count += 1
        elif stem.startswith("labels_"):
            labels_count += 1

    return {
        "concept_count": concept_count,
        "labels_count": labels_count,
        "not_found_count": not_found_count,
        "cache_dir": str(cache_dir),
    }


# ---------------------------------------------------------------------------
# Upstream REST lookups
# ---------------------------------------------------------------------------


def _upstream_lookup(label: str, lang: str, source: str) -> tuple[dict | None, bool]:
    """Dispatch to the appropriate upstream REST source.

    Returns ``(concept_or_None, query_failed)``.  ``query_failed=True``
    indicates a transient error; the result must not be added to the
    not-found cache.
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
    except httpx.TimeoutException as e:
        logger.warning("AGROVOC search timed out for '%s': %s", label, e)
        return None, True
    except httpx.HTTPError as e:
        logger.warning("AGROVOC search failed for '%s': %s", label, e)
        return None, True
    data = _parse_json(response, label)
    if data is None:
        return None, False

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
    except httpx.HTTPError as e:
        logger.warning("AGROVOC data fetch failed for %s: %s", concept_uri, e)
        return []
    concept_data = _parse_json(response, concept_uri)
    if concept_data is None:
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
    except httpx.TimeoutException as e:
        logger.warning("DBpedia lookup timed out for '%s': %s", label, e)
        return None, True
    except httpx.HTTPError as e:
        logger.warning("DBpedia lookup failed for '%s': %s", label, e)
        return None, True
    data = _parse_json(response, label)
    if data is None:
        return None, False

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

    # Fetch broader from the DBpedia data endpoint (skos:broader)
    broader = _get_broader_dbpedia(uri, lang)

    return {
        "uri": uri,
        "prefLabel": pref_label,
        "source": "dbpedia",
        "broader": broader,
        "description": description,
    }, False


def _get_broader_dbpedia(uri: str, lang: str) -> list[dict]:
    """Fetch skos:broader concepts for a DBpedia URI via the data API."""
    data_uri = uri.replace("http://dbpedia.org/resource/", "https://dbpedia.org/data/") + ".json"
    try:
        with httpx.Client(timeout=DEFAULT_TIMEOUT, follow_redirects=True) as client:
            response = client.get(data_uri)
            response.raise_for_status()
    except httpx.HTTPError as e:
        logger.debug("DBpedia data fetch failed for %s: %s", uri, e)
        return []
    data = _parse_json(response, uri)
    if data is None:
        return []

    resource_data = data.get(uri, {})
    broader_entries = resource_data.get("http://www.w3.org/2004/02/skos/core#broader", [])
    if not broader_entries:
        broader_entries = resource_data.get("http://dbpedia.org/ontology/broader", [])

    result: list[dict] = []
    for entry in broader_entries:
        broader_uri = entry.get("value", "")
        if not broader_uri or entry.get("type") != "uri":
            continue
        # Try to find an rdfs:label for this broader URI in the same response
        broader_data = data.get(broader_uri, {})
        label_val = ""
        for lbl_entry in broader_data.get("http://www.w3.org/2000/01/rdf-schema#label", []):
            if lbl_entry.get("lang") == lang:
                label_val = lbl_entry.get("value", "")
                break
        result.append({"uri": broader_uri, "label": label_val})

    return result


def _lookup_wikidata(label: str, lang: str) -> tuple[dict | None, bool]:
    """Look up a concept in Wikidata via the MediaWiki Action API.

    Uses ``wbsearchentities`` to find candidates, then ``wbgetentities``
    to retrieve P279 (subclass-of) broader links.
    """
    url = "https://www.wikidata.org/w/api.php"
    search_params: dict = {
        "action": "wbsearchentities",
        "search": label,
        "language": lang,
        "type": "item",
        "format": "json",
        "limit": "5",
    }
    headers = {"User-Agent": "tingbok/0.1 (SKOS lookup service)"}
    try:
        with httpx.Client(timeout=DEFAULT_TIMEOUT) as client:
            response = client.get(url, params=search_params, headers=headers)
            response.raise_for_status()
    except httpx.TimeoutException as e:
        logger.warning("Wikidata search timed out for '%s': %s", label, e)
        return None, True
    except httpx.HTTPError as e:
        logger.warning("Wikidata search failed for '%s': %s", label, e)
        return None, True
    data = _parse_json(response, label)
    if data is None:
        return None, False

    results: list[dict] = data.get("search", [])
    if not results:
        return None, False

    label_lower = label.lower()
    best = next((r for r in results if r.get("label", "").lower() == label_lower), results[0])
    qid: str = best.get("id", "")
    if not qid:
        return None, False

    uri = f"http://www.wikidata.org/entity/{qid}"
    pref_label = best.get("label", label)
    description = best.get("description")

    # Fetch P279 (subclass-of) claims for hierarchy building
    broader = _get_broader_wikidata(qid, lang, headers)

    return {
        "uri": uri,
        "prefLabel": pref_label,
        "source": "wikidata",
        "broader": broader,
        "description": description,
    }, False


def _get_broader_wikidata(qid: str, lang: str, headers: dict | None = None) -> list[dict]:
    """Fetch P279 (subclass-of) broader links for a Wikidata item."""
    url = "https://www.wikidata.org/w/api.php"
    params: dict = {
        "action": "wbgetentities",
        "ids": qid,
        "props": "claims|labels",
        "languages": lang,
        "format": "json",
    }
    if headers is None:
        headers = {"User-Agent": "tingbok/0.1 (SKOS lookup service)"}
    try:
        with httpx.Client(timeout=DEFAULT_TIMEOUT) as client:
            response = client.get(url, params=params, headers=headers)
            response.raise_for_status()
    except httpx.HTTPError as e:
        logger.debug("Wikidata wbgetentities failed for %s: %s", qid, e)
        return []
    data = _parse_json(response, qid)
    if data is None:
        return []

    entity = data.get("entities", {}).get(qid, {})
    p279_claims = entity.get("claims", {}).get("P279", [])

    broader_qids: list[str] = []
    for claim in p279_claims:
        mainsnak = claim.get("mainsnak", {})
        if mainsnak.get("snaktype") == "value":
            value = mainsnak.get("datavalue", {}).get("value", {})
            target_qid = value.get("id", "")
            if target_qid:
                broader_qids.append(target_qid)

    if not broader_qids:
        return []

    # Fetch labels for all broader QIDs in one request
    label_params: dict = {
        "action": "wbgetentities",
        "ids": "|".join(broader_qids),
        "props": "labels",
        "languages": lang,
        "format": "json",
    }
    try:
        with httpx.Client(timeout=DEFAULT_TIMEOUT) as client:
            response = client.get(url, params=label_params, headers=headers)
            response.raise_for_status()
    except httpx.HTTPError as e:
        logger.debug("Wikidata label fetch for broader failed: %s", e)
        return [{"uri": f"http://www.wikidata.org/entity/{bqid}", "label": ""} for bqid in broader_qids]
    label_data = _parse_json(response, "|".join(broader_qids))
    if label_data is None:
        return [{"uri": f"http://www.wikidata.org/entity/{bqid}", "label": ""} for bqid in broader_qids]

    broader: list[dict] = []
    entities = label_data.get("entities", {})
    for bqid in broader_qids:
        broader_uri = f"http://www.wikidata.org/entity/{bqid}"
        lbl = entities.get(bqid, {}).get("labels", {}).get(lang, {}).get("value", "")
        broader.append({"uri": broader_uri, "label": lbl})

    return broader


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
    except httpx.HTTPError as e:
        logger.warning("AGROVOC data fetch failed for %s: %s", uri, e)
        return {}
    data = _parse_json(response, uri)
    if data is None:
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
    except httpx.HTTPError as e:
        logger.warning("DBpedia data fetch failed for %s: %s", uri, e)
        return {}
    data = _parse_json(response, uri)
    if data is None:
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
    except httpx.HTTPError as e:
        logger.warning("Wikidata labels fetch failed for %s: %s", uri, e)
        return {}
    data = _parse_json(response, uri)
    if data is None:
        return {}
    return {lang: data[lang] for lang in languages if lang in data}
