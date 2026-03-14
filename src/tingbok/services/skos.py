"""SKOS lookup service — wraps upstream AGROVOC, DBpedia, Wikidata sources.

Cache format is compatible with inventory-md's skos.py cache so that the
/var/cache/tingbok/skos/ directory can be pre-seeded from an existing
inventory-md SKOS cache.
"""

import hashlib
import json
import logging
import re
import threading
import time
from pathlib import Path

import niquests

logger = logging.getLogger(__name__)


class UpstreamError(Exception):
    """Raised by :func:`lookup_concept` when an upstream source returns a transient error.

    The result must not be cached as a not-found entry.
    """


def _parse_json(response: niquests.Response, context: str = "") -> dict | None:
    """Parse a JSON response, returning None on empty or malformed bodies."""
    try:
        return response.json()
    except ValueError as e:
        logger.debug("Non-JSON response%s: %s", f" for {context}" if context else "", e)
        return None


CACHE_TTL_SECONDS = 60 * 60 * 24 * 60  # 60 days — matches inventory-md
TRANSIENT_TTL_SECONDS = 60 * 60 * 4  # 4 hours — short TTL for transient failures
DEFAULT_TIMEOUT = 10.0

#: Minimum similarity (0–100) for an unmatched DBpedia/Wikidata fallback result to be accepted.
_LOOKUP_SIMILARITY_THRESHOLD = 60

_HTML_TAG_RE = re.compile(r"<[^>]+>")


def _strip_html(text: str) -> str:
    """Strip HTML tags from *text* (e.g. DBpedia highlight markup ``<b>word</b>``)."""
    return _HTML_TAG_RE.sub("", text).strip()


def _label_similarity(a: str, b: str) -> float:
    """Return 0–100 similarity score between two lowercased labels.

    Uses rapidfuzz when available; falls back to exact-match (100) or 0.
    """
    a, b = a.lower().strip(), b.lower().strip()
    if a == b:
        return 100.0
    try:
        from rapidfuzz import fuzz  # noqa: PLC0415

        return fuzz.token_set_ratio(a, b)
    except ImportError:
        return 0.0


def _is_dbpedia_list_uri(uri: str) -> bool:
    """Return True if *uri* is a DBpedia 'List of …' article (not a real concept)."""
    local = uri.rsplit("/", 1)[-1]
    return local.startswith("List_of_") or local.startswith("Lists_of_")


#: RDF type URIs that identify DBpedia results as persons or geographical places.
#: Results containing any of these types are silently rejected — they are never
#: useful as product/category concepts.
_DBPEDIA_BLOCKED_TYPES: frozenset[str] = frozenset(
    {
        # Persons
        "http://dbpedia.org/ontology/Person",
        "http://xmlns.com/foaf/0.1/Person",
        "http://schema.org/Person",
        # Populated geographical places (cities, countries, districts …)
        "http://dbpedia.org/ontology/PopulatedPlace",
        "http://schema.org/Country",
        "http://schema.org/City",
        "http://schema.org/AdministrativeArea",
        # Natural geographical features (rivers, mountains, islands …)
        "http://dbpedia.org/ontology/NaturalPlace",
    }
)


def _is_dbpedia_non_concept(doc: dict) -> bool:
    """Return True if a DBpedia Lookup result doc should be excluded.

    Checks the ``type`` list for person or geographical-place RDF types.
    """
    types = doc.get("type") or []
    if isinstance(types, str):
        types = [types]
    return bool(set(types) & _DBPEDIA_BLOCKED_TYPES)


#: Wikidata QIDs (P31 "instance of" values) that identify non-concept entities
#: (persons, geographic places, disambiguation/list pages).
_WIKIDATA_BLOCKED_P31: frozenset[str] = frozenset(
    {
        # Persons
        "Q5",  # human
        "Q15632617",  # fictional human
        # Wikimedia meta-pages
        "Q4167410",  # Wikimedia disambiguation page
        "Q4167836",  # Wikimedia category page
        "Q13406463",  # Wikimedia list article
        # Geographical places (populated)
        "Q6256",  # country
        "Q515",  # city
        "Q3957",  # town
        "Q532",  # village
        "Q1549591",  # big city
        "Q7930989",  # city/town
        "Q1093829",  # city of the United States
        # Geographical places (natural)
        "Q23442",  # island
        "Q8502",  # mountain
        "Q4022",  # river
        "Q23397",  # lake
        "Q82794",  # geographic region
        "Q35145743",  # natural geographic object
    }
)


def _is_wikidata_non_concept(entity: dict) -> bool:
    """Return True if a Wikidata entity should be excluded from source lookups.

    Checks:
    * P31 (instance of) — rejects entities whose type is in _WIKIDATA_BLOCKED_P31.
    * P625 (coordinate location) — rejects geographic entities that have
      explicit map coordinates (almost always populated places or natural features).
    """
    claims = entity.get("claims") or {}
    # P625: coordinate location — geographic entities
    if "P625" in claims:
        return True
    # P31: instance of — check each value's QID
    for claim in claims.get("P31", []):
        mainsnak = claim.get("mainsnak", {})
        if mainsnak.get("snaktype") == "value":
            qid = mainsnak.get("datavalue", {}).get("value", {}).get("id", "")
            if qid in _WIKIDATA_BLOCKED_P31:
                return True
    return False


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

#: Labels that indicate a hierarchy branch has wandered into abstract
#: Wikipedia/DBpedia meta-categories unrelated to any product taxonomy.
#: When encountered as a broader concept, the branch is silently discarded.
_HIERARCHY_DEAD_ENDS: frozenset[str] = frozenset(
    {
        # Abstract philosophy / ontology
        "property",
        "physical property",
        "chemical property",
        "mathematical property",
        "abstract object",
        "abstraction",
        "concept",
        # Mathematics / physics
        "quantity",
        "physical quantity",
        "scalar",
        "scalar quantity",
        "vector",
        "tensor",
        "ratio",
        "value",
        "number",
        "multilinear map",
        "individual quantity",
        # Arts meta-categories
        "elements of music",
        "elements of art",
        # Wikipedia structural categories
        "categories",
        "main topic classifications",
    }
)


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


def _load_from_cache(cache_path: Path, ttl: int = CACHE_TTL_SECONDS) -> dict | None:  # noqa: ARG001
    """Load cached data if it exists.

    The *ttl* parameter is retained for call-site compatibility but is no longer
    enforced — freshness is maintained by the background :func:`cache_refresh_loop`
    rather than by hard expiry.  Stale-but-present data is always returned.

    Stamps ``_last_accessed`` on every hit so that :func:`prune_cache` can
    distinguish recently-used entries from abandoned ones.
    """
    if not cache_path.exists():
        return None
    try:
        with open(cache_path, encoding="utf-8") as f:
            data: dict = json.load(f)
        now = time.time()
        data["_last_accessed"] = now
        try:
            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except OSError as e:
            logger.debug("Cache access-stamp failed for %s: %s", cache_path, e)
        return data
    except (json.JSONDecodeError, OSError) as e:
        logger.debug("Cache read failed for %s: %s", cache_path, e)
        return None


def _save_to_cache(
    cache_path: Path,
    data: dict,
    *,
    last_accessed: float | None = None,
    cache_key: str | None = None,
) -> None:
    """Save data to a cache file, stamping ``_cached_at``.

    Args:
        last_accessed: When provided (e.g. during a background refresh that should
            not reset the access clock), the value is written as ``_last_accessed``
            instead of being left absent.  Pass ``None`` (default) to omit the field
            so that the next :func:`_load_from_cache` hit stamps it with the real
            access time.
        cache_key: When provided, stored as ``_cache_key`` so that
            :func:`_refresh_entry` can re-fetch the entry without external context.
    """
    payload = {**data, "_cached_at": time.time()}
    if last_accessed is not None:
        payload["_last_accessed"] = last_accessed
    if cache_key is not None:
        payload["_cache_key"] = cache_key
    try:
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        with open(cache_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)
    except OSError as e:
        logger.warning("Cache write failed for %s: %s", cache_path, e)


def _find_oldest_cache_entry(cache_dir: Path) -> tuple[Path, float] | None:
    """Return ``(path, timestamp)`` of the least-recently-used cache file in *cache_dir*.

    The timestamp is ``_last_accessed`` when present, falling back to ``_cached_at``.
    ``_not_found.json`` is skipped (it is a compound index, not a single entry).
    Returns ``None`` when the directory is empty or contains no readable entries.
    """
    oldest_path: Path | None = None
    oldest_ts: float = float("inf")

    for cache_path in cache_dir.glob("*.json"):
        if cache_path.name == "_not_found.json":
            continue
        try:
            with open(cache_path, encoding="utf-8") as f:
                data: dict = json.load(f)
        except (json.JSONDecodeError, OSError):
            continue
        ts = data.get("_last_accessed") or data.get("_cached_at", float("inf"))
        if ts < oldest_ts:
            oldest_ts = ts
            oldest_path = cache_path

    if oldest_path is None:
        return None
    return oldest_path, oldest_ts


def _refresh_entry(cache_path: Path, cache_dir: Path) -> bool:
    """Re-fetch the upstream data for a single cache entry in-place.

    Reads ``_cache_key`` from the file to determine the entry type and fetch
    parameters, calls the appropriate upstream function, then writes the result
    back preserving the original ``_last_accessed`` timestamp so the access
    clock is not reset by the background refresh.

    Returns ``True`` on success, ``False`` when the entry cannot be refreshed
    (missing key, unknown type, upstream error, etc.).
    """
    try:
        with open(cache_path, encoding="utf-8") as f:
            data: dict = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        logger.debug("Refresh skipped — cannot read %s: %s", cache_path, e)
        return False

    cache_key: str | None = data.get("_cache_key")
    if not cache_key:
        logger.debug("Refresh skipped — no _cache_key in %s", cache_path)
        return False

    last_accessed: float | None = data.get("_last_accessed")

    try:
        prefix, *rest = cache_key.split(":", 1)
        tail = rest[0] if rest else ""

        if prefix == "concept":
            # key: concept:{source}:{lang}:{label}
            parts = tail.split(":", 2)
            if len(parts) < 3:  # noqa: PLR2004
                return False
            source, lang, label = parts
            concept, query_failed = _upstream_lookup(label, lang, source, cache_dir)
            if query_failed:
                return False
            new_data = concept if concept else {}
            _save_to_cache(cache_path, new_data, last_accessed=last_accessed, cache_key=cache_key)
            return bool(concept)

        elif prefix in ("labels", "alt_labels"):
            uri = data.get("uri", "")
            source = data.get("source", "")
            if not uri or not source:
                return False
            if prefix == "labels":
                languages = list((data.get("labels") or {}).keys()) or ["en"]
                result = _upstream_get_labels(uri, source, languages)
                if result is None:
                    return False
                new_data = {"uri": uri, "source": source, "labels": result}
            else:
                languages = list((data.get("alt_labels") or {}).keys()) or ["en"]
                result = _upstream_get_alt_labels(uri, source, languages)
                if result is None:
                    return False
                new_data = {"uri": uri, "source": source, "alt_labels": result}
            _save_to_cache(cache_path, new_data, last_accessed=last_accessed, cache_key=cache_key)
            return True

        elif prefix == "description":
            uri = data.get("uri", "")
            source = data.get("source", "")
            lang = data.get("lang", "en")
            if not uri or not source:
                return False
            desc = _upstream_get_description(uri, source, lang)
            new_data = {"uri": uri, "source": source, "lang": lang, "description": desc}
            _save_to_cache(cache_path, new_data, last_accessed=last_accessed, cache_key=cache_key)
            return True

    except Exception as exc:  # noqa: BLE001
        logger.debug("Refresh failed for %s: %s", cache_path, exc)

    return False


async def cache_refresh_loop(
    cache_dir: Path,
    max_age_seconds: float = CACHE_TTL_SECONDS,
    divisor: float = 100.0,
) -> None:
    """Background task: continuously refresh the oldest cache entry.

    Sleep duration is proportional to how fresh the oldest entry is::

        sleep = max(0, (max_age_seconds - age_seconds) / divisor)

    When the oldest entry is younger than *max_age_seconds*, the task sleeps
    for a while before checking again.  When it is at or past *max_age_seconds*,
    the task refreshes immediately and loops without sleeping.

    Args:
        cache_dir:       Root cache directory.  Both ``skos/`` and ``ean/``
                         subdirectories are scanned for the oldest entry; only
                         SKOS entries (those with a ``_cache_key``) can be
                         re-fetched automatically.
        max_age_seconds: Target freshness window (default: ``CACHE_TTL_SECONDS``).
        divisor:         Controls refresh aggressiveness.  Higher values mean
                         longer sleeps and less frequent refreshes (default: 100).
    """
    import asyncio  # noqa: PLC0415

    logger.info(
        "Cache refresh loop started (max_age=%.0fd, divisor=%.0f)",
        max_age_seconds / 86400,
        divisor,
    )
    while True:
        oldest = _find_oldest_cache_entry(cache_dir)
        if oldest is None:
            await asyncio.sleep(3600)
            continue

        cache_path, oldest_ts = oldest
        age = time.time() - oldest_ts
        sleep_secs = max(0.0, (max_age_seconds - age) / divisor)

        if sleep_secs > 0:
            logger.debug(
                "Oldest cache entry is %.1fd old; sleeping %.0fs before refresh",
                age / 86400,
                sleep_secs,
            )
            await asyncio.sleep(sleep_secs)

        logger.debug("Refreshing oldest cache entry: %s (age %.1fd)", cache_path.name, age / 86400)
        await asyncio.to_thread(_refresh_entry, cache_path, cache_dir)


def _is_in_not_found_cache(cache_dir: Path, key: str, ttl: int = CACHE_TTL_SECONDS) -> bool:
    """Return True if *key* is present (and not expired) in the not-found cache.

    Transient entries (timeouts/connection errors) use ``TRANSIENT_TTL_SECONDS``
    regardless of the *ttl* argument.
    """
    cache_path = _get_not_found_cache_path(cache_dir)
    if not cache_path.exists():
        return False
    try:
        with open(cache_path, encoding="utf-8") as f:
            data: dict = json.load(f)
        entry = data.get("entries", {}).get(key)
        if entry is None:
            return False
        effective_ttl = TRANSIENT_TTL_SECONDS if entry.get("transient") else ttl
        return time.time() - entry.get("cached_at", 0) <= effective_ttl
    except (json.JSONDecodeError, OSError) as e:
        logger.debug("Not-found cache read failed: %s", e)
        return False


def _add_to_not_found_cache(cache_dir: Path, key: str, *, transient: bool = False) -> None:
    """Add *key* to the consolidated not-found cache file.

    Args:
        transient: When True the entry is a transient failure (timeout / connection
            error) and will expire after ``TRANSIENT_TTL_SECONDS`` instead of the
            normal 60-day TTL.
    """
    cache_path = _get_not_found_cache_path(cache_dir)
    data: dict = {"entries": {}}
    if cache_path.exists():
        try:
            with open(cache_path, encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError):
            data = {"entries": {}}
    entry: dict = {"cached_at": time.time()}
    if transient:
        entry["transient"] = True
    data.setdefault("entries", {})[key] = entry
    try:
        cache_dir.mkdir(parents=True, exist_ok=True)
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
        # Evict stale DBpedia results that are list articles (cached before the filter was added)
        if source == "dbpedia" and _is_dbpedia_list_uri(cached["uri"]):
            logger.debug("Evicting stale DBpedia list-article cache entry for '%s'", label)
            try:
                cache_path.unlink()
            except OSError:
                pass
        else:
            return cached

    if _is_in_not_found_cache(cache_dir, cache_key):
        return None

    concept, query_failed = _upstream_lookup(label, lang, source, cache_dir)

    if query_failed:
        _add_to_not_found_cache(cache_dir, cache_key, transient=True)
        raise UpstreamError(f"{source} request failed transiently for '{label}'")

    if concept:
        _save_to_cache(cache_path, concept, cache_key=cache_key)
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
    if all_labels is None:
        # Transient error — do not cache; caller gets empty result this time
        return {}
    # Cache even an empty dict so we don't re-query on every run
    _save_to_cache(cache_path, {"uri": uri, "source": source, "labels": all_labels}, cache_key=cache_key)
    return {lang: all_labels[lang] for lang in languages if lang in all_labels}


def get_alt_labels(uri: str, languages: list[str], source: str, cache_dir: Path) -> dict[str, list[str]]:
    """Fetch alternative labels (synonyms) for a concept URI.

    Serves from cache when available; falls back to upstream REST APIs.

    Args:
        uri:       Full SKOS concept URI.
        languages: List of BCP-47 language codes to return.
        source:    Taxonomy source: ``"agrovoc"``, ``"dbpedia"``, or ``"wikidata"``.
        cache_dir: Path to the SKOS cache directory.

    Returns:
        Dict mapping language code to list of alternative labels.
    """
    if not uri or not languages:
        return {}

    uri_hash = hashlib.md5(uri.encode()).hexdigest()[:16]  # noqa: S324 — non-crypto use
    cache_key = f"alt_labels:{source}:{uri_hash}"
    cache_path = _get_cache_path(cache_dir, cache_key)

    cached = _load_from_cache(cache_path)
    if cached is not None:
        cached_alts: dict[str, list[str]] = cached.get("alt_labels", {})
        return {lang: cached_alts[lang] for lang in languages if lang in cached_alts}

    all_alts = _upstream_get_alt_labels(uri, source, languages)
    if all_alts is None:
        # Transient error — do not cache
        return {}
    _save_to_cache(cache_path, {"uri": uri, "source": source, "alt_labels": all_alts}, cache_key=cache_key)
    return {lang: all_alts[lang] for lang in languages if lang in all_alts}


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
        logger.debug("Max hierarchy depth reached for label '%s'", label)
        return [], False, {}

    try:
        concept = lookup_concept(label, lang, source, cache_dir)
    except UpstreamError:
        return [], False, {}
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
        if broader_label.lower() in _HIERARCHY_DEAD_ENDS:
            logger.debug("Skipping abstract dead-end broader concept '%s'", broader_label)
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


def uri_to_source(uri: str) -> str | None:
    """Map a concept URI to its source name.

    Args:
        uri: Any URI stored in ``source_uris`` (e.g. ``"http://dbpedia.org/resource/Food"``
             or ``"off:en:potatoes"`` or ``"gpt:632"``).

    Returns:
        Source name string (``"agrovoc"``, ``"dbpedia"``, ``"wikidata"``, ``"off"``,
        ``"gpt"``) or ``None`` if the URI does not match a known source.
    """
    if uri.startswith(("http://aims.fao.org/", "https://aims.fao.org/")):
        return "agrovoc"
    if uri.startswith(("http://dbpedia.org/", "https://dbpedia.org/")):
        return "dbpedia"
    if uri.startswith(("http://www.wikidata.org/", "https://www.wikidata.org/")):
        return "wikidata"
    if uri.startswith("off:"):
        return "off"
    if uri.startswith("gpt:"):
        return "gpt"
    return None


def get_description(uri: str, source: str, lang: str, cache_dir: Path) -> str | None:
    """Fetch a human-readable description for a concept URI.

    Checks the labels cache first (descriptions are stored alongside labels when
    ``get_labels`` fetches from an upstream source that returns descriptions).
    Falls back to a live upstream call for DBpedia and Wikidata if not cached.

    Only DBpedia and Wikidata provide reliable descriptions; AGROVOC, OFF, and
    GPT are not supported and return ``None``.

    Args:
        uri:       Full concept URI.
        source:    Taxonomy source (``"dbpedia"`` or ``"wikidata"``).
        lang:      Preferred language code for the description (e.g. ``"en"``).
        cache_dir: Path to the SKOS cache directory.

    Returns:
        Description string, or ``None`` if unavailable.
    """
    if source not in ("dbpedia", "wikidata"):
        return None

    uri_hash = hashlib.md5(uri.encode()).hexdigest()[:16]  # noqa: S324 — non-crypto use
    # Use a separate "description:" prefix so this cache is independent of the labels cache.
    # If we shared the labels cache key, a previously cached labels entry (without a
    # "description" key) would block description fetching forever.
    cache_key = f"description:{source}:{uri_hash}"
    cache_path = _get_cache_path(cache_dir, cache_key)

    cached = _load_from_cache(cache_path)
    if cached is not None:
        return cached.get("description")

    desc = _upstream_get_description(uri, source, lang)
    _save_to_cache(cache_path, {"uri": uri, "source": source, "lang": lang, "description": desc}, cache_key=cache_key)
    return desc


def _upstream_get_description(uri: str, source: str, lang: str) -> str | None:
    """Fetch a description from the appropriate upstream source."""
    if source == "dbpedia":
        return _get_dbpedia_description(uri, lang)
    if source == "wikidata":
        return _get_wikidata_description(uri, lang)
    return None


def _get_dbpedia_description(uri: str, lang: str) -> str | None:
    """Fetch rdfs:comment for a DBpedia resource via the data API."""
    data_uri = uri.replace("http://dbpedia.org/resource/", "https://dbpedia.org/data/") + ".json"
    try:
        with niquests.Session() as session:
            response = session.get(data_uri, timeout=DEFAULT_TIMEOUT)
            response.raise_for_status()
    except niquests.exceptions.RequestException as e:
        logger.debug("DBpedia description fetch failed for %s: %s", uri, e)
        return None
    data = _parse_json(response, uri)
    if data is None:
        return None

    resource_data = data.get(uri, {})
    for entry in resource_data.get("http://www.w3.org/2000/01/rdf-schema#comment", []):
        if entry.get("lang") == lang:
            value = entry.get("value", "")
            if value:
                return value
    return None


def _get_wikidata_description(uri: str, lang: str) -> str | None:
    """Fetch the description for a Wikidata item via the Wikibase REST API."""
    qid = uri.rstrip("/").split("/")[-1]
    if not qid.startswith("Q"):
        return None
    url = f"https://www.wikidata.org/w/rest.php/wikibase/v1/entities/items/{qid}/descriptions"
    headers = {"User-Agent": "tingbok/0.1 (SKOS lookup service)"}
    try:
        with niquests.Session() as session:
            response = session.get(url, headers=headers, timeout=DEFAULT_TIMEOUT)
            response.raise_for_status()
    except niquests.exceptions.RequestException as e:
        logger.debug("Wikidata description fetch failed for %s: %s", uri, e)
        return None
    data = _parse_json(response, uri)
    if data is None:
        return None
    return data.get(lang) or None


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
# AGROVOC Oxigraph (local) lookup
# ---------------------------------------------------------------------------

#: Module-level cached Oxigraph store (None = not loaded or unavailable).
_agrovoc_store: object | None = None
#: True while a background thread is loading the store.
_agrovoc_loading: bool = False


def _do_load_agrovoc(cache_dir: Path) -> None:
    """Load the AGROVOC Oxigraph store from disk.  Runs in a background thread."""
    global _agrovoc_store, _agrovoc_loading  # noqa: PLW0603
    nt_path = cache_dir / "agrovoc.nt"
    try:
        import pyoxigraph  # noqa: PLC0415

        store = pyoxigraph.Store()
        with open(nt_path, "rb") as f:
            store.load(f, pyoxigraph.RdfFormat.N_TRIPLES)
        _agrovoc_store = store
        logger.info("Loaded AGROVOC Oxigraph store from %s (%d triples)", nt_path, len(store))
    except ImportError:
        logger.debug("pyoxigraph not installed; AGROVOC Oxigraph lookup unavailable")
    except Exception as exc:
        logger.warning("Failed to load AGROVOC Oxigraph store from %s: %s", nt_path, exc)
    finally:
        _agrovoc_loading = False


def load_agrovoc_background(cache_dir: Path) -> None:
    """Start loading the AGROVOC Oxigraph store in a background daemon thread.

    Returns immediately.  The store becomes available via :func:`get_agrovoc_store`
    once loading completes.  Subsequent calls while loading is in progress or
    after the store is already loaded are no-ops.

    Args:
        cache_dir: Directory that may contain ``agrovoc.nt``.
    """
    global _agrovoc_loading  # noqa: PLW0603
    if _agrovoc_store is not None or _agrovoc_loading:
        return
    nt_path = cache_dir / "agrovoc.nt"
    if not nt_path.exists():
        return
    _agrovoc_loading = True
    logger.info("Starting background load of AGROVOC Oxigraph store from %s", nt_path)
    thread = threading.Thread(target=_do_load_agrovoc, args=(cache_dir,), daemon=True)
    thread.start()


def get_agrovoc_store(cache_dir: Path) -> object | None:  # noqa: ARG001
    """Return the loaded pyoxigraph Store for AGROVOC, or ``None`` if unavailable.

    Non-blocking: returns ``None`` while a background load is in progress.
    Callers should fall back to the REST API when this returns ``None``.

    Use :func:`load_agrovoc_background` at application startup to trigger
    background loading so the store becomes available without blocking requests.
    """
    return _agrovoc_store


def _label_variations(label: str) -> list[str]:
    """Generate singular/plural label variations for AGROVOC SKOS-XL lookup.

    Mirrors the same logic used in inventory-md's ``_lookup_agrovoc_oxigraph``.
    """
    base = label.lower()
    variations = [base]

    if base.endswith("y") and len(base) > 2 and base[-2] not in "aeiou":
        variations.append(base[:-1] + "ies")  # berry -> berries
    elif base.endswith(("s", "x", "z", "ch", "sh", "o")):
        variations.append(base + "es")  # potato -> potatoes
    else:
        variations.append(base + "s")  # tool -> tools
    if base.endswith("o"):
        variations.append(base + "s")  # photo -> photos

    if base.endswith("ies") and len(base) > 3:
        variations.append(base[:-3] + "y")  # berries -> berry
    elif base.endswith("oes") and len(base) > 3:
        variations.append(base[:-2])  # potatoes -> potato
    elif base.endswith("es") and len(base) > 2:
        variations.append(base[:-2])  # brushes -> brush
    elif base.endswith("s") and len(base) > 1:
        variations.append(base[:-1])  # tools -> tool

    # Add title-case variants; deduplicate while preserving order
    with_title = []
    for v in variations:
        with_title.append(v)
        with_title.append(v.title())
    return list(dict.fromkeys(with_title))


def _get_broader_agrovoc_oxigraph(concept_uri: str, lang: str, store: object) -> list[dict]:
    """Fetch ``skos:broader`` concepts from the local Oxigraph store."""
    query = f"""
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    PREFIX skosxl: <http://www.w3.org/2008/05/skos-xl#>

    SELECT DISTINCT ?broader ?label WHERE {{
        <{concept_uri}> skos:broader ?broader .
        ?broader skosxl:prefLabel/skosxl:literalForm ?label .
        FILTER(lang(?label) = "{lang}")
    }}
    LIMIT 10
    """
    try:
        results = store.query(query)  # type: ignore[union-attr]
        broader = [{"uri": r["broader"].value, "label": r["label"].value} for r in results]

        # One level up for richer hierarchy context
        if broader:
            q2 = f"""
            PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
            PREFIX skosxl: <http://www.w3.org/2008/05/skos-xl#>

            SELECT DISTINCT ?broader ?label WHERE {{
                <{broader[0]["uri"]}> skos:broader ?broader .
                ?broader skosxl:prefLabel/skosxl:literalForm ?label .
                FILTER(lang(?label) = "{lang}")
            }}
            LIMIT 5
            """
            results2 = store.query(q2)  # type: ignore[union-attr]
            broader.extend({"uri": r["broader"].value, "label": r["label"].value} for r in results2)
        return broader
    except Exception as exc:
        logger.warning("Oxigraph broader query failed for %s: %s", concept_uri, exc)
        return []


def _get_agrovoc_labels_oxigraph(uri: str, languages: list[str], store: object) -> dict[str, str]:
    """Fetch SKOS-XL prefLabels for *uri* from the local Oxigraph store."""
    query = f"""
    PREFIX skosxl: <http://www.w3.org/2008/05/skos-xl#>
    SELECT DISTINCT ?label WHERE {{
        <{uri}> skosxl:prefLabel/skosxl:literalForm ?label .
    }}
    """
    try:
        results = list(store.query(query))  # type: ignore[union-attr]
        return {r["label"].language: r["label"].value for r in results if r["label"].language in languages}
    except Exception as exc:
        logger.debug("Oxigraph prefLabel query failed for %s: %s", uri, exc)
        return {}


def _get_agrovoc_alt_labels_oxigraph(uri: str, languages: list[str], store: object) -> dict[str, list[str]]:
    """Fetch SKOS-XL altLabels for *uri* from the local Oxigraph store."""
    query = f"""
    PREFIX skosxl: <http://www.w3.org/2008/05/skos-xl#>
    SELECT DISTINCT ?label WHERE {{
        <{uri}> skosxl:altLabel/skosxl:literalForm ?label .
    }}
    """
    try:
        results = list(store.query(query))  # type: ignore[union-attr]
        alts: dict[str, list[str]] = {}
        for r in results:
            lang = r["label"].language
            if lang in languages:
                alts.setdefault(lang, []).append(r["label"].value)
        return alts
    except Exception as exc:
        logger.debug("Oxigraph altLabel query failed for %s: %s", uri, exc)
        return {}


def _lookup_agrovoc_oxigraph(label: str, lang: str, store: object) -> tuple[dict | None, bool]:
    """Look up a concept in AGROVOC via the local Oxigraph store.

    Returns ``(concept_dict, query_failed)``.  When the concept is absent from
    the local store the result is authoritative: ``(None, False)`` — no REST
    fallback is needed.
    """
    for try_label in _label_variations(label):
        for predicate in ("prefLabel", "altLabel"):
            query = f"""
            PREFIX skosxl: <http://www.w3.org/2008/05/skos-xl#>
            SELECT DISTINCT ?concept ?prefLabel WHERE {{
                ?concept skosxl:{predicate}/skosxl:literalForm "{try_label}"@{lang} .
                ?concept skosxl:prefLabel/skosxl:literalForm ?prefLabel .
                FILTER(lang(?prefLabel) = "{lang}")
            }}
            LIMIT 1
            """
            try:
                results = list(store.query(query))  # type: ignore[union-attr]
                if results:
                    concept_uri: str = results[0]["concept"].value
                    pref_label: str = results[0]["prefLabel"].value
                    broader = _get_broader_agrovoc_oxigraph(concept_uri, lang, store)
                    return {
                        "uri": concept_uri,
                        "prefLabel": pref_label,
                        "source": "agrovoc",
                        "broader": broader,
                    }, False
            except Exception as exc:
                logger.debug("Oxigraph query failed for '%s' (%s): %s", try_label, predicate, exc)

    return None, False  # Not found — authoritative


# ---------------------------------------------------------------------------
# Upstream REST lookups
# ---------------------------------------------------------------------------


def _upstream_lookup(label: str, lang: str, source: str, cache_dir: Path) -> tuple[dict | None, bool]:
    """Dispatch to the appropriate upstream source.

    Tries the local Oxigraph store first for AGROVOC (when available), then
    falls back to the REST API.

    Returns ``(concept_or_None, query_failed)``.  ``query_failed=True``
    indicates a transient error; the result must not be added to the
    not-found cache.
    """
    if source == "agrovoc":
        return _lookup_agrovoc(label, lang, cache_dir)
    if source == "dbpedia":
        return _lookup_dbpedia(label, lang)
    if source == "wikidata":
        return _lookup_wikidata(label, lang)
    logger.warning("Unknown SKOS source: %s", source)
    return None, True


def _lookup_agrovoc(label: str, lang: str, cache_dir: Path) -> tuple[dict | None, bool]:
    """Look up a concept in AGROVOC.

    Prefers the local Oxigraph store (``agrovoc.nt`` in *cache_dir*) for
    accuracy and speed.  Falls back to the Skosmos REST API when the local
    store is unavailable.
    """
    store = get_agrovoc_store(cache_dir)
    if store is not None:
        return _lookup_agrovoc_oxigraph(label, lang, store)
    # Fall back to REST — local store not loaded yet (or unavailable)
    logger.info("Querying AGROVOC REST API for concept '%s' (local Oxigraph store not available)", label)
    rest_base = _REST_ENDPOINTS["agrovoc"]
    url = f"{rest_base}/search/"
    params = {"query": label, "lang": lang}
    try:
        with niquests.Session() as session:
            response = session.get(url, params=params, timeout=DEFAULT_TIMEOUT)
            response.raise_for_status()
    except niquests.exceptions.Timeout as e:
        logger.warning("AGROVOC search timed out for '%s': %s", label, e)
        return None, True
    except niquests.exceptions.RequestException as e:
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
        with niquests.Session() as session:
            response = session.get(url, params=params, timeout=DEFAULT_TIMEOUT)
            response.raise_for_status()
    except niquests.exceptions.RequestException as e:
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
        with niquests.Session() as session:
            response = session.get(url, params=params, timeout=DEFAULT_TIMEOUT)
            response.raise_for_status()
    except niquests.exceptions.Timeout as e:
        logger.warning("DBpedia lookup timed out for '%s': %s", label, e)
        return None, True
    except niquests.exceptions.RequestException as e:
        logger.warning("DBpedia lookup failed for '%s': %s", label, e)
        return None, True
    data = _parse_json(response, label)
    if data is None:
        return None, False

    # Filter list articles and non-concept entities (persons, places) immediately
    def _result_uri(r: dict) -> str:
        res = r.get("resource", [])
        return res[0] if isinstance(res, list) and res else (res if isinstance(res, str) else "")

    results: list[dict] = [
        r for r in data.get("docs", []) if not _is_dbpedia_list_uri(_result_uri(r)) and not _is_dbpedia_non_concept(r)
    ]
    if not results:
        return None, False

    label_lower = label.lower()
    best = None
    for result in results:
        raw_labels = result.get("label", [])
        labels = raw_labels if isinstance(raw_labels, list) else [raw_labels]
        if any(_strip_html(lbl).lower() == label_lower for lbl in labels):
            best = result
            break
    if best is None:
        # No exact match — accept first result only if it is sufficiently similar
        candidate = results[0]
        raw_labels = candidate.get("label", [])
        candidate_label = _strip_html(raw_labels[0] if isinstance(raw_labels, list) and raw_labels else str(raw_labels))
        if _label_similarity(label, candidate_label) < _LOOKUP_SIMILARITY_THRESHOLD:
            logger.debug(
                "DBpedia fallback result %r rejected for query %r (low similarity)",
                candidate_label,
                label,
            )
            return None, False
        best = candidate

    resource = best.get("resource", [])
    uri = resource[0] if isinstance(resource, list) and resource else (resource if isinstance(resource, str) else "")
    if not uri:
        return None, False

    raw_labels = best.get("label", [])
    pref_label = _strip_html(raw_labels[0] if isinstance(raw_labels, list) and raw_labels else str(raw_labels))
    comment = best.get("comment", [])
    raw_description = comment[0] if isinstance(comment, list) and comment else (comment or None)
    description: str | None = _strip_html(raw_description) if raw_description else None

    # Fetch broader from the DBpedia data endpoint (skos:broader); skip list articles
    broader_raw = _get_broader_dbpedia(uri, lang)
    broader = [b for b in broader_raw if not _is_dbpedia_list_uri(b.get("uri", ""))]

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
        with niquests.Session() as session:
            response = session.get(data_uri, timeout=DEFAULT_TIMEOUT)
            response.raise_for_status()
    except niquests.exceptions.RequestException as e:
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
        "limit": "15",
    }
    headers = {"User-Agent": "tingbok/0.1 (SKOS lookup service)"}
    try:
        with niquests.Session() as session:
            response = session.get(url, params=search_params, headers=headers, timeout=DEFAULT_TIMEOUT)
            response.raise_for_status()
    except niquests.exceptions.Timeout as e:
        logger.warning("Wikidata search timed out for '%s': %s", label, e)
        return None, True
    except niquests.exceptions.RequestException as e:
        logger.warning("Wikidata search failed for '%s': %s", label, e)
        return None, True
    data = _parse_json(response, label)
    if data is None:
        return None, False

    results: list[dict] = data.get("search", [])
    if not results:
        return None, False

    label_lower = label.lower()
    best = next((r for r in results if r.get("label", "").lower() == label_lower), None)
    if best is None:
        candidate = results[0]
        candidate_label = candidate.get("label", "")
        if _label_similarity(label, candidate_label) < _LOOKUP_SIMILARITY_THRESHOLD:
            logger.debug(
                "Wikidata fallback result %r rejected for query %r (low similarity)",
                candidate_label,
                label,
            )
            return None, False
        best = candidate
    qid: str = best.get("id", "")
    if not qid:
        return None, False

    uri = f"http://www.wikidata.org/entity/{qid}"
    pref_label = best.get("label", label)
    description = best.get("description")

    # Fetch entity claims (P31 for filtering + P279 for hierarchy) in one request
    entity_claims_url = "https://www.wikidata.org/w/api.php"
    entity_params: dict = {
        "action": "wbgetentities",
        "ids": qid,
        "props": "claims",
        "format": "json",
    }
    try:
        with niquests.Session() as session:
            entity_response = session.get(
                entity_claims_url, params=entity_params, headers=headers, timeout=DEFAULT_TIMEOUT
            )
            entity_response.raise_for_status()
        entity_data = _parse_json(entity_response, qid)
        entity = (entity_data or {}).get("entities", {}).get(qid, {})
    except niquests.exceptions.RequestException as e:
        logger.debug("Wikidata entity claims fetch failed for %s: %s", qid, e)
        return None, True  # Transient error

    # Reject persons, geographic places, and disambiguation/list pages
    if _is_wikidata_non_concept(entity):
        logger.debug("Wikidata %s filtered out (person/place/disambiguation)", qid)
        return None, False

    # Extract P279 broader QIDs from the same entity data
    p279_claims = entity.get("claims", {}).get("P279", [])
    broader_qids: list[str] = []
    for claim in p279_claims:
        mainsnak = claim.get("mainsnak", {})
        if mainsnak.get("snaktype") == "value":
            target_qid = mainsnak.get("datavalue", {}).get("value", {}).get("id", "")
            if target_qid:
                broader_qids.append(target_qid)

    # Fetch labels for broader QIDs
    broader: list[dict] = []
    if broader_qids:
        label_params: dict = {
            "action": "wbgetentities",
            "ids": "|".join(broader_qids),
            "props": "labels",
            "languages": lang,
            "format": "json",
        }
        try:
            with niquests.Session() as session:
                lbl_response = session.get(
                    entity_claims_url, params=label_params, headers=headers, timeout=DEFAULT_TIMEOUT
                )
                lbl_response.raise_for_status()
            lbl_data = _parse_json(lbl_response, "|".join(broader_qids))
            entities = (lbl_data or {}).get("entities", {})
            for bqid in broader_qids:
                broader_uri = f"http://www.wikidata.org/entity/{bqid}"
                lbl = entities.get(bqid, {}).get("labels", {}).get(lang, {}).get("value", "")
                broader.append({"uri": broader_uri, "label": lbl})
        except niquests.exceptions.RequestException as e:
            logger.debug("Wikidata label fetch for broader failed: %s", e)
            broader = [{"uri": f"http://www.wikidata.org/entity/{bqid}", "label": ""} for bqid in broader_qids]

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
        with niquests.Session() as session:
            response = session.get(url, params=params, headers=headers, timeout=DEFAULT_TIMEOUT)
            response.raise_for_status()
    except niquests.exceptions.RequestException as e:
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
        with niquests.Session() as session:
            response = session.get(url, params=label_params, headers=headers, timeout=DEFAULT_TIMEOUT)
            response.raise_for_status()
    except niquests.exceptions.RequestException as e:
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


def _upstream_get_alt_labels(uri: str, source: str, languages: list[str]) -> dict[str, list[str]] | None:
    """Fetch alternative labels for a URI from the appropriate upstream source.

    Returns:
        Dict of ``{lang: [altLabel, ...]}`` on success (may be empty).
        ``None`` when the request failed transiently.
    """
    if source == "agrovoc":
        return _get_agrovoc_alt_labels(uri, languages)
    if source == "dbpedia":
        return _get_dbpedia_alt_labels(uri, languages)
    if source == "wikidata":
        return _get_wikidata_alt_labels(uri, languages)
    return {}


def _upstream_get_labels(uri: str, source: str, languages: list[str]) -> dict[str, str] | None:
    """Fetch multilingual labels for a URI from the appropriate upstream source.

    Returns:
        Dict of ``{lang: label}`` on success (may be empty if no labels exist).
        ``None`` when the request failed transiently (timeout, connection error)
        and the result must not be cached.
    """
    if source == "agrovoc":
        return _get_agrovoc_labels(uri, languages)
    if source == "dbpedia":
        return _get_dbpedia_labels(uri, languages)
    if source == "wikidata":
        return _get_wikidata_labels(uri, languages)
    return {}


def _get_agrovoc_labels(uri: str, languages: list[str]) -> dict[str, str] | None:
    """Fetch multilingual labels for an AGROVOC URI.

    Uses the local Oxigraph store when available; falls back to the REST API.
    Returns ``None`` on transient (non-HTTP) errors; ``{}`` when the server
    responded but found no labels (e.g. 404).
    """
    if _agrovoc_store is not None:
        return _get_agrovoc_labels_oxigraph(uri, languages, _agrovoc_store)
    reason = "still loading" if _agrovoc_loading else "store unavailable"
    logger.info("Querying AGROVOC REST API for labels (%s): %s", reason, uri)
    rest_base = _REST_ENDPOINTS["agrovoc"]
    url = f"{rest_base}/data/"
    params = {"uri": uri}
    try:
        with niquests.Session() as session:
            response = session.get(url, params=params, timeout=DEFAULT_TIMEOUT)
            response.raise_for_status()
    except niquests.exceptions.HTTPError as e:
        logger.debug("AGROVOC HTTP error for %s: %s", uri, e)
        return {}  # Definitive server response — cache as empty
    except niquests.exceptions.RequestException as e:
        logger.warning("AGROVOC data fetch failed for %s: %s", uri, e)
        return None  # Transient — do not cache
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


def _get_dbpedia_labels(uri: str, languages: list[str]) -> dict[str, str] | None:
    """Fetch multilingual labels for a DBpedia URI via the Data REST API.

    Returns ``None`` on transient (non-HTTP) errors; ``{}`` when the server
    responded but found no labels (e.g. 404).
    """
    data_uri = uri.replace("http://dbpedia.org/resource/", "https://dbpedia.org/data/") + ".json"
    try:
        with niquests.Session() as session:
            response = session.get(data_uri, timeout=DEFAULT_TIMEOUT)
            response.raise_for_status()
    except niquests.exceptions.HTTPError as e:
        logger.debug("DBpedia HTTP error for %s: %s", uri, e)
        return {}  # Definitive server response — cache as empty
    except niquests.exceptions.RequestException as e:
        logger.warning("DBpedia data fetch failed for %s: %s", uri, e)
        return None  # Transient — do not cache
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


def _get_wikidata_labels(uri: str, languages: list[str]) -> dict[str, str] | None:
    """Fetch multilingual labels for a Wikidata item via the Wikibase REST API.

    Returns ``None`` on transient (non-HTTP) errors; ``{}`` when the server
    responded but found no labels.
    """
    qid = uri.rstrip("/").split("/")[-1]
    if not qid.startswith("Q"):
        return {}
    url = f"https://www.wikidata.org/w/rest.php/wikibase/v1/entities/items/{qid}/labels"
    try:
        with niquests.Session() as session:
            response = session.get(url, timeout=DEFAULT_TIMEOUT)
            response.raise_for_status()
    except niquests.exceptions.HTTPError as e:
        logger.debug("Wikidata HTTP error for %s: %s", uri, e)
        return {}  # Definitive server response — cache as empty
    except niquests.exceptions.RequestException as e:
        logger.warning("Wikidata labels fetch failed for %s: %s", uri, e)
        return None  # Transient — do not cache
    data = _parse_json(response, uri)
    if data is None:
        return {}
    return {lang: data[lang] for lang in languages if lang in data}


def _get_dbpedia_alt_labels(uri: str, languages: list[str]) -> dict[str, list[str]] | None:
    """Fetch SKOS altLabels for a DBpedia URI via the Data REST API."""
    data_uri = uri.replace("http://dbpedia.org/resource/", "https://dbpedia.org/data/") + ".json"
    try:
        with niquests.Session() as session:
            response = session.get(data_uri, timeout=DEFAULT_TIMEOUT)
            response.raise_for_status()
    except niquests.exceptions.HTTPError:
        return {}
    except niquests.exceptions.RequestException as e:
        logger.warning("DBpedia alt labels fetch failed for %s: %s", uri, e)
        return None
    data = _parse_json(response, uri)
    if data is None:
        return {}
    alts: dict[str, list[str]] = {}
    resource_data = data.get(uri, {})
    for entry in resource_data.get("http://www.w3.org/2004/02/skos/core#altLabel", []):
        lang = entry.get("lang", "")
        value = entry.get("value", "")
        if lang in languages and value:
            alts.setdefault(lang, []).append(value)
    return alts


def _get_wikidata_alt_labels(uri: str, languages: list[str]) -> dict[str, list[str]] | None:
    """Fetch aliases for a Wikidata item via the Wikibase REST API."""
    qid = uri.rstrip("/").split("/")[-1]
    if not qid.startswith("Q"):
        return {}
    url = f"https://www.wikidata.org/w/rest.php/wikibase/v1/entities/items/{qid}/aliases"
    try:
        with niquests.Session() as session:
            response = session.get(url, timeout=DEFAULT_TIMEOUT)
            response.raise_for_status()
    except niquests.exceptions.HTTPError:
        return {}
    except niquests.exceptions.RequestException as e:
        logger.warning("Wikidata aliases fetch failed for %s: %s", uri, e)
        return None
    data = _parse_json(response, uri)
    if data is None:
        return {}
    return {lang: data[lang] for lang in languages if lang in data and isinstance(data[lang], list)}


def _get_agrovoc_alt_labels(uri: str, languages: list[str]) -> dict[str, list[str]] | None:
    """Fetch SKOS altLabels for an AGROVOC URI.

    Uses the local Oxigraph store when available; falls back to the REST API.
    """
    if _agrovoc_store is not None:
        return _get_agrovoc_alt_labels_oxigraph(uri, languages, _agrovoc_store)
    reason = "still loading" if _agrovoc_loading else "store unavailable"
    logger.info("Querying AGROVOC REST API for alt labels (%s): %s", reason, uri)
    rest_base = _REST_ENDPOINTS["agrovoc"]
    url = f"{rest_base}/data/"
    params = {"uri": uri}
    try:
        with niquests.Session() as session:
            response = session.get(url, params=params, timeout=DEFAULT_TIMEOUT)
            response.raise_for_status()
    except niquests.exceptions.HTTPError:
        return {}
    except niquests.exceptions.RequestException as e:
        logger.warning("AGROVOC alt labels fetch failed for %s: %s", uri, e)
        return None
    data = _parse_json(response, uri)
    if data is None:
        return {}
    alts: dict[str, list[str]] = {}
    for item in data.get("graph", []):
        if item.get("uri") != uri:
            continue
        for al in item.get("altLabel", []):
            if isinstance(al, dict):
                lang = al.get("lang", "")
                value = al.get("value", "")
                if lang in languages and value:
                    alts.setdefault(lang, []).append(value)
    return alts
