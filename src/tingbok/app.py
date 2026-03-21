"""FastAPI application for tingbok."""

import asyncio
import json
import logging
import os
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import yaml
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi_mcp import FastApiMCP

from tingbok import __version__
from tingbok.models import HealthResponse, VocabularyConcept, VocabularyConceptUpdateRequest
from tingbok.routers import ean, skos
from tingbok.services import ean as ean_service
from tingbok.services import gpt as gpt_service
from tingbok.services import off as off_service
from tingbok.services import skos as skos_service

logger = logging.getLogger(__name__)

VOCABULARY_PATH = Path(__file__).parent / "data" / "vocabulary.yaml"
TINGBOK_BASE_URL = "https://tingbok.plann.no"

#: Root of the tingbok cache.  Set ``TINGBOK_CACHE_DIR`` to override.
_CACHE_BASE = Path(os.environ.get("TINGBOK_CACHE_DIR", str(Path.home() / ".cache" / "tingbok")))

#: Directory used for SKOS concept/label caches.
SKOS_CACHE_DIR: Path = _CACHE_BASE / "skos"

#: Directory used for EAN/product lookup caches.
EAN_CACHE_DIR: Path = _CACHE_BASE / "ean"

#: Path to the lookup-conflict warnings file.  Written whenever /api/lookup sees
#: sources disagree on a concept's top-level hierarchy root.
WARNINGS_PATH: Path = _CACHE_BASE / "lookup-warnings.json"

#: Runtime-writable JSON file for inventory-sourced EAN observations (category + name).
#: Lives next to vocabulary.yaml so observations persist across deployments.
EAN_OBSERVATIONS_PATH: Path = Path(__file__).parent / "data" / "ean-db.json"

#: Unix timestamp recorded when the application finished startup (overridden by lifespan).
_startup_time: float = time.time()

vocabulary: dict[str, Any] = {}

#: EAN observations loaded from ean-db.json (written by PUT /api/ean/{ean}).
ean_observations: dict[str, Any] = {}

#: Auto-discovered external source URIs for concepts that have none in vocabulary.yaml.
#: Maps concept_id -> {source_name: uri}.  Populated by _discover_source_uris_background().
_discovered_source_uris: dict[str, dict[str, str]] = {}

#: Labels fetched from external sources for each concept.
#: Maps concept_id -> {lang: label}.  Populated by _fetch_labels_background().
_fetched_labels: dict[str, dict[str, str]] = {}

#: Descriptions fetched from external sources for each concept.
#: Maps concept_id -> description string.  Populated by _fetch_labels_background().
_fetched_descriptions: dict[str, str] = {}

#: Alternative labels (synonyms) fetched from external sources for each concept.
#: Maps concept_id -> {lang: [altLabel, ...]}.  Populated by _fetch_labels_background().
_fetched_alt_labels: dict[str, dict[str, list[str]]] = {}

#: Concept IDs that have already had their labels/descriptions fetched (either by the
#: background task or by an on-demand fetch in get_vocabulary_concept).
_concepts_fetched: set[str] = set()

#: Reverse label cache for cross-language concept lookup.
#: Maps (label_lower, lang) -> (concept_id, labels, alts, source_uris, broader, description, wikipedia_url)
#: Populated when step-3 SKOS lookup succeeds so subsequent non-English lookups hit this cache
#: instead of re-querying all SKOS sources.
_skos_label_cache: dict[tuple[str, str], tuple] = {}

#: Languages to fetch from external sources in the background.
_DEFAULT_FETCH_LANGUAGES: list[str] = [
    "en",
    "nb",
    "nn",
    "da",
    "sv",
    "de",
    "fr",
    "es",
    "it",
    "nl",
    "pl",
    "ru",
    "uk",
    "fi",
    "bg",
]


#: Maps GPT top-level category labels (lowercased) to tingbok vocabulary root IDs.
_GPT_ROOT_MAPPING: dict[str, str] = {
    "animals & pet supplies": "pets",
    "apparel & accessories": "clothing",
    "arts & entertainment": "entertainment",
    "baby & toddler": "baby",
    "cameras & optics": "electronics",
    "electronics": "electronics",
    "food, beverages & tobacco": "food",
    "furniture": "furniture",
    "hardware": "hardware",
    "health & beauty": "health",
    "home & garden": "household",
    "luggage & bags": "bag",
    "media": "media",
    "office supplies": "office",
    "sporting goods": "sports",
    "toys & games": "entertainment",
    "vehicles & parts": "vehicle",
}


def _gpt_path_from_parts(path_parts: list[str]) -> str | None:
    """Derive a tingbok hierarchy path from a GPT taxonomy path_parts list.

    The first path part is mapped to a tingbok root via ``_GPT_ROOT_MAPPING``.
    Remaining parts are lowercased with spaces replaced by underscores.
    Returns ``None`` if the root is not recognised.
    """
    if not path_parts:
        return None
    root = _GPT_ROOT_MAPPING.get(path_parts[0].lower())
    if root is None:
        return None
    rest = [p.lower().replace(" & ", "_and_").replace(" ", "_") for p in path_parts[1:]]
    return "/".join([root] + rest)


def _load_vocabulary(path: Path | None = None) -> dict[str, Any]:
    """Load the package vocabulary from YAML.

    For concept IDs containing ``/``, ``broader`` is inferred from the path
    when not explicitly given (e.g. ``food/dairy`` → ``broader: [food]``).
    ``narrower`` for every concept is recomputed as the inverse of all
    ``broader`` relationships; explicit ``narrower`` entries that have no
    ``broader`` counterpart (e.g. ``_root.narrower``) are preserved as-is.
    """
    p = path or VOCABULARY_PATH
    with open(p) as f:
        data = yaml.safe_load(f)
    concepts: dict[str, Any] = data.get("concepts", {})

    # Pass 1: infer ``broader`` for path-style IDs that have none.
    for concept_id, entry in concepts.items():
        if entry is None:
            continue
        if "/" in concept_id and not entry.get("broader"):
            parent = "/".join(concept_id.split("/")[:-1])
            if parent in concepts:
                entry["broader"] = [parent]

    # Pass 2: compute ``narrower`` as the inverse of all ``broader`` links.
    computed_narrower: dict[str, list[str]] = {}
    for concept_id, entry in concepts.items():
        if entry is None:
            continue
        broader = entry.get("broader") or []
        if isinstance(broader, str):
            broader = [broader]
        for b in broader:
            lst = computed_narrower.setdefault(b, [])
            if concept_id not in lst:
                lst.append(concept_id)

    for concept_id, entry in concepts.items():
        if entry is None:
            continue
        computed = computed_narrower.get(concept_id)
        if computed:
            # Replace with computed children (keeps things consistent).
            entry["narrower"] = computed
        # Otherwise keep any explicit YAML narrower (e.g. _root ordering list).

    return concepts


async def _discover_source_uris_background() -> None:
    """Discover external source URIs for vocabulary concepts with no known sources.

    Queries DBpedia and Wikidata for each concept that lacks external source URIs
    in vocabulary.yaml.  Results are stored in ``_discovered_source_uris`` and merged
    into API responses at serving time.

    Only DBpedia and Wikidata are queried (not AGROVOC — too many false positives without
    the Oxigraph local store).  Sources listed in a concept's ``excluded_sources`` are
    skipped.  Concepts that already have at least one non-tingbok URI in ``source_uris``
    are also skipped.

    Results persist only in memory; they are rebuilt from the SKOS cache on next startup.
    """
    for concept_id, data in vocabulary.items():
        # Re-check AGROVOC availability each iteration — the store may finish
        # loading in the background partway through the discovery pass.
        # AGROVOC REST API has too many false positives, so only use it when
        # the local Oxigraph store is available.
        agrovoc_available = skos_service.get_agrovoc_store(SKOS_CACHE_DIR) is not None
        skos_sources = ("agrovoc", "dbpedia", "wikidata") if agrovoc_available else ("dbpedia", "wikidata")
        static_uris: list[str] = data.get("source_uris", [])
        excluded: set[str] = set(data.get("excluded_sources", []))

        # Skip if already has at least one non-tingbok external URI
        has_external = any(not u.startswith("https://tingbok.plann.no/") for u in static_uris)
        if has_external:
            continue

        label: str = data.get("prefLabel") or concept_id.split("/")[-1].replace("_", " ")
        discovered: dict[str, str] = {}

        for source in skos_sources:
            if source in excluded:
                continue
            try:
                concept = await asyncio.to_thread(skos_service.lookup_concept, label, "en", source, SKOS_CACHE_DIR)
                if concept and concept.get("uri"):
                    discovered[source] = concept["uri"]
            except Exception as exc:  # noqa: BLE001
                logger.debug("URI discovery failed for '%s' via %s: %s", concept_id, source, exc)

        # GPT: local taxonomy files in _CACHE_BASE/gpt/ (no network required)
        if "gpt" not in excluded:
            try:
                gpt_concept = await asyncio.to_thread(gpt_service.lookup_concept, label, "en", _CACHE_BASE)
                if gpt_concept and gpt_concept.get("uri"):
                    discovered["gpt"] = gpt_concept["uri"]
            except Exception as exc:  # noqa: BLE001
                logger.debug("GPT URI discovery failed for '%s': %s", concept_id, exc)

        # OFF: openfoodfacts package (food taxonomy only; no network calls at lookup time)
        if "off" not in excluded:
            try:
                off_concept = await asyncio.to_thread(off_service.lookup_concept, label, "en", SKOS_CACHE_DIR)
                if off_concept and off_concept.get("uri"):
                    discovered["off"] = off_concept["uri"]
            except Exception as exc:  # noqa: BLE001
                logger.debug("OFF URI discovery failed for '%s': %s", concept_id, exc)

        if discovered:
            _discovered_source_uris[concept_id] = discovered


async def _fetch_concept_labels(concept_id: str, data: dict[str, Any]) -> None:
    """Fetch labels, altLabels, and descriptions for a single vocabulary concept.

    Queries all known external source URIs for *concept_id* and merges the results
    into the module-level ``_fetched_labels``, ``_fetched_alt_labels``, and
    ``_fetched_descriptions`` dicts.  Marks the concept as done in
    ``_concepts_fetched`` on completion (even if no labels were found).

    Results are backed by the SKOS disk cache so network calls only happen on
    cache misses.
    """
    all_uris: list[str] = list(data.get("source_uris") or [])
    for uri in _discovered_source_uris.get(concept_id, {}).values():
        if uri not in all_uris:
            all_uris.append(uri)

    merged: dict[str, str] = {}
    merged_alts: dict[str, list[str]] = {}
    best_description: str | None = None

    for uri in all_uris:
        if uri.startswith(TINGBOK_BASE_URL):
            continue
        source = skos_service.uri_to_source(uri)
        if source is None:
            continue

        try:
            if source in ("agrovoc", "dbpedia", "wikidata"):
                fetched = await asyncio.to_thread(
                    skos_service.get_labels, uri, _DEFAULT_FETCH_LANGUAGES, source, SKOS_CACHE_DIR
                )
                fetched_alts = await asyncio.to_thread(
                    skos_service.get_alt_labels, uri, _DEFAULT_FETCH_LANGUAGES, source, SKOS_CACHE_DIR
                )
                desc = await asyncio.to_thread(skos_service.get_description, uri, source, "en", SKOS_CACHE_DIR)
                if desc and (best_description is None or len(desc) > len(best_description)):
                    best_description = desc
            elif source == "off":
                fetched = await asyncio.to_thread(off_service.get_labels, uri, _DEFAULT_FETCH_LANGUAGES)
                fetched_alts = await asyncio.to_thread(off_service.get_alt_labels, uri, _DEFAULT_FETCH_LANGUAGES)
            elif source == "gpt":
                fetched = await asyncio.to_thread(gpt_service.get_labels, uri, _DEFAULT_FETCH_LANGUAGES, _CACHE_BASE)
                fetched_alts = {}
            else:
                continue

            # First source wins for each language (preferred labels)
            for lang, label in fetched.items():
                if lang not in merged:
                    merged[lang] = label
            # Merge alt labels (accumulate across sources, deduplicate later)
            for lang, alts in fetched_alts.items():
                existing = merged_alts.setdefault(lang, [])
                for alt in alts:
                    if alt not in existing:
                        existing.append(alt)
        except Exception as exc:  # noqa: BLE001
            logger.debug("Label fetch failed for '%s' (%s): %s", concept_id, uri, exc)

    if merged:
        _fetched_labels[concept_id] = merged
    if merged_alts:
        _fetched_alt_labels[concept_id] = merged_alts
    if best_description:
        _fetched_descriptions[concept_id] = best_description
    _concepts_fetched.add(concept_id)


async def _fetch_labels_background() -> None:
    """Fetch labels and descriptions from source_uris for all vocabulary concepts.

    Delegates to :func:`_fetch_concept_labels` for each concept.  Results are
    rebuilt from the SKOS cache on every startup (expensive API calls only happen
    on cache misses, which are then cached for 60 days).
    """
    total = len(vocabulary)
    for i, (concept_id, data) in enumerate(vocabulary.items(), 1):
        if i % 10 == 0 or i == total:
            logger.info("Fetching labels [%d/%d]: %s", i, total, concept_id)
        await _fetch_concept_labels(concept_id, data)
    logger.info("Background label fetch complete (%d concepts)", total)


def _cache_refresh_config() -> tuple[float, float]:
    """Read cache refresh settings from environment variables.

    ``TINGBOK_CACHE_MAX_AGE_DAYS`` — how old (in days) the oldest entry must be
    before it is considered stale (default: 60).

    ``TINGBOK_CACHE_REFRESH_DIVISOR`` — controls sleep between refreshes;
    ``sleep = (max_age - age) / divisor`` (default: 100).
    """
    import os  # noqa: PLC0415

    max_age_days = float(os.environ.get("TINGBOK_CACHE_MAX_AGE_DAYS", "60"))
    divisor = float(os.environ.get("TINGBOK_CACHE_REFRESH_DIVISOR", "100"))
    return max_age_days * 86400, divisor


@asynccontextmanager
async def lifespan(app: FastAPI):  # noqa: ARG001
    """Load vocabulary on startup, then kick off background URI discovery and label fetching."""
    global vocabulary, ean_observations  # noqa: PLW0603
    vocabulary = _load_vocabulary()
    ean_observations = ean_service.load_ean_observations(EAN_OBSERVATIONS_PATH)
    skos_service.load_agrovoc_background(SKOS_CACHE_DIR)
    global _startup_time  # noqa: PLW0603
    _startup_time = time.time()
    max_age_seconds, divisor = _cache_refresh_config()
    discovery_task = asyncio.create_task(_discover_source_uris_background())
    labels_task = asyncio.create_task(_fetch_labels_background())
    refresh_task = asyncio.create_task(skos_service.cache_refresh_loop(SKOS_CACHE_DIR, max_age_seconds, divisor))
    try:
        yield
    finally:
        for task in (discovery_task, labels_task, refresh_task):
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass


app = FastAPI(
    title="tingbok",
    description="Product and category lookup service",
    version=__version__,
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.exception_handler(RequestValidationError)
async def _log_validation_error(request: Request, exc: RequestValidationError) -> JSONResponse:
    """Log the full validation error detail before returning 422."""
    logger.warning("422 validation error on %s %s: %s", request.method, request.url.path, exc.errors())
    return JSONResponse(status_code=422, content={"detail": exc.errors()})


app.include_router(skos.router, prefix="/api/skos", tags=["skos"])
app.include_router(ean.router, prefix="/api/ean", tags=["ean"])

_mcp = FastApiMCP(
    app,
    name="tingbok",
    description="Product and category lookup service for domestic inventory systems",
    exclude_operations=["health_health_get", "cache_stats_api_skos_cache_get"],
)
_mcp.mount_http()


@app.get("/", include_in_schema=False)
async def root(request: Request):
    """Root endpoint — returns HTML or JSON depending on Accept header."""
    accept = request.headers.get("accept", "")
    info = {
        "service": "tingbok",
        "version": __version__,
        "description": "Product and category lookup service for domestic inventory systems",
        "github": "https://github.com/tobixen/tingbok",
        "api_docs": f"{TINGBOK_BASE_URL}/docs",
    }
    if "text/html" in accept:
        html = f"""<!DOCTYPE html>
<html lang="en">
<head><meta charset="utf-8"><title>tingbok</title></head>
<body>
<h1>tingbok {__version__}</h1>
<p>Product and category lookup service for domestic inventory systems.</p>
<ul>
  <li><a href="https://github.com/tobixen/tingbok">GitHub repository</a></li>
  <li><a href="/docs">API documentation</a></li>
  <li><a href="/health">Health check</a></li>
</ul>
</body>
</html>"""
        return HTMLResponse(content=html)
    return JSONResponse(content=info)


def _oldest_cache_entry_age_days(*cache_dirs: Path) -> float | None:
    """Return age in days of the oldest (least recently used) entry across *cache_dirs*.

    Scans all ``*.json`` files (excluding ``_not_found.json``) and picks the
    smallest ``_last_accessed`` / ``_cached_at`` timestamp.  Returns ``None``
    if no cache files exist.
    """
    oldest_ts: float | None = None
    for cache_dir in cache_dirs:
        if not cache_dir.exists():
            continue
        for path in cache_dir.rglob("*.json"):
            if path.name.startswith("_"):
                continue
            try:
                data = json.loads(path.read_text(encoding="utf-8"))
            except Exception:  # noqa: BLE001
                continue
            ts = data.get("_last_accessed") or data.get("_cached_at")
            if ts is not None:
                if oldest_ts is None or ts < oldest_ts:
                    oldest_ts = ts
    if oldest_ts is None:
        return None
    return (time.time() - oldest_ts) / 86400


#: Lazy-built index for EAN category normalisation: normalised label → concept_id.
#: Rebuilt whenever the vocabulary changes.  ``None`` means not yet built.
_category_index: dict[str, str] | None = None


def _build_category_index() -> dict[str, str]:
    """Build a case-insensitive label → concept_id lookup from the loaded vocabulary.

    Covers prefLabels, static altLabels, and the final path segment of each
    concept ID (e.g. "caviar" → "food/caviar").  Concept IDs themselves are
    also included as exact matches.
    """
    index: dict[str, str] = {}
    for concept_id, data in vocabulary.items():
        if data is None:
            continue
        # Exact concept ID
        index[concept_id.lower()] = concept_id
        # Last path segment (e.g. "caviar" for "food/caviar")
        segment = concept_id.split("/")[-1].lower().replace("_", " ")
        index.setdefault(segment, concept_id)
        # prefLabel
        pref = data.get("prefLabel", "")
        if pref:
            index.setdefault(pref.lower(), concept_id)
        # Static altLabels
        for alts in (data.get("altLabel") or {}).values():
            for alt in alts:
                index.setdefault(alt.lower(), concept_id)
    return index


def _normalize_ean_categories(categories: list[str]) -> list[str]:
    """Normalize raw EAN category strings against the vocabulary.

    Each category is matched (case-insensitively) against vocabulary concept
    IDs, prefLabels, altLabels, and path-segment aliases.  Matched categories
    are replaced with the canonical concept ID; unmatched ones are kept as-is.
    """
    global _category_index  # noqa: PLW0603
    if not vocabulary:
        return categories
    if _category_index is None:
        _category_index = _build_category_index()
    result: list[str] = []
    for cat in categories:
        normalized = _category_index.get(cat.lower().strip())
        result.append(normalized if normalized is not None else cat)
    return result


@app.get("/health", response_model=HealthResponse)
async def health(request: Request):
    """Liveness check."""
    result = HealthResponse(
        version=__version__,
        uptime_seconds=time.time() - _startup_time,
        vocabulary_concepts=len(vocabulary),
        vocabulary_concepts_enriched=len(_concepts_fetched),
    )
    client_host = request.client.host if request.client else None
    if client_host in {"127.0.0.1", "::1", "localhost"}:
        result.paths = {
            "vocabulary": str(VOCABULARY_PATH),
            "ean_db": str(EAN_OBSERVATIONS_PATH),
            "skos_cache": str(SKOS_CACHE_DIR),
            "ean_cache": str(EAN_CACHE_DIR),
        }
        result.cache_oldest_entry_age_days = _oldest_cache_entry_age_days(SKOS_CACHE_DIR, EAN_CACHE_DIR)
    return result


def _normalise_uri(uri: str) -> str:
    """Normalise a source URI: upgrade http:// to https:// for known HTTPS hosts."""
    if uri.startswith("http://"):
        # All major LOD hubs serve over HTTPS; normalise unconditionally.
        return "https://" + uri[7:]
    return uri


def _build_source_uris(concept_id: str, data: dict[str, Any]) -> list[str]:
    """Build the full source_uris list for a concept.

    Combines static URIs from vocabulary.yaml with auto-discovered URIs.
    The canonical tingbok self-URI is exposed via the separate ``uri`` field
    and is excluded from ``source_uris`` to avoid redundancy.
    All http:// URIs are normalised to https://.
    """
    self_uri = f"{TINGBOK_BASE_URL}/api/vocabulary/{concept_id}"
    source_uris: list[str] = [_normalise_uri(u) for u in data.get("source_uris", []) if u != self_uri]
    # Merge in any auto-discovered URIs (values only; skip if already present)
    for uri in _discovered_source_uris.get(concept_id, {}).values():
        uri = _normalise_uri(uri)
        if uri not in source_uris and uri != self_uri:
            source_uris.append(uri)
    return source_uris


def _build_alt_labels(concept_id: str, data: dict[str, Any]) -> dict[str, list[str]]:
    """Build merged altLabels for a concept.

    Source-fetched synonyms are added to the static altLabel entries from
    vocabulary.yaml.  Duplicates (case-sensitive) and values that duplicate the
    prefLabel for that language are removed.
    """
    pref_label: str = data.get("prefLabel", concept_id)
    static: dict[str, list[str]] = data.get("altLabel") or {}
    fetched: dict[str, list[str]] = _fetched_alt_labels.get(concept_id) or {}

    merged: dict[str, list[str]] = {}
    all_langs = set(static) | set(fetched)
    for lang in all_langs:
        seen: set[str] = set()
        result: list[str] = []
        for alt in list(static.get(lang, [])) + list(fetched.get(lang, [])):
            if alt not in seen and alt != pref_label:
                seen.add(alt)
                result.append(alt)
        if result:
            merged[lang] = result
    return merged


def _build_labels(concept_id: str, data: dict[str, Any]) -> dict[str, str]:
    """Build the merged labels for a concept.

    Source-fetched labels provide the base.  ``prefLabel`` is treated as the
    canonical English label and overrides any source-fetched ``en`` value.
    Explicit ``labels:`` entries in vocabulary.yaml override everything.
    """
    merged = dict(_fetched_labels.get(concept_id, {}))
    if "prefLabel" in data:
        merged["en"] = data["prefLabel"]
    merged.update(data.get("labels", {}))
    return merged


def _build_description(concept_id: str, data: dict[str, Any]) -> str | None:
    """Return the description for a concept.

    Prefers the static description from vocabulary.yaml; falls back to the
    longest description fetched from external sources.
    """
    return data.get("description") or _fetched_descriptions.get(concept_id)


@app.get("/api/vocabulary")
async def get_vocabulary() -> dict[str, VocabularyConcept]:
    """Return the full package vocabulary.

    Returns 503 with a ``Retry-After`` header when the background label-fetch
    task has not yet processed all concepts, to avoid silently returning
    incomplete data.  Use ``GET /api/vocabulary/{concept_id}`` for individual
    concepts — that endpoint fetches labels on-demand.
    """
    from fastapi import HTTPException

    if len(_concepts_fetched) < len(vocabulary):
        remaining = len(vocabulary) - len(_concepts_fetched)
        raise HTTPException(
            status_code=503,
            detail=f"Vocabulary enrichment in progress ({remaining} concepts remaining); retry shortly.",
            headers={"Retry-After": "10"},
        )
    return {concept_id: _vocabulary_concept_from_data(concept_id, data) for concept_id, data in vocabulary.items()}


def _build_source_paths(data: dict[str, Any]) -> dict[str, str]:
    """Compute source-specific hierarchy paths for a vocabulary concept.

    Currently handles GPT: for each ``gpt:{id}`` URI found in ``source_uris``,
    looks up the GPT taxonomy to get the full path_parts and normalises them
    via :func:`_gpt_path_from_parts`.  Other sources may be added here later.
    """
    paths: dict[str, str] = {}
    for uri in data.get("source_uris", []):
        if not uri.startswith("gpt:"):
            continue
        gpt_concept = gpt_service.lookup_by_uri(uri, "en", _CACHE_BASE)
        if gpt_concept:
            gpt_path = _gpt_path_from_parts(gpt_concept.get("path_parts", []))
            if gpt_path:
                paths["gpt"] = gpt_path
    return paths


def _vocabulary_concept_from_data(concept_id: str, data: dict[str, Any]) -> VocabularyConcept:
    """Build a VocabularyConcept from a vocabulary.yaml entry."""
    broader = data.get("broader", [])
    if isinstance(broader, str):
        broader = [broader]
    return VocabularyConcept(
        id=concept_id,
        prefLabel=data.get("prefLabel", concept_id),
        altLabel=_build_alt_labels(concept_id, data),
        broader=broader,
        narrower=data.get("narrower", []),
        uri=f"{TINGBOK_BASE_URL}/api/vocabulary/{concept_id}",
        source_uris=_build_source_uris(concept_id, data),
        excluded_sources=data.get("excluded_sources", []),
        labels=_build_labels(concept_id, data),
        description=_build_description(concept_id, data),
        wikipediaUrl=data.get("wikipediaUrl"),
        source_paths=_build_source_paths(data),
        path_aliases=data.get("path_aliases") or {},
    )


def _best_vocabulary_anchored_path(paths: list[str], vocab: dict) -> str:
    """Pick the path whose longest prefix segment is present in the vocabulary.

    For each candidate path, walk up its parent segments (longest-first) and
    return the path whose deepest parent is anchored in *vocab*.  If two paths
    tie, the shorter (less specific) path wins so we stay closer to known
    vocabulary structure.  Falls back to ``paths[0]`` if no prefix is found.
    """

    def _score(path: str) -> tuple[int, int]:
        parts = path.split("/")
        for depth in range(len(parts) - 1, 0, -1):
            prefix = "/".join(parts[:depth])
            if prefix in vocab:
                return depth, -len(parts)  # higher depth wins; fewer parts breaks ties
        return 0, -len(parts)

    return max(paths, key=_score)


def _record_lookup_warning(label: str, source_roots: dict[str, str], source_paths: dict[str, list[str]]) -> None:
    """Write a source-conflict warning for *label* to ``WARNINGS_PATH``.

    Called when two or more sources return hierarchy paths whose top-level root
    differs (e.g. AGROVOC says ``livestock/bedding`` while DBpedia says
    ``household/bedding``), which indicates a likely semantic mismatch.
    """
    try:
        data: dict = {}
        if WARNINGS_PATH.exists():
            try:
                data = json.loads(WARNINGS_PATH.read_text(encoding="utf-8"))
            except Exception:  # noqa: BLE001
                pass
        data[label] = {
            "roots_per_source": source_roots,
            "paths_per_source": source_paths,
            "last_seen": time.strftime("%Y-%m-%d"),
        }
        WARNINGS_PATH.parent.mkdir(parents=True, exist_ok=True)
        WARNINGS_PATH.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to write lookup warning for %r: %s", label, exc)


@app.get("/api/vocabulary/{concept_id:path}")
async def get_vocabulary_concept(concept_id: str) -> VocabularyConcept:
    """Return a single concept from the package vocabulary.

    If labels have not yet been fetched for this concept by the background task,
    they are fetched on-demand before the response is built.
    """
    from fastapi import HTTPException

    data = vocabulary.get(concept_id)
    if data is None:
        raise HTTPException(status_code=404, detail=f"Concept '{concept_id}' not found")
    if concept_id not in _concepts_fetched:
        await _fetch_concept_labels(concept_id, data)
    return _vocabulary_concept_from_data(concept_id, data)


def _write_vocabulary_concept_update(
    concept_id: str,
    body: VocabularyConceptUpdateRequest,
    vocab_path: Path,
) -> None:
    """Apply *body* to *concept_id* in *vocab_path* using ruamel.yaml.

    Creates the concept (and any missing ancestor concepts in the path) if
    they do not yet exist.  Preserves all existing comments and formatting.
    """
    try:
        from ruamel.yaml import YAML  # noqa: PLC0415
    except ImportError as exc:
        raise RuntimeError("ruamel.yaml is required for vocabulary writes") from exc

    yaml_rw = YAML()
    yaml_rw.preserve_quotes = True

    if vocab_path.exists():
        with open(vocab_path) as f:
            doc = yaml_rw.load(f)
    else:
        doc = {"concepts": {}}

    concepts: dict = doc.setdefault("concepts", {})
    parts = concept_id.split("/")

    # Ensure all ancestor concepts exist.
    for depth in range(1, len(parts)):
        ancestor_id = "/".join(parts[:depth])
        if ancestor_id not in concepts or concepts[ancestor_id] is None:
            label = parts[depth - 1].replace("_", " ").replace("-", " ").title()
            concepts[ancestor_id] = {"prefLabel": label}

    # Ensure the concept itself exists.
    if concept_id not in concepts or concepts[concept_id] is None:
        label = parts[-1].replace("_", " ").replace("-", " ").title()
        concepts[concept_id] = {"prefLabel": label}

    entry = concepts[concept_id]

    if body.prefLabel is not None:
        entry["prefLabel"] = body.prefLabel
    if body.labels:
        if "labels" not in entry or entry["labels"] is None:
            entry["labels"] = {}
        entry["labels"].update(body.labels)
    if body.altLabel:
        if "altLabel" not in entry or entry["altLabel"] is None:
            entry["altLabel"] = {}
        for lang, alts in body.altLabel.items():
            if lang not in entry["altLabel"]:
                entry["altLabel"][lang] = []
            for alt in alts:
                if alt not in entry["altLabel"][lang]:
                    entry["altLabel"][lang].append(alt)
    if body.add_source_uris:
        if "source_uris" not in entry or entry["source_uris"] is None:
            entry["source_uris"] = []
        for uri in body.add_source_uris:
            if uri not in entry["source_uris"]:
                entry["source_uris"].append(uri)
    if body.remove_source_uris:
        entry["source_uris"] = [u for u in (entry.get("source_uris") or []) if u not in body.remove_source_uris]
    if body.add_excluded_sources:
        if "excluded_sources" not in entry or entry["excluded_sources"] is None:
            entry["excluded_sources"] = []
        for src in body.add_excluded_sources:
            if src not in entry["excluded_sources"]:
                entry["excluded_sources"].append(src)
    if body.remove_excluded_sources:
        entry["excluded_sources"] = [
            s for s in (entry.get("excluded_sources") or []) if s not in body.remove_excluded_sources
        ]

    with open(vocab_path, "w") as f:
        yaml_rw.dump(doc, f)


@app.put("/api/vocabulary/{concept_id:path}", response_model=VocabularyConcept)
async def put_vocabulary_concept(concept_id: str, body: VocabularyConceptUpdateRequest) -> VocabularyConcept:
    """Create or update a vocabulary concept.

    Creates the concept entry (and any missing ancestors in the path hierarchy)
    if it does not yet exist.  All body fields are optional; omitted fields
    leave existing data unchanged.

    Changes are persisted to ``vocabulary.yaml`` and immediately reflected in
    the in-memory vocabulary so subsequent GET requests see the updated data.
    """
    global vocabulary, _category_index  # noqa: PLW0603

    await asyncio.to_thread(_write_vocabulary_concept_update, concept_id, body, VOCABULARY_PATH)

    # Reload so path-inference and narrower computation are consistent.
    vocabulary = _load_vocabulary()
    _category_index = None

    data = vocabulary.get(concept_id)
    if data is None:
        from fastapi import HTTPException  # noqa: PLC0415

        raise HTTPException(status_code=500, detail="Concept write succeeded but could not be read back")

    return _vocabulary_concept_from_data(concept_id, data)


@app.get("/api/lookup/{label:path}")
async def lookup_concept(
    label: str,
    lang: str = "en",
) -> VocabularyConcept:
    """Look up a concept by label or ID, merging data from all available sources.

    1. If ``label`` matches a vocabulary concept ID or prefLabel/altLabel → return
       that concept (already enriched with external-source data in the background).
    2. Otherwise query AGROVOC, DBpedia and Wikidata **in parallel**, merge labels,
       altLabels, descriptions and source URIs from all sources, and derive the
       canonical concept ID from the hierarchy path.  Returns 404 only when no
       source finds the label.
    """
    from fastapi import HTTPException

    # 1. Exact concept-ID match in vocabulary
    data = vocabulary.get(label)
    if data is not None:
        return _vocabulary_concept_from_data(label, data)

    # 1.5. Language-specific path alias match (e.g. "klær/vinter" → clothing/thermal
    #      when lang=nb).  Checked before generic label matching so that a foreign-
    #      language path never accidentally hits an English concept with the same text.
    #      Both "no" and "nb" are treated as equivalent (both refer to Norwegian Bokmål).
    label_lower = label.lower()
    _nb_langs = {"nb", "no", "nn"}

    def _alias_lang_matches(alias_lang: str, req_lang: str) -> bool:
        if alias_lang == req_lang:
            return True
        # Treat nb/no/nn as the same group
        return alias_lang in _nb_langs and req_lang in _nb_langs

    if "/" in label:  # only full-path labels can be path aliases
        for concept_id, vdata in vocabulary.items():
            for alias_lang, aliases in (vdata.get("path_aliases") or {}).items():
                if _alias_lang_matches(alias_lang, lang):
                    if label_lower in [a.lower() for a in aliases]:
                        return _vocabulary_concept_from_data(concept_id, vdata)

    # 2. Match by prefLabel, altLabel, or runtime-fetched labels/altLabels
    #    in vocabulary (case-insensitive).
    #    This also catches e.g. "spices" → food/spices when Wikidata has returned
    #    "Spices" as an altLabel for that concept at runtime, even though it is not
    #    listed in vocabulary.yaml.
    for concept_id, vdata in vocabulary.items():
        if vdata.get("prefLabel", "").lower() == label_lower:
            return _vocabulary_concept_from_data(concept_id, vdata)
        # Static altLabels from vocabulary.yaml
        for alts in (vdata.get("altLabel") or {}).values():
            if label_lower in [a.lower() for a in alts]:
                return _vocabulary_concept_from_data(concept_id, vdata)
        # Runtime-fetched altLabels from external sources (Wikidata, DBpedia, …)
        for alts in (_fetched_alt_labels.get(concept_id) or {}).values():
            if label_lower in [a.lower() for a in alts]:
                return _vocabulary_concept_from_data(concept_id, vdata)
        # Runtime-fetched translated prefLabels from external sources
        for lbl in (_fetched_labels.get(concept_id) or {}).values():
            if lbl.lower() == label_lower:
                return _vocabulary_concept_from_data(concept_id, vdata)

    # 2.5. Check reverse label cache populated by previous successful SKOS lookups.
    #      This allows e.g. "skrivemaskin?lang=nb" to find a concept previously
    #      resolved via "typewriter?lang=en" without re-querying all SKOS sources.
    cached_entry = _skos_label_cache.get((label_lower, lang))
    if cached_entry is not None:
        c_id, c_labels, c_alts, c_uris, c_broader, c_desc, c_wiki = cached_entry
        return VocabularyConcept(
            id=c_id,
            prefLabel=c_labels.get(lang, label),
            source_uris=c_uris,
            broader=c_broader,
            labels=c_labels,
            altLabel=c_alts,
            description=c_desc,
            wikipediaUrl=c_wiki,
        )

    # 3. Query all SKOS sources in parallel, merge results
    skos_sources = ("agrovoc", "dbpedia", "wikidata")
    fetch_languages = _DEFAULT_FETCH_LANGUAGES

    async def _fetch_source(source: str) -> tuple[dict | None, list[str], list[str]]:
        """Return (concept, paths, source_uris) for one source; never raises."""
        try:
            concept = await asyncio.to_thread(skos_service.lookup_concept, label, lang, source, SKOS_CACHE_DIR)
            if not concept:
                return None, [], []
            uri = concept.get("uri") or ""
            paths, found, _ = await asyncio.to_thread(
                skos_service.build_hierarchy_paths, label, lang, source, SKOS_CACHE_DIR
            )
            return concept, (paths if found else []), ([uri] if uri else [])
        except Exception as exc:
            logger.debug("Lookup failed for '%s' via %s: %s", label, source, exc)
            return None, [], []

    results = await asyncio.gather(*(_fetch_source(s) for s in skos_sources))

    # Merge across sources
    merged_labels: dict[str, str] = {}
    merged_alts: dict[str, list[str]] = {}
    source_uris: list[str] = []
    descriptions: list[str] = []
    wikipedia_url: str | None = None
    concept_id: str | None = None
    pref_label: str = label
    all_paths: list[str] = []  # every path from every source, for multi-path broader

    for source, (concept, paths, uris) in zip(skos_sources, results, strict=False):
        if concept is None:
            continue

        # Collect source URIs (normalise http → https)
        for uri in uris:
            uri = _normalise_uri(uri)
            if uri and uri not in source_uris:
                source_uris.append(uri)

        # Accumulate all paths; canonical ID chosen below once all sources are merged
        for p in paths:
            if p not in all_paths:
                all_paths.append(p)

        # Collect prefLabel (en wins if available)
        if lang not in merged_labels:
            pref_label = concept.get("prefLabel", label)
            merged_labels[lang] = pref_label

        # Fetch labels + altLabels for all languages
        uri = concept.get("uri")
        if uri:
            fetched = await asyncio.to_thread(skos_service.get_labels, uri, fetch_languages, source, SKOS_CACHE_DIR)
            for lg, lbl in fetched.items():
                merged_labels.setdefault(lg, lbl)

            fetched_alts = await asyncio.to_thread(
                skos_service.get_alt_labels, uri, fetch_languages, source, SKOS_CACHE_DIR
            )
            for lg, alts in fetched_alts.items():
                existing = merged_alts.setdefault(lg, [])
                for alt in alts:
                    if alt not in existing:
                        existing.append(alt)

            desc = await asyncio.to_thread(skos_service.get_description, uri, source, lang, SKOS_CACHE_DIR)
            if desc:
                descriptions.append(desc)

        if not wikipedia_url:
            wikipedia_url = concept.get("wikipediaUrl")

    # Also query GPT (local taxonomy, no network) — provides product hierarchy paths
    gpt_concept = await asyncio.to_thread(gpt_service.lookup_concept, label, lang, _CACHE_BASE)
    if gpt_concept:
        gpt_uri = gpt_concept.get("uri", "")
        if gpt_uri and gpt_uri not in source_uris:
            source_uris.append(gpt_uri)
        gpt_path = _gpt_path_from_parts(gpt_concept.get("path_parts", []))
        if gpt_path and concept_id is None:
            concept_id = gpt_path
        gpt_labels = await asyncio.to_thread(gpt_service.get_labels, gpt_uri, fetch_languages, _CACHE_BASE)
        for lg, lbl in gpt_labels.items():
            merged_labels.setdefault(lg, lbl)

    # Also query OFF (local food taxonomy, no network) — URI and multilingual labels only;
    # hierarchy path building from OFF is deferred to future work since AGROVOC covers food.
    off_concept = await asyncio.to_thread(off_service.lookup_concept, label, lang, SKOS_CACHE_DIR)
    if off_concept:
        off_uri = off_concept.get("uri", "")
        if off_uri and off_uri not in source_uris:
            source_uris.append(off_uri)
        off_labels = await asyncio.to_thread(off_service.get_labels, off_uri, fetch_languages)
        for lg, lbl in off_labels.items():
            merged_labels.setdefault(lg, lbl)
        off_alts = await asyncio.to_thread(off_service.get_alt_labels, off_uri, fetch_languages)
        for lg, alts in off_alts.items():
            existing = merged_alts.setdefault(lg, [])
            for alt in alts:
                if alt not in existing:
                    existing.append(alt)

    if not source_uris and not all_paths:
        raise HTTPException(status_code=404, detail=f"Concept '{label}' not found in vocabulary or SKOS sources")

    # Detect semantic conflicts: if two or more sources found paths but under different
    # top-level roots, record a warning so the operator can add excluded_sources entries.
    warn_roots: dict[str, str] = {}
    warn_paths: dict[str, list[str]] = {}
    for src, (_, src_paths, _) in zip(skos_sources, results, strict=False):
        if src_paths:
            warn_roots[src] = src_paths[0].split("/")[0]
            warn_paths[src] = src_paths
    if len(set(warn_roots.values())) > 1:
        _record_lookup_warning(label, warn_roots, warn_paths)

    # Pick vocabulary-anchored canonical path as concept ID; fall back to label slug
    if all_paths:
        concept_id = _best_vocabulary_anchored_path(all_paths, vocabulary)
    if concept_id is None:
        concept_id = label_lower.replace(" ", "_")

    # Build broader from ALL paths so every hierarchy link is preserved
    # (one concept, multiple paths — each path contributes its direct parent)
    broader_set: list[str] = []
    for p in all_paths:
        parent = "/".join(p.split("/")[:-1])
        if parent and parent not in broader_set:
            broader_set.append(parent)
    broader = broader_set if broader_set else (["/".join(concept_id.split("/")[:-1])] if "/" in concept_id else [])
    best_description = max(descriptions, key=len) if descriptions else None

    # Populate reverse label cache so future non-English lookups can find this concept
    # without re-querying all SKOS sources.
    cache_entry = (concept_id, merged_labels, merged_alts, source_uris, broader, best_description, wikipedia_url)
    for lg, lbl in merged_labels.items():
        _skos_label_cache[(lbl.lower(), lg)] = cache_entry

    return VocabularyConcept(
        id=concept_id,
        prefLabel=merged_labels.get(lang, pref_label),
        source_uris=source_uris,
        broader=broader,
        labels=merged_labels,
        altLabel=merged_alts,
        description=best_description,
        wikipediaUrl=wikipedia_url,
    )
