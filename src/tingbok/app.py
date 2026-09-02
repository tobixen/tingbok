"""FastAPI application for tingbok."""

import asyncio
import json
import logging
import os
import shutil
import signal
import subprocess
import sys
import time
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import yaml
from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from fastmcp import FastMCP
from fastmcp.server.providers.openapi import MCPType, RouteMap

from tingbok import __version__
from tingbok.models import (
    AncestorsResponse,
    HealthResponse,
    SourceInfo,
    SourcesResponse,
    VocabularyConcept,
    VocabularyConceptUpdateRequest,
    VocabularyResolveRequest,
    VocabularyResolveResponse,
)
from tingbok.routers import ean, skos
from tingbok.services import ean as ean_service
from tingbok.services import gpt as gpt_service
from tingbok.services import off as off_service
from tingbok.services import skos as skos_service
from tingbok.sources import SOURCES, skos_lookup_sources
from tingbok.text import number_variations
from tingbok.vocabulary_file import coerce_scalar_source_uris

logger = logging.getLogger(__name__)

TINGBOK_BASE_URL = "https://tingbok.plann.no"

#: Root of the tingbok cache.  Set ``TINGBOK_CACHE_DIR`` to override.
_CACHE_BASE = Path(os.environ.get("TINGBOK_CACHE_DIR", str(Path.home() / ".cache" / "tingbok")))

#: Optional writable data directory for vocabulary.yaml and ean-db.json.
#: Set ``TINGBOK_DATA_DIR`` to enable git-tracked auto-commits on writes.
_DATA_BASE: Path | None = Path(os.environ["TINGBOK_DATA_DIR"]) if os.environ.get("TINGBOK_DATA_DIR") else None

#: Path to the runtime-writable vocabulary file.
VOCABULARY_PATH: Path = (
    (_DATA_BASE / "vocabulary.yaml") if _DATA_BASE else Path(__file__).parent / "data" / "vocabulary.yaml"
)

#: Directory used for SKOS concept/label caches.
SKOS_CACHE_DIR: Path = _CACHE_BASE / "skos"

#: Directory used for EAN/product lookup caches.
EAN_CACHE_DIR: Path = _CACHE_BASE / "ean"

#: Path to the lookup-conflict warnings file.  Written whenever /api/lookup sees
#: sources disagree on a concept's top-level hierarchy root.
WARNINGS_PATH: Path = _CACHE_BASE / "lookup-warnings.json"

#: Runtime-writable JSON file for inventory-sourced EAN observations (category + name).
#: Lives next to vocabulary.yaml so observations persist across deployments.
EAN_OBSERVATIONS_PATH: Path = (
    (_DATA_BASE / "ean-db.json") if _DATA_BASE else Path(__file__).parent / "data" / "ean-db.json"
)

#: Unix timestamp recorded when the application finished startup (overridden by lifespan).
_startup_time: float = time.time()

#: Debounce state for git auto-commits triggered by data writes.
_pending_commit_ips: set[str] = set()
_pending_commit_task: asyncio.Task | None = None
_GIT_DEBOUNCE_SECONDS: float = float(os.environ.get("TINGBOK_GIT_DEBOUNCE_SECONDS", "10"))

vocabulary: dict[str, Any] = {}

#: Reverse map from normalised source URI to vocabulary concept_id.
#: Rebuilt whenever *vocabulary* is reloaded.  Used for cross-taxonomy URI bridging
#: so that SKOS hierarchy paths whose segments carry Wikidata URIs matching a known
#: vocabulary concept can be linked to that concept.
_vocab_uri_index: dict[str, str] = {}

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


#: Language fallback chains for closely related languages.
#: When a SKOS lookup fails for the primary language, these languages are tried in order.
#: Covers Scandinavian variants where the same word may be indexed under a sibling code.
_LANGUAGE_FALLBACKS: dict[str, list[str]] = {
    "nb": ["no", "da", "nn", "sv"],
    "no": ["nb", "da", "nn", "sv"],
    "nn": ["nb", "no", "da", "sv"],
    "da": ["nb", "no", "nn", "sv"],
    "sv": ["da", "nb", "no", "nn"],
}


def _fallback_langs(lang: str) -> list[str]:
    """Fallback language order to try after *lang*, always ending with English.

    Lookup labels are English-derived concept ids (e.g. ``mushroom_hunting``), so
    English is the universal backstop for any language whose own index does not
    contain the (English) query string — without it, a source that only matches in
    English is silently dropped and the merged result is poorer (see
    docs/language-fallback-findings.md). English itself has no fallback.
    """
    chain = list(_LANGUAGE_FALLBACKS.get(lang, []))
    if lang != "en" and "en" not in chain:
        chain.append("en")
    return chain


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


def _git_commit_data(data_dir: Path, ips: frozenset[str] = frozenset()) -> None:
    """Stage and commit changed data files in *data_dir* using git.

    Only ``ean-db.json`` and ``vocabulary.yaml`` are considered.  Does nothing
    if neither file exists or neither has any uncommitted changes.  Runs
    synchronously; call via ``asyncio.to_thread`` from async contexts.
    """

    def run(*cmd: str) -> subprocess.CompletedProcess[str]:
        return subprocess.run(cmd, cwd=data_dir, capture_output=True, text=True)

    known = ["ean-db.json", "vocabulary.yaml"]
    existing = [f for f in known if (data_dir / f).exists()]
    if not existing:
        return
    if not run("git", "status", "--porcelain", *existing).stdout.strip():
        return  # nothing changed
    run("git", "add", *existing)
    ip_str = f" (from {', '.join(sorted(ips))})" if ips else ""
    result = run(
        "git",
        "commit",
        "-m",
        f"auto-commit: data update {time.strftime('%Y-%m-%dT%H:%M:%S')}{ip_str}",
        "--author=tingbok <tingbok@localhost>",
    )
    if result.returncode == 0:
        logger.info("Git auto-commit: %s", result.stdout.strip().splitlines()[0])
    else:
        logger.warning("Git auto-commit failed: %s", result.stderr.strip())


def _schedule_git_commit(ip: str | None = None) -> None:
    """Schedule a debounced git commit of data files.

    Each call resets the debounce timer.  The commit runs
    ``_GIT_DEBOUNCE_SECONDS`` after the last call.  Does nothing when
    ``_DATA_BASE`` is not configured.
    """
    global _pending_commit_task  # noqa: PLW0603
    if not _DATA_BASE:
        return
    if ip:
        _pending_commit_ips.add(ip)
    if _pending_commit_task is not None and not _pending_commit_task.done():
        _pending_commit_task.cancel()
    _pending_commit_task = asyncio.create_task(_do_git_commit_after_delay())


async def _do_git_commit_after_delay() -> None:
    """Coroutine: wait for the debounce window then commit."""
    await asyncio.sleep(_GIT_DEBOUNCE_SECONDS)
    ips = frozenset(_pending_commit_ips)
    _pending_commit_ips.clear()
    if _DATA_BASE:
        await asyncio.to_thread(_git_commit_data, _DATA_BASE, ips)


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
    """Load the vocabulary from YAML.

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

    # Pass 0: normalise the document's shape (see tingbok.vocabulary_file).
    coerce_scalar_source_uris(concepts)

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
        skos_sources = (
            skos_lookup_sources() if agrovoc_available else tuple(n for n in skos_lookup_sources() if n != "agrovoc")
        )
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
            if source in skos_lookup_sources():
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
    on cache misses, which are then cached for 90 days).
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
    before it is considered stale (default: 90).

    ``TINGBOK_CACHE_REFRESH_DIVISOR`` — controls sleep between refreshes;
    ``sleep = (max_age - age) / divisor`` (default: 200).
    """
    import os  # noqa: PLC0415

    max_age_days = float(os.environ.get("TINGBOK_CACHE_MAX_AGE_DAYS", "90"))
    divisor = float(os.environ.get("TINGBOK_CACHE_REFRESH_DIVISOR", "200"))
    return max_age_days * 86400, divisor


def _toggle_log_level(signum: int, frame: object) -> None:  # noqa: ARG001
    """Toggle the tingbok logger between INFO and DEBUG on SIGUSR1.

    Send ``kill -USR1 <pid>`` to enable debug logging; send it again to disable.
    The PID is available via ``systemctl show -p MainPID tingbok``.
    """
    app_logger = logging.getLogger("tingbok")
    if app_logger.level == logging.DEBUG:
        app_logger.setLevel(logging.INFO)
        logger.info("SIGUSR1: debug logging disabled. Send SIGUSR1 again to re-enable.")
    else:
        app_logger.setLevel(logging.DEBUG)
        logger.info("SIGUSR1: debug logging enabled. Send SIGUSR1 again to disable.")


def bootstrap_data_dir() -> None:
    """Seed a configured ``TINGBOK_DATA_DIR`` with the packaged vocabulary.

    Only does anything when ``TINGBOK_DATA_DIR`` is set and the file is not
    there yet.  Both entry points need it before they load: the service through
    its lifespan, and :mod:`tingbok.embedded` on first use — an embedded client
    is typically running on a host where no service ever started, so there is
    nobody else to have created the file.
    """
    if _DATA_BASE and not VOCABULARY_PATH.exists():
        _DATA_BASE.mkdir(parents=True, exist_ok=True)
        _pkg_vocab = Path(__file__).parent / "data" / "vocabulary.yaml"
        shutil.copy(_pkg_vocab, VOCABULARY_PATH)
        logger.info("Bootstrapped vocabulary.yaml to %s", VOCABULARY_PATH)


@asynccontextmanager
async def lifespan(app: FastAPI):  # noqa: ARG001
    """Load vocabulary on startup, then kick off background URI discovery and label fetching."""
    global vocabulary, ean_observations  # noqa: PLW0603

    bootstrap_data_dir()
    vocabulary = _load_vocabulary()
    global _vocab_uri_index  # noqa: PLW0603
    _vocab_uri_index = _build_vocab_uri_index(vocabulary)
    ean_observations = ean_service.load_ean_observations(EAN_OBSERVATIONS_PATH)
    skos_service.load_agrovoc_background(SKOS_CACHE_DIR)
    global _startup_time  # noqa: PLW0603
    _startup_time = time.time()

    # Ensure application-level INFO+ messages appear in the journal.
    # Uvicorn configures its own loggers but leaves tingbok.* silenced at root level.
    _app_logger = logging.getLogger("tingbok")
    if not _app_logger.handlers:
        _handler = logging.StreamHandler()
        _handler.setFormatter(logging.Formatter("%(levelname)s:%(name)s:%(message)s"))
        _app_logger.addHandler(_handler)
        _app_logger.propagate = False
    _app_logger.setLevel(logging.INFO)

    signal.signal(signal.SIGUSR1, _toggle_log_level)
    logger.info("SIGUSR1 handler registered. Send 'kill -USR1 %d' to toggle debug logging.", os.getpid())

    max_age_seconds, divisor = _cache_refresh_config()
    discovery_task = asyncio.create_task(_discover_source_uris_background())
    labels_task = asyncio.create_task(_fetch_labels_background())
    refresh_task = asyncio.create_task(skos_service.cache_refresh_loop(SKOS_CACHE_DIR, max_age_seconds, divisor))
    try:
        # The MCP app mounted at /mcp owns a session manager that only exists
        # for the duration of its own lifespan; a mount does not run it, so it
        # is entered here or /mcp is dead on arrival.
        async with _mcp_app.lifespan(app):
            yield
    finally:
        for task in (discovery_task, labels_task, refresh_task):
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        # Force-commit any pending data changes before exit (don't lose the last write).
        if _DATA_BASE:
            if _pending_commit_task is not None and not _pending_commit_task.done():
                _pending_commit_task.cancel()
            await asyncio.to_thread(_git_commit_data, _DATA_BASE, frozenset(_pending_commit_ips))
            _pending_commit_ips.clear()


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

    Covers prefLabels, static altLabels, the final path segment of each concept
    ID (e.g. "caviar" → "food/caviar"), and translated-root paths: for every
    path concept like ``food/baking``, altLabels of the root concept (e.g. "mat"
    for "food") are combined with the rest of the path to add entries such as
    "mat/baking" → "food/baking".
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

    # Second pass: add translated-root path entries for path-style concept IDs.
    # E.g. "mat" is the nb altLabel of "food", so index "mat/baking" → "food/baking".
    for concept_id, data in vocabulary.items():
        if data is None or "/" not in concept_id:
            continue
        root, rest = concept_id.split("/", 1)
        root_data = vocabulary.get(root)
        if root_data is None:
            continue
        for alts in (root_data.get("altLabel") or {}).values():
            for alt in alts:
                index.setdefault(f"{alt.lower()}/{rest}", concept_id)

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
            "module": str(Path(__file__)),
            "executable": sys.executable,
            **({"data_dir": str(_DATA_BASE)} if _DATA_BASE else {}),
        }
        result.cache_oldest_entry_age_days = _oldest_cache_entry_age_days(SKOS_CACHE_DIR, EAN_CACHE_DIR)
        if skos_service._next_refresh_at is not None:
            result.cache_next_refresh_in_seconds = max(0.0, skos_service._next_refresh_at - time.time())
    return result


def _normalise_uri(uri: str) -> str:
    """Normalise a source URI: upgrade http:// to https:// for known HTTPS hosts."""
    if uri.startswith("http://"):
        # All major LOD hubs serve over HTTPS; normalise unconditionally.
        return "https://" + uri[7:]
    return uri


def _build_vocab_uri_index(vocab: dict[str, Any]) -> dict[str, str]:
    """Build a reverse map from normalised source URI to vocabulary concept_id."""
    idx: dict[str, str] = {}
    for concept_id, vdata in vocab.items():
        for uri in vdata.get("source_uris") or []:
            n = _normalise_uri(uri)
            if n and n not in idx:
                idx[n] = concept_id
    return idx


def _normalise_self_id(concept_id: str) -> str:
    """Normalise a concept ID for self-reference comparison.

    Folds case, unifies separators and strips a trailing plural ``s`` from the
    final path segment, so that ``rope``/``Rope``/``ropes`` and
    ``lentil``/``lentils`` all compare equal.  Used only to decide whether a
    candidate broader entry is really the concept itself under trivial spelling
    variation — a concept must never be its own ancestor.
    """
    folded = concept_id.casefold().replace("_", " ").replace("-", " ").strip()
    segments = folded.split("/")
    last = segments[-1]
    if len(last) > 3 and last.endswith("s"):
        segments[-1] = last[:-1]
    return "/".join(segments)


def _build_broader_from_paths(
    all_paths: list[str],
    uri_map: dict[str, str],
    uri_index: dict[str, str],
    self_ids: set[str],
    fallback_concept_id: str | None = None,
) -> list[str]:
    """Build a broader list from SKOS hierarchy paths and URI map.

    Args:
        all_paths: Hierarchy paths from build_hierarchy_paths (combined across sources).
        uri_map: Maps path-segment keys to source URIs (combined across sources).
        uri_index: Maps normalised source URIs to concept IDs for bridging.
        self_ids: Concept IDs to exclude — prevents self-referential broader entries.
            Matching is normalisation-aware (case-, separator- and plural-insensitive)
            so a concept never becomes its own ancestor via a spelling variant such as
            ``rope`` -> ``Rope`` or ``lentil`` -> ``lentils``.
        fallback_concept_id: When all_paths is empty and this has a "/", its parent
            path is used as the sole broader entry.
    """
    self_norms = {_normalise_self_id(s) for s in self_ids}

    def _is_self(candidate: str) -> bool:
        return _normalise_self_id(candidate) in self_norms

    broader: list[str] = []
    for p in all_paths:
        parent = "/".join(p.split("/")[:-1])
        if parent and parent not in broader and not _is_self(parent):
            broader.append(parent)

    if not broader and fallback_concept_id and "/" in fallback_concept_id:
        parent = "/".join(fallback_concept_id.split("/")[:-1])
        if parent and not _is_self(parent):
            broader.append(parent)

    for _path_seg, seg_uri in uri_map.items():
        bridged = uri_index.get(_normalise_uri(seg_uri))
        if not bridged:
            bridged = _concept_id_from_path_seg(_path_seg)
        if bridged and not _is_self(bridged) and bridged not in broader:
            broader.append(bridged)

    return broader


def _concept_id_from_path_seg(path_seg: str) -> str | None:
    """Derive a concept ID from a uri_map path-segment key if its root is a vocabulary concept.

    uri_map keys from skos_service.build_hierarchy_paths are full path prefixes built with
    _normalize_label (underscores), e.g. "food/condiments/oil/cooking_oil".  The last
    component is the concept's own normalised label; converting underscores to hyphens yields
    the concept ID ("cooking-oil").

    Returns None when the path root is not a vocabulary concept (filters out Wikidata
    encyclopedic paths like "juridical_person/..." and "primary_commodity/..." that
    produce spurious ancestors).
    """
    parts = path_seg.split("/")
    root = parts[0]
    if root not in vocabulary:
        return None
    last = parts[-1]
    if not last:
        return None
    return last.replace("_", "-").replace(" ", "-").lower()


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
    """Return the full vocabulary.

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


def _separator_variants(label: str) -> list[str]:
    """Return separator variants of *label* with spaces, underscores, and dashes substituted.

    Normalises to spaces first, then generates underscore and dash forms.
    The original label is excluded from the result.
    """
    spaced = label.replace("_", " ").replace("-", " ")
    candidates = [spaced, spaced.replace(" ", "_"), spaced.replace(" ", "-")]
    return [v for v in dict.fromkeys(candidates) if v != label]


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


@app.get("/api/sources", response_model=SourcesResponse)
async def get_sources() -> SourcesResponse:
    """Return every category source tingbok knows about.

    tingbok is the authority on which sources exist, but clients also need the
    URI prefixes (to tell which source a ``source_uris`` entry came from) and
    the display names (to label a per-source category subtree).  Both used to be
    duplicated client-side, so a new source here did not show up over there
    until the client was released again.  The list is generated from
    :data:`tingbok.sources.SOURCES`; it is not a second hand-kept list.
    """
    return SourcesResponse(
        sources=[
            SourceInfo(
                name=source.name,
                label=source.label,
                uri_prefixes=list(source.uri_prefixes),
                hosts=list(source.hosts),
                homepage=source.homepage,
                is_self=source.is_self,
            )
            for source in SOURCES
        ]
    )


def _parents_of(concept_id: str, vocab: dict[str, Any]) -> list[str]:
    """Return the immediate parents of *concept_id* within *vocab*.

    Declared ``broader``, minus any entry the vocabulary does not have.  Turning
    a path prefix into a parent — what makes ``food/nuts`` a child of ``food``
    — is :func:`_load_vocabulary`'s job and is already recorded in ``broader``
    by the time anything gets here; repeating the rule at this level would be
    dead for a normally loaded vocabulary, and live only for a concept whose
    declared parents all dangle, where it would make this function disagree with
    the ``broader`` that ``GET /api/vocabulary/{id}`` serves for the same
    concept.
    """
    data = vocab.get(concept_id)
    if data is None:
        return []
    broader = data.get("broader") or []
    if isinstance(broader, str):
        broader = [broader]
    # ``vocab.get(b) is not None``, not ``b in vocab``: a bare ``food:`` key in
    # the YAML is a concept id whose value is null, and calling it a parent
    # hands ``None`` to every consumer that then reads the concept's data.
    return [b for b in broader if vocab.get(b) is not None]


def ancestors_of(concept_id: str, vocab: dict[str, Any]) -> list[str]:
    """Every transitive ancestor of *concept_id*, nearest first.

    Breadth-first over :func:`_parents_of`, so a concept with several parents
    reports all of them; already-seen concepts are skipped, which also breaks
    the broader/narrower cycles that turn up in upstream SKOS data.

    The one implementation: ``GET /api/ancestors``,
    :func:`tingbok.embedded.get_ancestors` and :func:`_collect_with_ancestors`
    (behind ``POST /api/vocabulary/resolve``) all call it.  Two hand-written
    copies of a tree walk that disagree subtly is the problem this endpoint
    exists to solve, and having one inside this repository would be worse than
    having one in a client.
    """
    ancestors: list[str] = []
    seen: set[str] = {concept_id}
    queue: list[str] = _parents_of(concept_id, vocab)
    while queue:
        current = queue.pop(0)
        if current in seen:
            continue
        seen.add(current)
        ancestors.append(current)
        queue.extend(_parents_of(current, vocab))
    return ancestors


@app.get("/api/ancestors/{concept_id:path}", response_model=AncestorsResponse)
async def get_concept_ancestors(concept_id: str) -> AncestorsResponse:
    """Return every transitive ancestor of a concept, nearest first.

    ``GET /api/vocabulary`` serves a flat list that each client then has to
    re-structure, so "is soybeans food?" was answered by a hand-written tree
    walk in every client, each subtly different.  This answers it once, here.

    Its own path namespace, rather than ``/api/vocabulary/{id}/ancestors``:
    concept ids are themselves paths, so that shape collides with a concept
    genuinely called ``x/ancestors`` — and whichever way such a collision is
    resolved, one of the two is then unreachable.
    """
    from fastapi import HTTPException

    # A concept id never ends in a slash, so this can only be a typo.
    concept_id = concept_id.rstrip("/")
    if concept_id not in vocabulary:
        raise HTTPException(status_code=404, detail=f"Concept '{concept_id}' not found")
    return AncestorsResponse(id=concept_id, ancestors=ancestors_of(concept_id, vocabulary))


@app.get("/api/vocabulary/{concept_id:path}")
async def get_vocabulary_concept(concept_id: str) -> VocabularyConcept:
    """Return a single concept from the vocabulary.

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

    # This path reads with ruamel and never goes through _load_vocabulary, so
    # it needs the same normalisation: a deployment's own copy under
    # TINGBOK_DATA_DIR is seeded once and never migrated, so the bad shape can
    # still be on disk even though the packaged file is fixed.
    coerce_scalar_source_uris(concepts)

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
async def put_vocabulary_concept(
    concept_id: str, body: VocabularyConceptUpdateRequest, request: Request
) -> VocabularyConcept:
    """Create or update a vocabulary concept.

    Creates the concept entry (and any missing ancestors in the path hierarchy)
    if it does not yet exist.  All body fields are optional; omitted fields
    leave existing data unchanged.

    Changes are persisted to ``vocabulary.yaml`` and immediately reflected in
    the in-memory vocabulary so subsequent GET requests see the updated data.
    """
    global vocabulary, _category_index, _vocab_uri_index  # noqa: PLW0603

    await asyncio.to_thread(_write_vocabulary_concept_update, concept_id, body, VOCABULARY_PATH)
    _schedule_git_commit(ip=request.client.host if request.client else None)

    # Reload so path-inference and narrower computation are consistent.
    vocabulary = _load_vocabulary()
    _vocab_uri_index = _build_vocab_uri_index(vocabulary)
    _category_index = None

    data = vocabulary.get(concept_id)
    if data is None:
        from fastapi import HTTPException  # noqa: PLC0415

        raise HTTPException(status_code=500, detail="Concept write succeeded but could not be read back")

    return _vocabulary_concept_from_data(concept_id, data)


def _lookup_in_vocabulary(label: str, lang: str) -> VocabularyConcept | None:
    """Look up a label in the loaded vocabulary (no SKOS network calls).

    Checks concept IDs, prefLabels, altLabels, path aliases, and separator
    variants.  Returns a VocabularyConcept on hit, None on miss.
    """
    label_lower = label.lower()
    _nb_langs = {"nb", "no", "nn"}

    def _alias_lang_matches(alias_lang: str, req_lang: str) -> bool:
        return alias_lang == req_lang or (alias_lang in _nb_langs and req_lang in _nb_langs)

    # 1. Direct concept ID or separator variants
    data = vocabulary.get(label)
    if data is not None:
        return _vocabulary_concept_from_data(label, data)
    for variant in _separator_variants(label):
        data = vocabulary.get(variant)
        if data is not None:
            return _vocabulary_concept_from_data(variant, data)

    # 2. Language path alias (e.g. "klær/vinter" → clothing/thermal when lang=nb)
    if "/" in label:
        for concept_id, vdata in vocabulary.items():
            for alias_lang, aliases in (vdata.get("path_aliases") or {}).items():
                if _alias_lang_matches(alias_lang, lang):
                    if label_lower in [a.lower() for a in aliases]:
                        return _vocabulary_concept_from_data(concept_id, vdata)

    # 2b. Translated root segment (e.g. "mat/baking" → food/baking because "mat" is
    #     the nb altLabel of "food").  Tries to find a root concept whose altLabel
    #     matches the first path segment, then looks up the reconstructed canonical path.
    if "/" in label:
        root, rest = label.split("/", 1)
        root_lower = root.lower()
        for root_id, root_vdata in vocabulary.items():
            if "/" in root_id or root_vdata is None:
                continue
            for alts in (root_vdata.get("altLabel") or {}).values():
                if root_lower in [a.lower() for a in alts]:
                    candidate = root_id + "/" + rest
                    cand_data = vocabulary.get(candidate)
                    if cand_data is not None:
                        return _vocabulary_concept_from_data(candidate, cand_data)
                    break  # root matched but candidate path not in vocabulary

    # 3. prefLabel / altLabel / runtime-fetched labels (check all separator variants)
    label_variants = {label_lower} | {v.lower() for v in _separator_variants(label)}
    for concept_id, vdata in vocabulary.items():
        if vdata.get("prefLabel", "").lower() in label_variants:
            return _vocabulary_concept_from_data(concept_id, vdata)
        for alts in (vdata.get("altLabel") or {}).values():
            if any(a.lower() in label_variants for a in alts):
                return _vocabulary_concept_from_data(concept_id, vdata)
        for alts in (_fetched_alt_labels.get(concept_id) or {}).values():
            if any(a.lower() in label_variants for a in alts):
                return _vocabulary_concept_from_data(concept_id, vdata)
        for lbl in (_fetched_labels.get(concept_id) or {}).values():
            if lbl.lower() in label_variants:
                return _vocabulary_concept_from_data(concept_id, vdata)

    # 4. Singular/plural variants.  Builds number variations of every separator
    #    form from step 3 (so "fruit-juices" is tried as "fruit juice" just as the
    #    singular "fruit-juice" is), and — like step 3 — matches them against
    #    prefLabels, altLabels, and the runtime-enriched label caches, where
    #    altLabels such as "fruit juice" typically live.  Reuses ``label_variants``
    #    (separators) and ``text.number_variations`` (number) so the inflection
    #    rules live in one place.
    number_variants = {v for base in label_variants for v in number_variations(base)} - label_variants
    if number_variants:
        for concept_id, vdata in vocabulary.items():
            if vdata.get("prefLabel", "").lower() in number_variants:
                return _vocabulary_concept_from_data(concept_id, vdata)
            for alts in (vdata.get("altLabel") or {}).values():
                if any(a.lower() in number_variants for a in alts):
                    return _vocabulary_concept_from_data(concept_id, vdata)
            for alts in (_fetched_alt_labels.get(concept_id) or {}).values():
                if any(a.lower() in number_variants for a in alts):
                    return _vocabulary_concept_from_data(concept_id, vdata)
            for lbl in (_fetched_labels.get(concept_id) or {}).values():
                if lbl.lower() in number_variants:
                    return _vocabulary_concept_from_data(concept_id, vdata)

    # 5. Reverse label cache from previous SKOS lookups
    cached = _skos_label_cache.get((label_lower, lang))
    if cached is not None:
        c_id, c_labels, c_alts, c_uris, c_broader, c_desc, c_wiki = cached
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

    return None


def _add_input_label_as_altlabel(concept: VocabularyConcept, label: str, lang: str) -> None:
    """Record a raw input label as an altLabel on its canonical concept.

    Used when a resolve input matched a concept via altLabel / prefLabel / number
    variant: the original spelling is folded into the canonical concept's
    altLabels (under ``lang``) so clients can resolve the raw label back to this
    one ID.  No-op if the label already appears among any of the concept's labels
    (case-insensitive), so e.g. a plural that equals the prefLabel is not added.
    """
    label_lower = label.lower()
    if concept.prefLabel.lower() == label_lower:
        return
    for alts in concept.altLabel.values():
        if any(a.lower() == label_lower for a in alts):
            return
    concept.altLabel.setdefault(lang, []).append(label)


def _collect_with_ancestors(
    concept_id: str,
    result: dict[str, VocabularyConcept],
) -> None:
    """Add concept_id and all its vocabulary ancestors to result.

    The walk itself is :func:`ancestors_of`, which is also what ``GET
    /api/ancestors`` answers with — this only turns the ids it returns into
    concepts.  Cycle-breaking comes with it.
    """
    data = vocabulary.get(concept_id)
    if data is None:
        return
    result.setdefault(concept_id, _vocabulary_concept_from_data(concept_id, data))
    for ancestor_id in ancestors_of(concept_id, vocabulary):
        if ancestor_id not in result:
            result[ancestor_id] = _vocabulary_concept_from_data(ancestor_id, vocabulary[ancestor_id])


@app.post("/api/vocabulary/resolve", response_model=VocabularyResolveResponse)
async def resolve_vocabulary(request: VocabularyResolveRequest) -> VocabularyResolveResponse:
    """Resolve a list of category labels to a tailored vocabulary.

    For each label the inventory uses, returns the matching concept plus all
    ancestor concepts needed to render a complete category tree.  Concepts not
    found in the vocabulary are returned as minimal stubs with
    ``source="inventory"`` so the client can still represent them in the tree.

    This is the preferred alternative to ``GET /api/vocabulary`` for clients
    that only need a subset of the vocabulary: one round-trip, no local
    hierarchy-building needed, and every concept carries its canonical URI.
    """
    lang = request.lang
    skos_sources: tuple[str, ...] = () if request.offline else skos_lookup_sources()

    # Phase 1: resolve all labels.  Vocabulary hits are resolved directly; unknown
    # labels are looked up in all SKOS sources in parallel so hierarchy paths and
    # source URIs are available for the bridging step below.
    vocab_hits: dict[str, VocabularyConcept] = {}
    skos_labels: list[str] = []

    for label in request.labels:
        hit = _lookup_in_vocabulary(label, lang)
        if hit is not None:
            vocab_hits[label] = hit
        else:
            skos_labels.append(label)

    # Fetch SKOS data for all unresolved labels in parallel
    async def _fetch_all_sources(label: str) -> tuple[str, list[tuple], dict[str, str]]:
        """Return (label, per_source_results, combined_uri_map) for one label."""
        lookup_label = label.replace("_", " ").replace("-", " ")
        per_source = await asyncio.gather(*(_fetch_one_skos_source(lookup_label, s, lang) for s in skos_sources))
        uri_map: dict[str, str] = {}
        for _, _, _, m in per_source:
            uri_map.update(m)
        return label, list(per_source), uri_map

    skos_fetches = await asyncio.gather(*(_fetch_all_sources(lbl) for lbl in skos_labels))

    # Phase 2: build per-label URI → input_label index so descendants can reference
    # sibling labels resolved in the same batch by their input label (not SKOS concept_id).
    # Start from the static vocabulary index and overlay batch-level entries.
    local_uri_to_label: dict[str, str] = dict(_vocab_uri_index)

    for label, per_source, _ in skos_fetches:
        for _, _, uris, _ in per_source:
            for uri in uris:
                n = _normalise_uri(uri)
                if n and n not in local_uri_to_label:
                    local_uri_to_label[n] = label  # input label as concept ID

    # Phase 3: build VocabularyConcept for each SKOS-resolved label and assemble response
    concepts: dict[str, VocabularyConcept] = {}
    unresolved: list[str] = []

    # Add vocabulary hits (with full ancestor chain).
    # When the input label differs from the canonical concept ID (e.g. altLabel
    # "fresh-milk" → concept "whole-milk", or singular "vegetable" → "vegetables"),
    # record the raw input as an altLabel on the canonical concept rather than
    # emitting a separate bridge node.  A bridge node keyed by the input label
    # would split synonym/number variants into disjoint sibling concepts on the
    # client (so e.g. "vegetable" and "vegetables" never match each other); an
    # altLabel keeps everything anchored to one canonical ID.
    for _label, hit in vocab_hits.items():
        _collect_with_ancestors(hit.id, concepts)
        if _label != hit.id:
            canonical = concepts.get(hit.id)
            if canonical is not None:
                _add_input_label_as_altlabel(canonical, _label, lang)

    for label, per_source, uri_map in skos_fetches:
        lookup_label = label.replace("_", " ").replace("-", " ")
        merged_labels: dict[str, str] = {}
        source_uris: list[str] = []
        all_paths: list[str] = []

        for _, (concept, paths, uris, _) in zip(skos_sources, per_source, strict=False):
            if concept is None:
                continue
            for uri in uris:
                uri = _normalise_uri(uri)
                if uri and uri not in source_uris:
                    source_uris.append(uri)
            for p in paths:
                if p not in all_paths:
                    all_paths.append(p)
            if lang not in merged_labels:
                pref_label_val = concept.get("prefLabel", label)
                merged_labels[lang] = pref_label_val

        if not source_uris and not all_paths:
            unresolved.append(label)
            concepts[label] = VocabularyConcept(
                id=label,
                prefLabel=label,
                broader=[],
                narrower=[],
                uri=f"{TINGBOK_BASE_URL}/api/vocabulary/{label}",
                source_uris=[],
                labels={lang: label},
            )
            continue

        # Build broader from SKOS paths and URI bridging.
        broader = _build_broader_from_paths(all_paths, uri_map, local_uri_to_label, {label})

        # Use INPUT LABEL as concept ID (not the SKOS-derived vocabulary-anchored path)
        # so that resolve_category() on the client side finds concepts by their raw label.
        pref_label_str = merged_labels.get(lang, lookup_label)
        resolved_concept = VocabularyConcept(
            id=label,
            prefLabel=pref_label_str,
            broader=broader,
            narrower=[],
            uri=f"{TINGBOK_BASE_URL}/api/vocabulary/{label}",
            source_uris=source_uris,
            labels=merged_labels,
        )
        concepts[label] = resolved_concept

        # Pull in vocabulary ancestors for any vocabulary concept IDs in broader.
        # Also create minimal stubs for derived (path-segment-normalised) concept IDs
        # so that JS clients can resolve them in the offline category browser.
        for parent_id in broader:
            if parent_id in vocabulary:
                _collect_with_ancestors(parent_id, concepts)
        for path_seg, seg_uri in uri_map.items():
            if _vocab_uri_index.get(_normalise_uri(seg_uri)):
                continue  # vocabulary concept — already handled above
            already = local_uri_to_label.get(_normalise_uri(seg_uri))
            if already and already in concepts:
                continue  # same URI already bridges to a resolved concept in this batch
            derived_id = _concept_id_from_path_seg(path_seg)
            if not derived_id or derived_id == label or derived_id in concepts:
                continue
            parent_path = "/".join(path_seg.split("/")[:-1])
            parent_id: str | None = None
            if parent_path:
                parent_uri = uri_map.get(parent_path)
                if parent_uri:
                    parent_id = _vocab_uri_index.get(_normalise_uri(parent_uri))
                if not parent_id and parent_path:
                    parent_id = _concept_id_from_path_seg(parent_path)
            raw_label = path_seg.split("/")[-1].replace("_", " ").replace("-", " ")
            concepts[derived_id] = VocabularyConcept(
                id=derived_id,
                prefLabel=raw_label.title(),
                broader=[parent_id] if parent_id else [],
                narrower=[],
                uri=f"{TINGBOK_BASE_URL}/api/vocabulary/{derived_id}",
                source_uris=[seg_uri] if seg_uri else [],
                labels={},
            )
            if parent_id and parent_id in vocabulary:
                _collect_with_ancestors(parent_id, concepts)

    return VocabularyResolveResponse(concepts=concepts, unresolved=unresolved)


async def _fetch_one_skos_source(
    lookup_label: str,
    source: str,
    lang: str,
) -> tuple[dict | None, list[str], list[str], dict[str, str]]:
    """Fetch concept from one SKOS source.  Returns (concept, paths, source_uris, uri_map).  Never raises."""
    try:
        found_lang = lang
        concept = await asyncio.to_thread(skos_service.lookup_concept, lookup_label, lang, source, SKOS_CACHE_DIR)
        if not concept:
            for fallback_lang in _fallback_langs(lang):
                concept = await asyncio.to_thread(
                    skos_service.lookup_concept, lookup_label, fallback_lang, source, SKOS_CACHE_DIR
                )
                if concept:
                    found_lang = fallback_lang
                    break
        if not concept:
            return None, [], [], {}
        uri = concept.get("uri") or ""
        paths, found, uri_map = await asyncio.to_thread(
            skos_service.build_hierarchy_paths, lookup_label, found_lang, source, SKOS_CACHE_DIR
        )
        return concept, (paths if found else []), ([uri] if uri else []), (uri_map if found else {})
    except Exception as exc:
        logger.debug("Lookup failed for '%s' via %s: %s", lookup_label, source, exc)
        return None, [], [], {}


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

    # Steps 1–2.5: vocabulary-only lookup (no SKOS network calls)
    vocab_hit = _lookup_in_vocabulary(label, lang)
    if vocab_hit is not None:
        return vocab_hit

    # 3. Query all SKOS sources in parallel, merge results
    skos_sources = skos_lookup_sources()
    fetch_languages = _DEFAULT_FETCH_LANGUAGES

    # External sources index natural-language labels with spaces, not underscores or dashes.
    # Normalise the lookup label so "olive_oil" queries as "olive oil".
    lookup_label = label.replace("_", " ").replace("-", " ")

    results = await asyncio.gather(*(_fetch_one_skos_source(lookup_label, s, lang) for s in skos_sources))

    # Merge across sources
    merged_labels: dict[str, str] = {}
    merged_alts: dict[str, list[str]] = {}
    source_uris: list[str] = []
    descriptions: list[str] = []
    wikipedia_url: str | None = None
    concept_id: str | None = None
    pref_label: str = label
    all_paths: list[str] = []  # every path from every source, for multi-path broader
    combined_uri_map: dict[str, str] = {}  # path_segment → URI across all sources

    for source, (concept, paths, uris, uri_map) in zip(skos_sources, results, strict=False):
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

        combined_uri_map.update(uri_map)

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
    gpt_concept = await asyncio.to_thread(gpt_service.lookup_concept, lookup_label, lang, _CACHE_BASE)
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
    off_concept = await asyncio.to_thread(off_service.lookup_concept, lookup_label, lang, SKOS_CACHE_DIR)
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
    for src, (_, src_paths, _, _) in zip(skos_sources, results, strict=False):
        if src_paths:
            warn_roots[src] = src_paths[0].split("/")[0]
            warn_paths[src] = src_paths
    if len(set(warn_roots.values())) > 1:
        _record_lookup_warning(label, warn_roots, warn_paths)

    # Pick vocabulary-anchored canonical path as concept ID; fall back to label slug
    if all_paths:
        concept_id = _best_vocabulary_anchored_path(all_paths, vocabulary)
    if concept_id is None:
        concept_id = lookup_label.lower().replace(" ", "_")

    # Build broader from ALL paths and URI bridging.
    # self_ids excludes both the full concept_id path and its normalised last segment
    # (e.g. "food/spices/cumin" and "cumin") so neither appears as its own ancestor.
    _concept_seg = concept_id.split("/")[-1].replace("_", "-").replace(" ", "-").lower() if concept_id else None
    _self_ids: set[str] = {concept_id} if concept_id else set()
    if _concept_seg:
        _self_ids.add(_concept_seg)
    broader = _build_broader_from_paths(
        all_paths, combined_uri_map, _vocab_uri_index, _self_ids, fallback_concept_id=concept_id
    )
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


# The MCP server snapshots the app's routes when it is constructed, so this has
# to come after every endpoint in this module and not merely after the routers
# are included — built at the top, it saw the two routers and none of the
# vocabulary, sources or ancestors endpoints, which is not what an exclusion
# list is asking for.
#
# Exclusions are matched on method and path rather than on FastAPI's generated
# operation ids: an id is a name FastAPI derives and may change, and an
# exclusion naming an id that does not exist fails open — the endpoint stays
# exposed and nothing complains.  A path pattern says what is meant.
_mcp = FastMCP.from_fastapi(
    app,
    name="tingbok",
    instructions="Product and category lookup service for domestic inventory systems",
    route_maps=[
        RouteMap(pattern=r"^/health$", mcp_type=MCPType.EXCLUDE),
        RouteMap(pattern=r"^/api/skos/cache$", mcp_type=MCPType.EXCLUDE),
        # Read-only over MCP: this one rewrites vocabulary.yaml and commits it.
        RouteMap(methods=["PUT"], pattern=r"^/api/vocabulary/", mcp_type=MCPType.EXCLUDE),
    ],
)

# ``http_app()`` is a Starlette app with a lifespan of its own (it owns the
# session manager); mounting it without running that lifespan yields a /mcp
# that 500s on the first request.  ``lifespan`` above enters it around its own
# yield, which is why this is a module global rather than a local.
_mcp_app = _mcp.http_app(path="/")


class _BareMcpPath:
    """Serve the MCP endpoint at ``/mcp`` as well as at ``/mcp/``.

    ``http_app()`` insists on a route path starting with ``/``, so the endpoint
    can only sit at ``/mcp/`` once mounted.  Starlette's ``Mount`` does not
    match the prefix on its own, so bare ``/mcp`` falls through to the router's
    slash redirect: a 307 preserves method and body, and a client that follows
    redirects is fine, but the previous MCP mount answered ``/mcp`` directly and
    an already-configured client should not have to care.

    Rewriting the scope is deliberate — a ``BaseHTTPMiddleware`` would wrap the
    response, and this endpoint streams server-sent events.
    """

    def __init__(self, app: object) -> None:
        self._app = app

    async def __call__(self, scope: dict, receive: object, send: object) -> None:
        if scope["type"] == "http" and scope.get("path") == "/mcp":
            scope = {**scope, "path": "/mcp/", "raw_path": b"/mcp/"}
        await self._app(scope, receive, send)


app.mount("/mcp", _mcp_app)
app.add_middleware(_BareMcpPath)
