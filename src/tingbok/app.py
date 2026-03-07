"""FastAPI application for tingbok."""

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import yaml
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi_mcp import FastApiMCP

from tingbok import __version__
from tingbok.models import HealthResponse, VocabularyConcept
from tingbok.routers import ean, skos
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

vocabulary: dict[str, Any] = {}

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


def _load_vocabulary() -> dict[str, Any]:
    """Load the package vocabulary from YAML."""
    with open(VOCABULARY_PATH) as f:
        data = yaml.safe_load(f)
    return data.get("concepts", {})


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
    # Include AGROVOC only when the local Oxigraph store is available — the REST
    # API has too many false positives for a reliable auto-discovery pass.
    agrovoc_available = skos_service.get_agrovoc_store(SKOS_CACHE_DIR) is not None
    skos_sources = ("agrovoc", "dbpedia", "wikidata") if agrovoc_available else ("dbpedia", "wikidata")

    for concept_id, data in vocabulary.items():
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


async def _fetch_labels_background() -> None:
    """Fetch labels and descriptions from source_uris for all vocabulary concepts.

    For each concept, queries all known external source URIs and merges the
    returned translations into ``_fetched_labels``.  Descriptions are fetched
    for DBpedia and Wikidata sources; the longest available description is
    stored in ``_fetched_descriptions``.

    Results are rebuilt from the SKOS cache on every startup (expensive API
    calls only happen on cache misses, which are then cached for 60 days).
    """
    for concept_id, data in vocabulary.items():
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
                    fetched = await asyncio.to_thread(
                        gpt_service.get_labels, uri, _DEFAULT_FETCH_LANGUAGES, _CACHE_BASE
                    )
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


@asynccontextmanager
async def lifespan(app: FastAPI):  # noqa: ARG001
    """Load vocabulary on startup, then kick off background URI discovery and label fetching."""
    global vocabulary  # noqa: PLW0603
    vocabulary = _load_vocabulary()
    discovery_task = asyncio.create_task(_discover_source_uris_background())
    labels_task = asyncio.create_task(_fetch_labels_background())
    try:
        yield
    finally:
        for task in (discovery_task, labels_task):
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


@app.get("/health", response_model=HealthResponse)
async def health():
    """Liveness check."""
    return HealthResponse(version=__version__)


def _build_source_uris(concept_id: str, data: dict[str, Any]) -> list[str]:
    """Build the full source_uris list for a concept.

    Combines static URIs from vocabulary.yaml with auto-discovered URIs, always
    prepending the canonical tingbok self-URI.
    """
    self_uri = f"{TINGBOK_BASE_URL}/api/vocabulary/{concept_id}"
    source_uris: list[str] = list(data.get("source_uris", []))
    # Merge in any auto-discovered URIs (values only; skip if already present)
    for uri in _discovered_source_uris.get(concept_id, {}).values():
        if uri not in source_uris:
            source_uris.append(uri)
    if self_uri not in source_uris:
        source_uris.insert(0, self_uri)
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
    """Return the full package vocabulary."""
    result = {}
    for concept_id, data in vocabulary.items():
        broader = data.get("broader", [])
        if isinstance(broader, str):
            broader = [broader]
        result[concept_id] = VocabularyConcept(
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
        )
    return result


@app.get("/api/vocabulary/{concept_id}")
async def get_vocabulary_concept(concept_id: str) -> VocabularyConcept:
    """Return a single concept from the package vocabulary."""
    from fastapi import HTTPException

    data = vocabulary.get(concept_id)
    if data is None:
        raise HTTPException(status_code=404, detail=f"Concept '{concept_id}' not found")
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
    )
