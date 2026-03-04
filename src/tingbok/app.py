"""FastAPI application for tingbok."""

import asyncio
import logging
import os
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import yaml
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

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

vocabulary: dict[str, Any] = {}

#: Auto-discovered external source URIs for concepts that have none in vocabulary.yaml.
#: Maps concept_id -> {source_name: uri}.  Populated by _discover_source_uris_background().
_discovered_source_uris: dict[str, dict[str, str]] = {}


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
                off_concept = await asyncio.to_thread(off_service.lookup_concept, label, "en")
                if off_concept and off_concept.get("uri"):
                    discovered["off"] = off_concept["uri"]
            except Exception as exc:  # noqa: BLE001
                logger.debug("OFF URI discovery failed for '%s': %s", concept_id, exc)

        if discovered:
            _discovered_source_uris[concept_id] = discovered


@asynccontextmanager
async def lifespan(app: FastAPI):  # noqa: ARG001
    """Load vocabulary on startup, then kick off background URI discovery."""
    global vocabulary  # noqa: PLW0603
    vocabulary = _load_vocabulary()
    discovery_task = asyncio.create_task(_discover_source_uris_background())
    try:
        yield
    finally:
        discovery_task.cancel()
        try:
            await discovery_task
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
            altLabel=data.get("altLabel", {}),
            broader=broader,
            narrower=data.get("narrower", []),
            uri=data.get("uri"),
            source_uris=_build_source_uris(concept_id, data),
            excluded_sources=data.get("excluded_sources", []),
            labels=data.get("labels", {}),
            description=data.get("description"),
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
        altLabel=data.get("altLabel", {}),
        broader=broader,
        narrower=data.get("narrower", []),
        uri=data.get("uri"),
        source_uris=_build_source_uris(concept_id, data),
        excluded_sources=data.get("excluded_sources", []),
        labels=data.get("labels", {}),
        description=data.get("description"),
        wikipediaUrl=data.get("wikipediaUrl"),
    )
