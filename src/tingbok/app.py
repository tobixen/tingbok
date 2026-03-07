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
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi_mcp import FastApiMCP

from tingbok import __version__
from tingbok.models import HealthResponse, VocabularyConcept
from tingbok.routers import ean, skos
from tingbok.services import ean as ean_service
from tingbok.services import gpt as gpt_service
from tingbok.services import off as off_service
from tingbok.services import skos as skos_service

logger = logging.getLogger(__name__)

VOCABULARY_PATH = Path(__file__).parent / "data" / "vocabulary.yaml"
MANUAL_EAN_PATH = Path(__file__).parent / "data" / "ean-db.yaml"
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
#: Separate from the git-tracked ean-db.yaml; accumulates PUT /api/ean/{ean} calls.
EAN_OBSERVATIONS_PATH: Path = _CACHE_BASE / "ean-db.json"

vocabulary: dict[str, Any] = {}

#: Manually curated EAN data loaded from manual-ean.yaml.
manual_ean: dict[str, Any] = {}

#: Inventory-sourced EAN observations loaded from ean-db.json (written by PUT /api/ean/{ean}).
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
    global vocabulary, manual_ean, ean_observations  # noqa: PLW0603
    vocabulary = _load_vocabulary()
    manual_ean = ean_service.load_manual_ean(MANUAL_EAN_PATH)
    ean_observations = ean_service.load_ean_observations(EAN_OBSERVATIONS_PATH)
    skos_service.load_agrovoc_background(SKOS_CACHE_DIR)
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

    Combines static URIs from vocabulary.yaml with auto-discovered URIs.
    The canonical tingbok self-URI is exposed via the separate ``uri`` field
    and is excluded from ``source_uris`` to avoid redundancy.
    """
    self_uri = f"{TINGBOK_BASE_URL}/api/vocabulary/{concept_id}"
    source_uris: list[str] = [u for u in data.get("source_uris", []) if u != self_uri]
    # Merge in any auto-discovered URIs (values only; skip if already present)
    for uri in _discovered_source_uris.get(concept_id, {}).values():
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
    """Return the full package vocabulary."""
    return {concept_id: _vocabulary_concept_from_data(concept_id, data) for concept_id, data in vocabulary.items()}


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
    )


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
    """Return a single concept from the package vocabulary."""
    from fastapi import HTTPException

    data = vocabulary.get(concept_id)
    if data is None:
        raise HTTPException(status_code=404, detail=f"Concept '{concept_id}' not found")
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

    # 2. Match by prefLabel or altLabel in vocabulary (case-insensitive)
    label_lower = label.lower()
    for concept_id, vdata in vocabulary.items():
        if vdata.get("prefLabel", "").lower() == label_lower:
            return _vocabulary_concept_from_data(concept_id, vdata)
        for alts in (vdata.get("altLabel") or {}).values():
            if label_lower in [a.lower() for a in alts]:
                return _vocabulary_concept_from_data(concept_id, vdata)

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

    for source, (concept, paths, uris) in zip(skos_sources, results, strict=False):
        if concept is None:
            continue

        # Collect source URIs
        for uri in uris:
            if uri and uri not in source_uris:
                source_uris.append(uri)

        # Prefer hierarchy-derived concept ID (most specific path first)
        if paths and concept_id is None:
            concept_id = paths[0]

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

    if not source_uris and concept_id is None:
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

    if concept_id is None:
        concept_id = label_lower.replace(" ", "_")
    broader = ["/".join(concept_id.split("/")[:-1])] if "/" in concept_id else []
    best_description = max(descriptions, key=len) if descriptions else None

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
