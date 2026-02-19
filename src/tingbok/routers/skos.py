"""SKOS category lookup endpoints."""

import asyncio

from fastapi import APIRouter, HTTPException, Query

import tingbok.app as _app
from tingbok.models import (
    BatchLabelsRequest,
    BatchLabelsResponse,
    CacheStatsResponse,
    ConceptResponse,
    HierarchyResponse,
    LabelsResponse,
)
from tingbok.services import skos as skos_service

router = APIRouter()


@router.get("/lookup", response_model=ConceptResponse)
async def lookup(
    label: str = Query(..., description="Concept label to look up"),
    lang: str = Query("en", description="Language code"),
    source: str = Query("agrovoc", description="Source: agrovoc, dbpedia, wikidata"),
) -> ConceptResponse:
    """Look up a single SKOS concept by label.

    Returns the concept from cache when available; falls back to a live
    upstream REST query for cache misses.  Returns 404 when the concept is
    not found in either the cache or the upstream source.
    """
    concept = await asyncio.to_thread(skos_service.lookup_concept, label, lang, source, _app.SKOS_CACHE_DIR)
    if concept is None:
        raise HTTPException(
            status_code=404,
            detail=f"Concept '{label}' not found in {source}",
        )

    alt_labels = concept.get("altLabel", {})
    if not isinstance(alt_labels, dict):
        alt_labels = {}

    # Preserve broader with labels (list[dict] with uri/label keys)
    broader_raw = concept.get("broader", [])
    broader = [
        {"uri": item["uri"], "label": item.get("label", "")} if isinstance(item, dict) else {"uri": item, "label": ""}
        for item in broader_raw
        if (item.get("uri") if isinstance(item, dict) else item)
    ]

    return ConceptResponse(
        uri=concept.get("uri"),
        prefLabel=concept.get("prefLabel", label),
        altLabels=alt_labels,
        broader=broader,
        source=concept.get("source", source),
        labels=concept.get("labels", {}),
        description=concept.get("description"),
        wikipediaUrl=concept.get("wikipediaUrl"),
    )


@router.get("/hierarchy", response_model=HierarchyResponse)
async def hierarchy(
    label: str = Query(..., description="Concept label"),
    lang: str = Query("en", description="Language code"),
    source: str = Query("agrovoc", description="Source: agrovoc, dbpedia, wikidata"),
) -> HierarchyResponse:
    """Build full hierarchy paths from a concept to its root(s).

    Follows the ``skos:broader`` chain recursively, applying root mapping
    (e.g., AGROVOC's "Plant products" becomes "food").  Returns 200 with
    ``found=false`` when the concept cannot be resolved.
    """
    paths, found, uri_map = await asyncio.to_thread(
        skos_service.build_hierarchy_paths, label, lang, source, _app.SKOS_CACHE_DIR
    )
    return HierarchyResponse(label=label, paths=paths, found=found, source=source, uri_map=uri_map)


@router.get("/labels", response_model=LabelsResponse)
async def labels(
    uri: str = Query(..., description="SKOS concept URI"),
    languages: str = Query("en,nb,de", description="Comma-separated language codes"),
    source: str = Query("agrovoc", description="Source: agrovoc, dbpedia, wikidata"),
) -> LabelsResponse:
    """Get translations for a SKOS concept URI.

    Returns cached labels when available; falls back to a live upstream
    REST query for cache misses.
    """
    lang_list = [lang.strip() for lang in languages.split(",") if lang.strip()]
    found_labels = await asyncio.to_thread(skos_service.get_labels, uri, lang_list, source, _app.SKOS_CACHE_DIR)

    return LabelsResponse(uri=uri, labels=found_labels, source=source)


@router.post("/labels/batch", response_model=BatchLabelsResponse)
async def labels_batch(body: BatchLabelsRequest) -> BatchLabelsResponse:
    """Fetch translations for multiple SKOS concept URIs in one request.

    Each URI is served from cache independently; uncached URIs trigger one
    upstream REST call each.  Useful for efficiently translating all path
    segments in a vocabulary.
    """
    result = await asyncio.to_thread(
        skos_service.get_labels_batch,
        body.uris,
        body.languages,
        body.source,
        _app.SKOS_CACHE_DIR,
    )
    return BatchLabelsResponse(labels=result, source=body.source)


@router.get("/cache", response_model=CacheStatsResponse)
async def cache() -> CacheStatsResponse:
    """Return statistics about the SKOS cache directory."""
    stats = await asyncio.to_thread(skos_service.cache_stats, _app.SKOS_CACHE_DIR)
    return CacheStatsResponse(**stats)
