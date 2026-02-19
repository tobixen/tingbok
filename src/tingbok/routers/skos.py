"""SKOS category lookup endpoints."""

from fastapi import APIRouter, HTTPException, Query

import tingbok.app as _app
from tingbok.models import ConceptResponse, HierarchyResponse, LabelsResponse
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
    concept = skos_service.lookup_concept(label, lang, source, _app.SKOS_CACHE_DIR)
    if concept is None:
        raise HTTPException(
            status_code=404,
            detail=f"Concept '{label}' not found in {source}",
        )

    broader = skos_service._broader_to_uris(concept.get("broader", []))
    alt_labels = concept.get("altLabel", {})
    if not isinstance(alt_labels, dict):
        alt_labels = {}

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
) -> HierarchyResponse:
    """Get full hierarchy paths for a label."""
    # Stub — recursive path-to-root building requires SPARQL; deferred to a later phase
    raise HTTPException(
        status_code=501,
        detail=f"SKOS hierarchy not yet implemented (label={label}, lang={lang})",
    )


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
    found_labels = skos_service.get_labels(uri, lang_list, source, _app.SKOS_CACHE_DIR)

    return LabelsResponse(
        uri=uri,
        labels=found_labels,
        source=source,
    )
