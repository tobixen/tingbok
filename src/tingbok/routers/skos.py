"""SKOS category lookup endpoints."""

from fastapi import APIRouter, HTTPException, Query

from tingbok.models import ConceptResponse, HierarchyResponse, LabelsResponse

router = APIRouter()


@router.get("/lookup", response_model=ConceptResponse)
async def lookup(
    label: str = Query(..., description="Concept label to look up"),
    lang: str = Query("en", description="Language code"),
    source: str = Query("agrovoc", description="Source: agrovoc, dbpedia, wikidata"),
) -> ConceptResponse:
    """Look up a single SKOS concept by label."""
    # Stub — will proxy to upstream SKOS sources in a later phase
    raise HTTPException(
        status_code=501,
        detail=f"SKOS lookup not yet implemented (label={label}, lang={lang}, source={source})",
    )


@router.get("/hierarchy", response_model=HierarchyResponse)
async def hierarchy(
    label: str = Query(..., description="Concept label"),
    lang: str = Query("en", description="Language code"),
) -> HierarchyResponse:
    """Get full hierarchy paths for a label."""
    # Stub — will proxy to upstream SKOS sources in a later phase
    raise HTTPException(
        status_code=501,
        detail=f"SKOS hierarchy not yet implemented (label={label}, lang={lang})",
    )


@router.get("/labels", response_model=LabelsResponse)
async def labels(
    uri: str = Query(..., description="SKOS concept URI"),
    languages: str = Query("en,nb,de", description="Comma-separated language codes"),
) -> LabelsResponse:
    """Get translations for a SKOS concept URI."""
    # Stub — will proxy to upstream SKOS sources in a later phase
    raise HTTPException(
        status_code=501,
        detail=f"SKOS labels not yet implemented (uri={uri}, languages={languages})",
    )
