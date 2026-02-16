"""EAN/barcode product lookup endpoints."""

from fastapi import APIRouter, HTTPException

from tingbok.models import ProductResponse

router = APIRouter()


@router.get("/{ean}", response_model=ProductResponse)
async def lookup_ean(ean: str) -> ProductResponse:
    """Look up product data by EAN/barcode."""
    # Stub — will proxy to OFF, UPCitemdb, etc. in a later phase
    raise HTTPException(
        status_code=501,
        detail=f"EAN lookup not yet implemented (ean={ean})",
    )
