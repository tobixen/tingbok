"""EAN/barcode product lookup endpoints."""

import asyncio

from fastapi import APIRouter, HTTPException

import tingbok.app as _app
from tingbok.models import ProductResponse
from tingbok.services import ean as ean_service

router = APIRouter()


@router.get("/{ean}", response_model=ProductResponse)
async def lookup_ean(ean: str) -> ProductResponse:
    """Look up product data by EAN/barcode.

    Returns product name, brand, quantity, category hints and image URL
    sourced from Open Food Facts, UPCitemdb, Open Library, or nb.no.
    Locally observed data (prices, receipt names, notes) from manual-ean.yaml
    is merged into the response.  Results are cached for 60 days.
    Returns 404 when the barcode is not found in any source.
    """
    upstream = await asyncio.to_thread(ean_service.lookup_product, ean, _app.EAN_CACHE_DIR)
    manual = _app.manual_ean.get(ean)
    result = ean_service.merge_manual_data(upstream, manual)
    if result is None:
        raise HTTPException(status_code=404, detail=f"Product not found for EAN {ean}")
    return ProductResponse(**result)
