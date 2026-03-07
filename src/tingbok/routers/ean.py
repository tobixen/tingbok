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
    sourced from Open Food Facts.  Results are cached for 60 days.
    Returns 404 when the barcode is not found in any upstream source.
    """
    result = await asyncio.to_thread(ean_service.lookup_product, ean, _app.EAN_CACHE_DIR)
    if result is None:
        raise HTTPException(status_code=404, detail=f"Product not found for EAN {ean}")
    return ProductResponse(**result)
