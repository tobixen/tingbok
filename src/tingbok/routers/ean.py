"""EAN/barcode product lookup endpoints."""

import asyncio
import logging

from fastapi import APIRouter, HTTPException

import tingbok.app as _app
from tingbok.models import EanObservationRequest, ProductResponse
from tingbok.services import ean as ean_service

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/{ean}", response_model=ProductResponse)
async def lookup_ean(ean: str) -> ProductResponse:
    """Look up product data by EAN/barcode.

    Returns product name, brand, quantity, category hints and image URL
    sourced from Open Food Facts, UPCitemdb, Open Library, or nb.no.
    Locally observed data (categories, prices, receipt names) from ean-db.json
    is merged into the response.  Results are cached for 60 days.
    Returns 404 when the barcode is not found in any source.
    """
    upstream = await asyncio.to_thread(ean_service.lookup_product, ean, _app.EAN_CACHE_DIR)
    observation = _app.ean_observations.get(ean)
    if upstream is None:
        if not observation:
            raise HTTPException(status_code=404, detail=f"Product not found for EAN {ean}")
        result = dict(observation)
        result.setdefault("ean", ean)
        result.setdefault("source", "observation")
    else:
        result = dict(upstream)
        result.setdefault("ean", ean)
        if observation:
            result = ean_service.merge_observation(result, observation)
    if result.get("categories"):
        result["categories"] = _app._normalize_ean_categories(result["categories"])
    return ProductResponse(**result)


@router.put("/{ean}", response_model=ProductResponse)
async def observe_ean(ean: str, body: EanObservationRequest) -> ProductResponse:
    """Store an inventory-sourced category and/or name for an EAN.

    The observation is persisted to ``ean-db.json`` in the cache directory
    and merged into future GET responses for this EAN.  This supplements (but
    does not replace) data from upstream sources; inventory categories take
    priority in the ``categories`` list.
    """
    if not body.categories and body.name is None:
        raise HTTPException(status_code=422, detail="At least one of 'categories' or 'name' must be provided")

    prices_raw = [p.model_dump() for p in body.prices]
    receipt_names_raw = [r.model_dump() for r in body.receipt_names]
    await asyncio.to_thread(
        ean_service.save_ean_observation,
        _app.EAN_OBSERVATIONS_PATH,
        ean,
        body.categories,
        body.name,
        body.quantity,
        prices_raw,
        receipt_names_raw,
    )
    # Update in-memory observations so subsequent GETs reflect the change immediately
    entry = _app.ean_observations.setdefault(ean, {})
    if body.categories:
        entry["categories"] = body.categories
    if body.name is not None:
        entry["name"] = body.name
    if body.quantity is not None:
        entry["quantity"] = body.quantity
    if prices_raw:
        existing = entry.get("prices", [])
        for p in prices_raw:
            key = (p.get("date"), p.get("currency"), p.get("price"))
            if not any((ep.get("date"), ep.get("currency"), ep.get("price")) == key for ep in existing):
                existing.append(p)
        entry["prices"] = existing
    if receipt_names_raw:
        existing_rn = entry.get("receipt_names", [])
        for rn in receipt_names_raw:
            if not any(e.get("name") == rn.get("name") and e.get("shop") == rn.get("shop") for e in existing_rn):
                existing_rn.append(rn)
        entry["receipt_names"] = existing_rn

    logger.info(
        "Stored EAN observation for %s: categories=%s name=%r quantity=%r prices=%d receipt_names=%d",
        ean,
        body.categories,
        body.name,
        body.quantity,
        len(prices_raw),
        len(receipt_names_raw),
    )

    # Return the full merged product view
    upstream = await asyncio.to_thread(ean_service.lookup_product, ean, _app.EAN_CACHE_DIR)
    if upstream is None:
        result = dict(entry)
        result.setdefault("ean", ean)
        result.setdefault("source", "observation")
    else:
        result = dict(upstream)
        result.setdefault("ean", ean)
        result = ean_service.merge_observation(result, entry)
    return ProductResponse(**result)
