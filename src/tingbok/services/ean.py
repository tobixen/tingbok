"""EAN lookup service — wraps upstream OFF, UPCitemdb, Open Library sources.

Lookup strategy:

* ISBN (EAN-13 starting with 978/979): Open Library → nb.no (Norwegian ISBNs).
* Other EAN/UPC: Open Food Facts → UPCitemdb.

Results (including not-found) are cached in the EAN cache directory.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

import niquests
import yaml

from tingbok.services.skos import (
    _add_to_not_found_cache,
    _get_cache_path,
    _is_in_not_found_cache,
    _load_from_cache,
    _save_to_cache,
)

logger = logging.getLogger(__name__)

DEFAULT_TIMEOUT = 10.0

_USER_AGENT = "tingbok/1.0 (https://github.com/tobixen/tingbok)"
_HEADERS = {"User-Agent": _USER_AGENT}


# ---------------------------------------------------------------------------
# ISBN helpers
# ---------------------------------------------------------------------------


def _is_isbn(code: str) -> bool:
    """Return True if *code* is an ISBN-13 (EAN starting with 978 or 979)."""
    return len(code) == 13 and code.isdigit() and code[:3] in ("978", "979")


def _isbn10_to_isbn13(isbn10: str) -> str:
    """Convert a 10-digit ISBN to ISBN-13."""
    base = "978" + isbn10[:-1]
    digits = [int(d) for d in base]
    total = sum(d * (1 if i % 2 == 0 else 3) for i, d in enumerate(digits))
    check = (10 - (total % 10)) % 10
    return base + str(check)


def _normalize_isbn(isbn: str) -> str:
    """Strip hyphens/spaces and normalise to ISBN-13."""
    cleaned = isbn.replace("-", "").replace(" ", "")
    if len(cleaned) == 10:
        cleaned = _isbn10_to_isbn13(cleaned)
    return cleaned


# ---------------------------------------------------------------------------
# Individual upstream fetch functions (each returns raw API JSON or raises)
# ---------------------------------------------------------------------------


def _fetch_off(ean: str) -> dict[str, Any]:
    """Fetch raw product data from Open Food Facts."""
    response = niquests.get(
        f"https://world.openfoodfacts.org/api/v2/product/{ean}.json",
        params={"fields": "product_name,product_name_en,brands,quantity,categories_tags,image_url"},
        headers=_HEADERS,
        timeout=DEFAULT_TIMEOUT,
    )
    response.raise_for_status()
    return response.json()  # type: ignore[return-value]


def _fetch_upcitemdb(ean: str) -> dict[str, Any]:
    """Fetch raw product data from UPCitemdb (trial API)."""
    response = niquests.get(
        f"https://api.upcitemdb.com/prod/trial/lookup?upc={ean}",
        headers={**_HEADERS, "Accept": "application/json"},
        timeout=DEFAULT_TIMEOUT,
    )
    response.raise_for_status()
    return response.json()  # type: ignore[return-value]


def _fetch_openlibrary(isbn: str) -> dict[str, Any]:
    """Fetch raw book data from Open Library."""
    response = niquests.get(
        f"https://openlibrary.org/isbn/{isbn}.json",
        headers=_HEADERS,
        timeout=DEFAULT_TIMEOUT,
    )
    response.raise_for_status()
    return response.json()  # type: ignore[return-value]


def _fetch_openlibrary_author(author_key: str) -> dict[str, Any]:
    """Fetch a single author record from Open Library."""
    response = niquests.get(
        f"https://openlibrary.org{author_key}.json",
        headers=_HEADERS,
        timeout=5.0,
    )
    response.raise_for_status()
    return response.json()  # type: ignore[return-value]


def _fetch_nb_no(isbn: str) -> dict[str, Any]:
    """Fetch raw book data from the Norwegian National Library (nb.no)."""
    response = niquests.get(
        f"https://api.nb.no/catalog/v1/items?q=isbn:{isbn}",
        headers=_HEADERS,
        timeout=DEFAULT_TIMEOUT,
    )
    response.raise_for_status()
    return response.json()  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# Source-specific parsers: raw JSON → normalised result dict
# ---------------------------------------------------------------------------


def _parse_off(ean: str, data: dict[str, Any]) -> dict[str, Any] | None:
    """Parse an OFF API response into a normalised product dict."""
    if data.get("status") == 0 or "product" not in data:
        return None
    product = data["product"]
    name = product.get("product_name") or product.get("product_name_en") or None
    if not name:
        return None

    categories: list[str] = []
    for tag in product.get("categories_tags") or []:
        if tag.startswith("en:"):
            categories.append(tag[3:].replace("-", " "))

    return {
        "ean": ean,
        "name": name,
        "brand": product.get("brands") or None,
        "quantity": product.get("quantity") or None,
        "categories": categories,
        "image_url": product.get("image_url") or None,
        "source": "openfoodfacts",
        "type": "product",
    }


def _parse_upcitemdb(ean: str, data: dict[str, Any]) -> dict[str, Any] | None:
    """Parse a UPCitemdb API response into a normalised product dict."""
    items = data.get("items") or []
    if not items:
        return None
    item = items[0]
    name = item.get("title") or None
    if not name:
        return None

    category = item.get("category")
    categories = [category] if category else []

    return {
        "ean": ean,
        "name": name,
        "brand": item.get("brand") or None,
        "quantity": item.get("size") or item.get("weight") or None,
        "categories": categories,
        "image_url": (item.get("images") or [None])[0],
        "source": "upcitemdb",
        "type": "product",
    }


def _parse_openlibrary(isbn: str, data: dict[str, Any]) -> dict[str, Any] | None:
    """Parse an Open Library API response into a normalised book dict."""
    title = data.get("title") or None
    if not title:
        return None

    # Fetch author names (best-effort; failures are ignored)
    authors: list[str] = []
    for author_ref in data.get("authors") or []:
        author_key = author_ref.get("key") if isinstance(author_ref, dict) else None
        if author_key:
            try:
                author_data = _fetch_openlibrary_author(author_key)
                name = author_data.get("name")
                if name:
                    authors.append(name)
            except Exception:  # noqa: BLE001
                pass

    subjects: list[str] = []
    for s in (data.get("subjects") or [])[:5]:
        label = s.get("name") if isinstance(s, dict) else s
        if label:
            subjects.append(str(label))

    return {
        "ean": isbn,
        "name": title,
        "brand": (data.get("publishers") or [None])[0],
        "quantity": None,
        "categories": subjects,
        "image_url": None,
        "source": "openlibrary",
        "author": ", ".join(authors) if authors else None,
        "type": "book",
    }


def _parse_nb_no(isbn: str, data: dict[str, Any]) -> dict[str, Any] | None:
    """Parse a nb.no API response into a normalised book dict."""
    items = (data.get("_embedded") or {}).get("items") or []
    if not items:
        return None
    metadata = items[0].get("metadata") or {}
    title = metadata.get("title") or None
    if not title:
        return None

    creators = metadata.get("creators") or []
    authors = [c for c in creators if isinstance(c, str)]
    origin = metadata.get("originInfo") or {}

    return {
        "ean": isbn,
        "name": title,
        "brand": origin.get("publisher") or None,
        "quantity": None,
        "categories": [],
        "image_url": None,
        "source": "nb.no",
        "author": ", ".join(authors) if authors else None,
        "type": "book",
    }


# ---------------------------------------------------------------------------
# High-level lookup orchestration
# ---------------------------------------------------------------------------


def _lookup_isbn(isbn: str) -> dict[str, Any] | None:
    """Try Open Library, then nb.no for Norwegian ISBNs."""
    try:
        data = _fetch_openlibrary(isbn)
        result = _parse_openlibrary(isbn, data)
        if result:
            return result
    except Exception as exc:
        logger.debug("Open Library lookup failed for %s: %s", isbn, exc)

    if isbn.startswith("97882"):
        try:
            data = _fetch_nb_no(isbn)
            result = _parse_nb_no(isbn, data)
            if result:
                return result
        except Exception as exc:
            logger.debug("nb.no lookup failed for %s: %s", isbn, exc)

    return None


def _lookup_ean(ean: str) -> dict[str, Any] | None:
    """Try Open Food Facts, then UPCitemdb."""
    try:
        data = _fetch_off(ean)
        result = _parse_off(ean, data)
        if result:
            return result
    except Exception as exc:
        logger.debug("OFF lookup failed for %s: %s", ean, exc)

    try:
        data = _fetch_upcitemdb(ean)
        result = _parse_upcitemdb(ean, data)
        if result:
            return result
    except Exception as exc:
        logger.debug("UPCitemdb lookup failed for %s: %s", ean, exc)

    return None


def lookup_product(code: str, cache_dir: Path) -> dict[str, Any] | None:
    """Look up a product or book by EAN/ISBN, trying multiple upstream sources.

    ISBNs (EAN-13 starting with 978/979) are routed to Open Library and nb.no.
    Other EAN/UPC codes are routed to Open Food Facts and UPCitemdb.

    Results (including not-found) are cached in *cache_dir*.

    Args:
        code:      EAN/UPC or ISBN-13 barcode string.
        cache_dir: Directory for the persistent product cache.

    Returns:
        Dict compatible with :class:`tingbok.models.ProductResponse`, or
        ``None`` if the product is not found in any upstream source.
    """
    isbn = _normalize_isbn(code) if _is_isbn(code) else None
    cache_key = f"ean:{isbn or code}"
    cache_path = _get_cache_path(cache_dir, cache_key)

    # Positive cache
    cached = _load_from_cache(cache_path)
    if cached is not None:
        return cached

    # Not-found cache
    if _is_in_not_found_cache(cache_dir, cache_key):
        return None

    result = _lookup_isbn(isbn) if isbn else _lookup_ean(code)

    if result is None:
        _add_to_not_found_cache(cache_dir, cache_key)
    else:
        _save_to_cache(cache_path, result)

    return result


def load_manual_ean(path: Path) -> dict[str, Any]:
    """Load manually curated EAN data from a YAML file.

    Returns an empty dict if the file does not exist or cannot be parsed.
    Keys are EAN strings; values are dicts with supplementary or full product data.
    """
    if not path.exists():
        return {}
    try:
        with open(path, encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
        # Normalise keys to strings (YAML may parse numeric EANs as ints)
        return {str(k): v for k, v in data.items()}
    except Exception as exc:
        logger.warning("Failed to load manual EAN data from %s: %s", path, exc)
        return {}


def load_ean_observations(path: Path) -> dict[str, Any]:
    """Load inventory-sourced EAN observations from *path* (JSON).

    Returns an empty dict if the file does not exist or cannot be parsed.
    Keys are EAN strings; values are dicts with ``categories`` and/or ``name``.
    """
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("Failed to load EAN observations from %s: %s", path, exc)
        return {}


def save_ean_observation(
    path: Path,
    ean: str,
    categories: list[str],
    name: str | None,
    quantity: str | None = None,
    prices: list[dict[str, Any]] | None = None,
    receipt_names: list[dict[str, Any]] | None = None,
) -> None:
    """Persist a single EAN observation to *path* (JSON), merging with existing data.

    Existing entries for *ean* are updated in-place; all other EANs are preserved.
    Price observations are appended (de-duplicated by date+currency+price).
    """
    data = load_ean_observations(path)
    entry: dict[str, Any] = data.get(ean, {})
    if categories:
        entry["categories"] = categories
    if name:
        entry["name"] = name
    if quantity:
        entry["quantity"] = quantity
    if prices:
        existing_prices: list[dict[str, Any]] = entry.get("prices", [])
        for p in prices:
            key = (p.get("date"), p.get("currency"), p.get("price"))
            if not any((ep.get("date"), ep.get("currency"), ep.get("price")) == key for ep in existing_prices):
                existing_prices.append(p)
        entry["prices"] = existing_prices
    if receipt_names:
        existing_rn: list[dict[str, Any]] = entry.get("receipt_names", [])
        for rn in receipt_names:
            rn_name = rn.get("name", "")
            rn_shop = rn.get("shop")
            existing = next((e for e in existing_rn if e.get("name") == rn_name and e.get("shop") == rn_shop), None)
            if existing:
                # Update last_seen date if newer
                if rn.get("last_seen", "") > existing.get("last_seen", ""):
                    existing["last_seen"] = rn["last_seen"]
            else:
                existing_rn.append(rn)
        entry["receipt_names"] = existing_rn
    data[ean] = entry
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    logger.debug("Saved EAN observation for %s to %s", ean, path)


def merge_observation(result: dict[str, Any], observation: dict[str, Any]) -> dict[str, Any]:
    """Merge an inventory observation into a product result dict.

    * ``categories``: observation categories are prepended (de-duplicated).
    * ``name``: used only when the result has no name yet.
    * ``quantity``: used only when the result has no quantity yet.
    * ``prices``: appended to existing price observations (de-duplicated).
    """
    if not observation:
        return result
    merged = dict(result)
    obs_cats: list[str] = observation.get("categories") or []
    if obs_cats:
        existing: list[str] = list(merged.get("categories") or [])
        for cat in reversed(obs_cats):
            if cat not in existing:
                existing.insert(0, cat)
        merged["categories"] = existing
    if observation.get("name") and not merged.get("name"):
        merged["name"] = observation["name"]
    if observation.get("quantity") and not merged.get("quantity"):
        merged["quantity"] = observation["quantity"]
    obs_prices: list[dict[str, Any]] = observation.get("prices") or []
    if obs_prices:
        existing_prices: list[dict[str, Any]] = list(merged.get("prices") or [])
        for p in obs_prices:
            key = (p.get("date"), p.get("currency"), p.get("price"))
            if not any((ep.get("date"), ep.get("currency"), ep.get("price")) == key for ep in existing_prices):
                existing_prices.append(p)
        merged["prices"] = existing_prices
    obs_rn: list[dict[str, Any]] = observation.get("receipt_names") or []
    if obs_rn:
        existing_rn: list[dict[str, Any]] = list(merged.get("receipt_names") or [])
        for rn in obs_rn:
            rn_name = rn.get("name", "")
            rn_shop = rn.get("shop")
            if not any(e.get("name") == rn_name and e.get("shop") == rn_shop for e in existing_rn):
                existing_rn.append(rn)
        merged["receipt_names"] = existing_rn
    return merged


def merge_manual_data(upstream: dict[str, Any] | None, manual: dict[str, Any] | None) -> dict[str, Any] | None:
    """Merge upstream product data with a manual-ean.yaml entry.

    * If upstream is present: supplementary fields (``prices``, ``receipt_names``,
      ``note``) from *manual* are merged in; all other upstream fields are preserved.
    * If upstream is absent and *manual* has ``source: manual``: the manual entry
      is returned as the product result.
    * Otherwise: ``None`` is returned.
    """
    if upstream is not None:
        if not manual:
            return upstream
        result = dict(upstream)
        for key in ("prices", "receipt_names"):
            if manual.get(key):
                result[key] = manual[key]
        if manual.get("note"):
            result["note"] = manual["note"]
        return result

    if manual and manual.get("source") == "manual":
        return dict(manual)

    return None
