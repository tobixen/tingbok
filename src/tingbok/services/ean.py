"""EAN lookup service — wraps upstream OFF, UPCitemdb, Open Library sources.

Lookup strategy:

* ISBN (EAN-13 starting with 978/979): Open Library → nb.no (Norwegian ISBNs).
* Other EAN/UPC: Open Food Facts → UPCitemdb.

Results (including not-found) are cached in the EAN cache directory.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import niquests

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
