"""Tests for EAN router endpoints and EAN lookup service."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _off_found(
    ean: str, name: str, brands: str = "Brand", quantity: str = "100g", categories_tags: list[str] | None = None
) -> dict:
    return {
        "status": 1,
        "product": {
            "product_name": name,
            "brands": brands,
            "quantity": quantity,
            "categories_tags": categories_tags or ["en:food"],
            "image_url": None,
        },
    }


def _off_not_found() -> dict:
    return {"status": 0, "status_verbose": "product not found"}


def _upcitemdb_found(ean: str, title: str, brand: str = "Brand", category: str = "Electronics") -> dict:
    return {"items": [{"title": title, "brand": brand, "category": category, "images": []}]}


def _upcitemdb_not_found() -> dict:
    return {"items": []}


def _openlibrary_found(title: str, with_author: bool = True) -> dict:
    return {
        "title": title,
        "authors": [{"key": "/authors/OL123A"}] if with_author else [],
        "publishers": ["Penguin"],
        "subjects": [{"name": "Fiction"}, {"name": "Adventure"}],
    }


def _nb_no_found(title: str, authors: list[str] | None = None) -> dict:
    return {
        "_embedded": {
            "items": [
                {
                    "metadata": {
                        "title": title,
                        "creators": authors or [],
                        "originInfo": {"publisher": "Gyldendal", "issued": "2020"},
                    }
                }
            ]
        }
    }


# ---------------------------------------------------------------------------
# Service-level tests — OFF
# ---------------------------------------------------------------------------


class TestLookupProductOFF:
    def test_found_product_returns_dict(self, tmp_path: Path) -> None:
        from tingbok.services import ean as ean_service

        off_data = _off_found(
            "7310865004703", "Kalles Kaviar", "Abba", "300g", ["en:spreads", "en:fish-spreads", "en:caviar-spreads"]
        )
        with patch("tingbok.services.ean._fetch_off", return_value=off_data):
            with patch("tingbok.services.ean._fetch_upcitemdb"):  # should not be called
                result = ean_service.lookup_product("7310865004703", tmp_path)

        assert result is not None
        assert result["ean"] == "7310865004703"
        assert result["name"] == "Kalles Kaviar"
        assert result["brand"] == "Abba"
        assert result["quantity"] == "300g"
        assert result["source"] == "openfoodfacts"
        assert result["type"] == "product"

    def test_categories_extracted_from_tags(self, tmp_path: Path) -> None:
        from tingbok.services import ean as ean_service

        off_data = _off_found(
            "7310865004703", "Kalles Kaviar", categories_tags=["en:spreads", "en:fish-spreads", "en:caviar-spreads"]
        )
        with patch("tingbok.services.ean._fetch_off", return_value=off_data):
            result = ean_service.lookup_product("7310865004703", tmp_path)

        assert result is not None
        assert "caviar spreads" in result["categories"]

    def test_non_english_categories_excluded(self, tmp_path: Path) -> None:
        from tingbok.services import ean as ean_service

        off_data = _off_found("1234567890128", "Product", categories_tags=["fr:produits-alimentaires", "en:snacks"])
        with patch("tingbok.services.ean._fetch_off", return_value=off_data):
            result = ean_service.lookup_product("1234567890128", tmp_path)

        assert result is not None
        assert "snacks" in result["categories"]
        assert "produits-alimentaires" not in result["categories"]


# ---------------------------------------------------------------------------
# Service-level tests — UPCitemdb fallback
# ---------------------------------------------------------------------------


class TestLookupProductUPCItemdb:
    def test_falls_back_to_upcitemdb_when_off_returns_nothing(self, tmp_path: Path) -> None:
        from tingbok.services import ean as ean_service

        with patch("tingbok.services.ean._fetch_off", return_value=_off_not_found()):
            with patch(
                "tingbok.services.ean._fetch_upcitemdb",
                return_value=_upcitemdb_found("0012345678905", "Widget Pro", "Acme", "Tools"),
            ):
                result = ean_service.lookup_product("0012345678905", tmp_path)

        assert result is not None
        assert result["name"] == "Widget Pro"
        assert result["source"] == "upcitemdb"
        assert result["categories"] == ["Tools"]

    def test_falls_back_to_upcitemdb_when_off_raises(self, tmp_path: Path) -> None:
        from tingbok.services import ean as ean_service

        with patch("tingbok.services.ean._fetch_off", side_effect=Exception("network error")):
            with patch(
                "tingbok.services.ean._fetch_upcitemdb", return_value=_upcitemdb_found("0012345678905", "Widget Pro")
            ):
                result = ean_service.lookup_product("0012345678905", tmp_path)

        assert result is not None
        assert result["source"] == "upcitemdb"

    def test_not_found_in_both_sources_returns_none(self, tmp_path: Path) -> None:
        from tingbok.services import ean as ean_service

        with patch("tingbok.services.ean._fetch_off", return_value=_off_not_found()):
            with patch("tingbok.services.ean._fetch_upcitemdb", return_value=_upcitemdb_not_found()):
                result = ean_service.lookup_product("0000000000000", tmp_path)

        assert result is None


# ---------------------------------------------------------------------------
# Service-level tests — ISBN / Open Library
# ---------------------------------------------------------------------------


class TestLookupISBN:
    ISBN = "9780134685991"  # EAN-13 starting with 978

    def test_isbn_routed_to_openlibrary(self, tmp_path: Path) -> None:
        from tingbok.services import ean as ean_service

        with patch("tingbok.services.ean._fetch_openlibrary", return_value=_openlibrary_found("Clean Code")) as mock_ol:
            with patch("tingbok.services.ean._fetch_openlibrary_author", return_value={"name": "Robert Martin"}):
                with patch("tingbok.services.ean._fetch_off") as mock_off:
                    result = ean_service.lookup_product(self.ISBN, tmp_path)

        mock_ol.assert_called_once()
        mock_off.assert_not_called()
        assert result is not None
        assert result["name"] == "Clean Code"
        assert result["source"] == "openlibrary"
        assert result["type"] == "book"
        assert result["author"] == "Robert Martin"

    def test_isbn_subjects_as_categories(self, tmp_path: Path) -> None:
        from tingbok.services import ean as ean_service

        with patch("tingbok.services.ean._fetch_openlibrary", return_value=_openlibrary_found("Clean Code")):
            with patch("tingbok.services.ean._fetch_openlibrary_author", return_value={}):
                result = ean_service.lookup_product(self.ISBN, tmp_path)

        assert result is not None
        assert "Fiction" in result["categories"]

    def test_norwegian_isbn_falls_back_to_nb_no(self, tmp_path: Path) -> None:
        from tingbok.services import ean as ean_service

        norwegian_isbn = "9788205314726"  # starts with 97882
        with patch("tingbok.services.ean._fetch_openlibrary", side_effect=Exception("not found")):
            with patch(
                "tingbok.services.ean._fetch_nb_no", return_value=_nb_no_found("Hunger", ["Knut Hamsun"])
            ) as mock_nb:
                result = ean_service.lookup_product(norwegian_isbn, tmp_path)

        mock_nb.assert_called_once()
        assert result is not None
        assert result["name"] == "Hunger"
        assert result["source"] == "nb.no"
        assert result["author"] == "Knut Hamsun"

    def test_non_norwegian_isbn_does_not_try_nb_no(self, tmp_path: Path) -> None:
        from tingbok.services import ean as ean_service

        with patch("tingbok.services.ean._fetch_openlibrary", side_effect=Exception("not found")):
            with patch("tingbok.services.ean._fetch_nb_no") as mock_nb:
                result = ean_service.lookup_product(self.ISBN, tmp_path)

        mock_nb.assert_not_called()
        assert result is None


# ---------------------------------------------------------------------------
# Caching
# ---------------------------------------------------------------------------


class TestCaching:
    def test_found_product_is_cached(self, tmp_path: Path) -> None:
        from tingbok.services import ean as ean_service

        off_data = _off_found("7310865004703", "Kalles Kaviar")
        with patch("tingbok.services.ean._fetch_off", return_value=off_data) as mock_fetch:
            ean_service.lookup_product("7310865004703", tmp_path)
            mock_fetch.reset_mock()
            result = ean_service.lookup_product("7310865004703", tmp_path)
            mock_fetch.assert_not_called()

        assert result is not None
        assert result["name"] == "Kalles Kaviar"

    def test_not_found_is_cached(self, tmp_path: Path) -> None:
        from tingbok.services import ean as ean_service

        with patch("tingbok.services.ean._fetch_off", return_value=_off_not_found()):
            with patch("tingbok.services.ean._fetch_upcitemdb", return_value=_upcitemdb_not_found()) as mock:
                ean_service.lookup_product("0000000000000", tmp_path)
                mock.reset_mock()
                result = ean_service.lookup_product("0000000000000", tmp_path)
                mock.assert_not_called()

        assert result is None


# ---------------------------------------------------------------------------
# Router-level tests
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_ean_lookup_found(client) -> None:
    from tingbok.services import ean as ean_service

    product = {
        "ean": "7310865004703",
        "name": "Kalles Kaviar",
        "brand": "Abba",
        "quantity": "300g",
        "categories": ["spreads", "caviar spreads"],
        "image_url": None,
        "source": "openfoodfacts",
        "author": None,
        "type": "product",
    }
    with patch.object(ean_service, "lookup_product", return_value=product):
        response = await client.get("/api/ean/7310865004703")

    assert response.status_code == 200
    data = response.json()
    assert data["ean"] == "7310865004703"
    assert data["name"] == "Kalles Kaviar"
    assert "caviar spreads" in data["categories"]


@pytest.mark.anyio
async def test_ean_lookup_not_found(client) -> None:
    from tingbok.services import ean as ean_service

    with patch.object(ean_service, "lookup_product", return_value=None):
        response = await client.get("/api/ean/0000000000000")

    assert response.status_code == 404


@pytest.mark.anyio
async def test_isbn_lookup_returns_book_type(client) -> None:
    from tingbok.services import ean as ean_service

    book = {
        "ean": "9780134685991",
        "name": "Clean Code",
        "brand": "Pearson",
        "quantity": None,
        "categories": ["Programming"],
        "image_url": None,
        "source": "openlibrary",
        "author": "Robert Martin",
        "type": "book",
    }
    with patch.object(ean_service, "lookup_product", return_value=book):
        response = await client.get("/api/ean/9780134685991")

    assert response.status_code == 200
    data = response.json()
    assert data["type"] == "book"
    assert data["author"] == "Robert Martin"
