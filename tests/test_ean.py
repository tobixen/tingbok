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


@pytest.mark.anyio
async def test_manual_only_ean_returns_200(client) -> None:
    """EAN with source=manual and no upstream data returns 200 with ean field injected."""
    import tingbok.app as _app
    from tingbok.services import ean as ean_service

    manual_entry = {
        "name": "Welding electrodes",
        "brand": "GRAPHITE",
        "categories": ["Tools"],
        "source": "manual",
        "type": "product",
    }
    with patch.object(ean_service, "lookup_product", return_value=None):
        with patch.object(_app, "manual_ean", {"2037795575": manual_entry}):
            response = await client.get("/api/ean/2037795575")

    assert response.status_code == 200
    data = response.json()
    assert data["ean"] == "2037795575"
    assert data["name"] == "Welding electrodes"
    assert data["source"] == "manual"


# ---------------------------------------------------------------------------
# Manual EAN data: loading and merging
# ---------------------------------------------------------------------------


class TestManualEanData:
    """Tests for manual-ean.yaml loading and merge into lookup_product results."""

    def test_load_manual_ean_returns_dict(self, tmp_path: Path) -> None:
        """load_manual_ean() parses the YAML and returns a plain dict."""
        from tingbok.services import ean as ean_service

        yaml_file = tmp_path / "manual-ean.yaml"
        yaml_file.write_text(
            "7310865004703:\n"
            "  prices:\n"
            "    - shop: Test Shop\n"
            "      date: '2026-01-01'\n"
            "      price: 9.99\n"
            "      currency: NOK\n"
        )
        data = ean_service.load_manual_ean(yaml_file)
        assert "7310865004703" in data
        assert data["7310865004703"]["prices"][0]["price"] == 9.99

    def test_load_manual_ean_missing_file_returns_empty(self, tmp_path: Path) -> None:
        """Missing manual-ean.yaml returns {} without raising."""
        from tingbok.services import ean as ean_service

        data = ean_service.load_manual_ean(tmp_path / "nonexistent.yaml")
        assert data == {}

    def test_merge_adds_prices_to_upstream_result(self, tmp_path: Path) -> None:
        """Prices from manual data are merged into an upstream product result."""
        from tingbok.services import ean as ean_service

        manual = {
            "7310865004703": {
                "prices": [{"shop": "ICA", "date": "2026-01-01", "price": 29.9, "currency": "NOK"}],
                "receipt_names": [
                    {"shop": "ICA", "name": "KAVIAR", "first_seen": "2026-01-01", "last_seen": "2026-01-01"}
                ],
                "note": "sale price",
            }
        }
        upstream = {
            "ean": "7310865004703",
            "name": "Kalles Kaviar",
            "brand": "Abba",
            "quantity": "300g",
            "categories": ["spreads"],
            "source": "openfoodfacts",
            "type": "product",
        }
        result = ean_service.merge_manual_data(upstream, manual.get("7310865004703"))
        assert result["prices"] == manual["7310865004703"]["prices"]
        assert result["receipt_names"] == manual["7310865004703"]["receipt_names"]
        assert result["note"] == "sale price"
        # Upstream fields preserved
        assert result["name"] == "Kalles Kaviar"

    def test_merge_manual_source_used_when_no_upstream(self, tmp_path: Path) -> None:
        """Manual entry with source=manual is returned when upstream finds nothing."""
        from tingbok.services import ean as ean_service

        manual_entry = {
            "name": "Welding electrodes",
            "brand": "GRAPHITE",
            "categories": ["Tools", "Welding"],
            "source": "manual",
            "type": "product",
        }
        result = ean_service.merge_manual_data(None, manual_entry)
        assert result is not None
        assert result["name"] == "Welding electrodes"
        assert result["source"] == "manual"

    def test_merge_no_manual_entry_returns_upstream_unchanged(self) -> None:
        """When no manual entry exists, upstream result is returned as-is."""
        from tingbok.services import ean as ean_service

        upstream = {
            "ean": "1234567890128",
            "name": "Test product",
            "categories": ["food"],
            "source": "openfoodfacts",
            "type": "product",
        }
        result = ean_service.merge_manual_data(upstream, None)
        assert result == upstream

    def test_merge_no_data_anywhere_returns_none(self) -> None:
        """None upstream + None manual entry → None."""
        from tingbok.services import ean as ean_service

        assert ean_service.merge_manual_data(None, None) is None

    def test_product_response_accepts_prices_and_receipt_names(self) -> None:
        """ProductResponse model accepts the new fields."""
        from tingbok.models import PriceObservation, ProductResponse, ReceiptNameObservation

        r = ProductResponse(
            ean="7310865004703",
            name="Kaviar",
            source="openfoodfacts",
            prices=[PriceObservation(shop="ICA", date="2026-01-01", price=29.9, currency="NOK")],
            receipt_names=[
                ReceiptNameObservation(shop="ICA", name="KAVIAR", first_seen="2026-01-01", last_seen="2026-01-01")
            ],
            note="sale",
        )
        assert r.prices[0].price == 29.9
        assert r.receipt_names[0].name == "KAVIAR"
        assert r.note == "sale"


# ---------------------------------------------------------------------------
# EAN observation: save / load / merge
# ---------------------------------------------------------------------------


class TestEanObservations:
    def test_save_and_load_observation(self, tmp_path: Path) -> None:
        """save_ean_observation persists to JSON; load_ean_observations reads it back."""
        from tingbok.services import ean as ean_service

        path = tmp_path / "ean-db.json"
        ean_service.save_ean_observation(path, "1234567890", ["food/dairy"], "Milk", quantity="1l")
        data = ean_service.load_ean_observations(path)
        assert data["1234567890"]["categories"] == ["food/dairy"]
        assert data["1234567890"]["name"] == "Milk"
        assert data["1234567890"]["quantity"] == "1l"

    def test_save_merges_prices_without_duplicates(self, tmp_path: Path) -> None:
        """Saving the same price twice does not create a duplicate."""
        from tingbok.services import ean as ean_service

        path = tmp_path / "ean-db.json"
        price = {"date": "2026-01-01", "currency": "EUR", "price": 1.5, "unit": "pcs", "shop": None}
        ean_service.save_ean_observation(path, "111", [], None, prices=[price])
        ean_service.save_ean_observation(path, "111", [], None, prices=[price])
        data = ean_service.load_ean_observations(path)
        assert len(data["111"]["prices"]) == 1

    def test_merge_observation_prepends_categories(self) -> None:
        """Inventory categories are prepended, giving them priority."""
        from tingbok.services import ean as ean_service

        result = {"ean": "1", "source": "off", "categories": ["Food/Dairy"], "name": "Milk"}
        obs = {"categories": ["food/dairy/yogurt"]}
        merged = ean_service.merge_observation(result, obs)
        assert merged["categories"][0] == "food/dairy/yogurt"
        assert "Food/Dairy" in merged["categories"]

    def test_merge_observation_fills_missing_name_and_quantity(self) -> None:
        """name and quantity from observation fill gaps in upstream data."""
        from tingbok.services import ean as ean_service

        result = {"ean": "1", "source": "off", "categories": []}
        obs = {"name": "Organic Milk", "quantity": "1l"}
        merged = ean_service.merge_observation(result, obs)
        assert merged["name"] == "Organic Milk"
        assert merged["quantity"] == "1l"

    def test_merge_observation_does_not_overwrite_existing_name(self) -> None:
        """Upstream name takes precedence over observation name."""
        from tingbok.services import ean as ean_service

        result = {"ean": "1", "source": "off", "categories": [], "name": "Official Name"}
        obs = {"name": "Inventory Name"}
        merged = ean_service.merge_observation(result, obs)
        assert merged["name"] == "Official Name"


# ---------------------------------------------------------------------------
# PUT /api/ean/{ean} endpoint
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_put_ean_observation_stores_and_returns_product(client, tmp_path: Path) -> None:
    """PUT /api/ean/{ean} stores the observation and returns a merged product."""
    from unittest.mock import patch

    import tingbok.app as _app

    obs_path = tmp_path / "ean-db.json"
    upstream = {
        "ean": "4006381333931",
        "name": "Tesa tape",
        "source": "upcitemdb",
        "categories": [],
    }
    with patch.object(_app, "EAN_OBSERVATIONS_PATH", obs_path):
        with patch.object(_app, "ean_observations", {}):
            with patch("tingbok.services.ean.lookup_product", return_value=upstream):
                response = await client.put(
                    "/api/ean/4006381333931",
                    json={"categories": ["household/office"], "name": "Tesa tape", "quantity": "10m"},
                )
    assert response.status_code == 200
    data = response.json()
    assert "household/office" in data["categories"]
    assert data["quantity"] == "10m"
    assert obs_path.exists()


@pytest.mark.anyio
async def test_put_ean_observation_empty_body_returns_422(client) -> None:
    """PUT with neither categories nor name is rejected."""
    response = await client.put("/api/ean/1234567890", json={})
    assert response.status_code == 422
