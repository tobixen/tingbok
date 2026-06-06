"""Tests for reverse receipt-name -> EAN search.

Receipt parsers (e.g. Lidl) only print a localised receipt name, not the EAN.
These let a shopping importer propose candidate EANs by matching the printed
receipt name against the ``receipt_names`` observations stored in ean-db.json.
"""

from unittest.mock import patch

import pytest

import tingbok.app as _app
from tingbok.services import ean as ean_service

# A small synthetic observation DB shaped like ean-db.json entries.
OBSERVATIONS = {
    "4056489080510": {
        "name": "Pilos Fresh Milk 3% 1l",
        "categories": ["food/dairy"],
        "receipt_names": [
            {"shop": "Lidl Varna", "name": "ПРЯСНО МЛЯКО 3%"},
        ],
    },
    "4056489080527": {
        "name": "Pilos Fresh Milk 3.7% 1l",
        "categories": ["food/dairy"],
        "receipt_names": [
            {"shop": "Lidl Varna", "name": "ПРЯСНО МЛЯКО 3,7%"},
        ],
    },
    "4054500440923": {
        "name": "Steam Brew German Red amber beer 500ml",
        "categories": ["food/beverages/beer"],
        "receipt_names": [
            {"shop": "Kaufland", "name": "СВЕТЛА БИРА 3,0%"},
        ],
    },
    "0000000000000": {
        "name": "No receipt names here",
        "categories": ["misc"],
    },
}


class TestSearchByReceiptNameService:
    def test_exact_match_scores_one(self) -> None:
        results = ean_service.search_by_receipt_name(OBSERVATIONS, "ПРЯСНО МЛЯКО 3,7%")
        assert results
        assert results[0]["ean"] == "4056489080527"
        assert results[0]["score"] == pytest.approx(1.0)
        assert results[0]["matched_name"] == "ПРЯСНО МЛЯКО 3,7%"

    def test_exact_match_is_case_and_space_insensitive(self) -> None:
        results = ean_service.search_by_receipt_name(OBSERVATIONS, "  пРЯСНО   мляко 3,7%  ")
        assert results[0]["ean"] == "4056489080527"
        assert results[0]["score"] == pytest.approx(1.0)

    def test_fuzzy_match_ranks_close_names(self) -> None:
        # Query close to the 3% milk; both milks should appear, 3% ranked first.
        results = ean_service.search_by_receipt_name(OBSERVATIONS, "ПРЯСНО МЛЯКО 3%")
        eans = [r["ean"] for r in results]
        assert eans[0] == "4056489080510"
        assert "4056489080527" in eans  # the 3,7% milk is a near-miss candidate
        assert all(0.0 < r["score"] <= 1.0 for r in results)

    def test_shop_filter_excludes_other_shops(self) -> None:
        results = ean_service.search_by_receipt_name(OBSERVATIONS, "СВЕТЛА БИРА 3,0%", shop="Lidl Varna")
        assert results == []

    def test_min_score_filters_weak_matches(self) -> None:
        results = ean_service.search_by_receipt_name(OBSERVATIONS, "completely unrelated text", min_score=0.5)
        assert results == []

    def test_limit_caps_results(self) -> None:
        results = ean_service.search_by_receipt_name(OBSERVATIONS, "ПРЯСНО МЛЯКО", limit=1, min_score=0.0)
        assert len(results) == 1

    def test_entries_without_receipt_names_are_ignored(self) -> None:
        results = ean_service.search_by_receipt_name(OBSERVATIONS, "No receipt names here", min_score=0.0)
        assert all(r["ean"] != "0000000000000" for r in results)

    def test_results_carry_product_name(self) -> None:
        results = ean_service.search_by_receipt_name(OBSERVATIONS, "ПРЯСНО МЛЯКО 3,7%")
        assert results[0]["name"] == "Pilos Fresh Milk 3.7% 1l"


@pytest.mark.anyio
async def test_search_endpoint_returns_ranked_candidates(client) -> None:
    with patch.object(_app, "ean_observations", OBSERVATIONS):
        response = await client.get("/api/ean/search", params={"receipt_name": "ПРЯСНО МЛЯКО 3,7%"})
    assert response.status_code == 200
    data = response.json()
    assert data["query"] == "ПРЯСНО МЛЯКО 3,7%"
    assert data["results"][0]["ean"] == "4056489080527"
    assert data["results"][0]["score"] == pytest.approx(1.0)


@pytest.mark.anyio
async def test_search_endpoint_no_match_returns_empty(client) -> None:
    with patch.object(_app, "ean_observations", OBSERVATIONS):
        response = await client.get("/api/ean/search", params={"receipt_name": "zzz nonexistent"})
    assert response.status_code == 200
    assert response.json()["results"] == []


@pytest.mark.anyio
async def test_search_endpoint_requires_receipt_name(client) -> None:
    response = await client.get("/api/ean/search")
    assert response.status_code == 422


@pytest.mark.anyio
async def test_search_route_not_shadowed_by_ean_route(client) -> None:
    """The static /search path must win over the dynamic /{ean} catch-all."""
    with patch.object(_app, "ean_observations", OBSERVATIONS):
        response = await client.get("/api/ean/search", params={"receipt_name": "x"})
    # 200 (search ran), not 404 (treated as EAN 'search' lookup miss)
    assert response.status_code == 200
    assert "results" in response.json()
