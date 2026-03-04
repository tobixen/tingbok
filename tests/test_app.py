"""Tests for the main FastAPI application."""

import pytest


@pytest.mark.anyio
async def test_health(client):
    response = await client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert "version" in data


@pytest.mark.anyio
async def test_get_vocabulary(client):
    response = await client.get("/api/vocabulary")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, dict)
    assert "food" in data
    assert data["food"]["prefLabel"] == "Food"


@pytest.mark.anyio
async def test_get_vocabulary_concept(client):
    response = await client.get("/api/vocabulary/food")
    assert response.status_code == 200
    data = response.json()
    assert data["id"] == "food"
    assert data["prefLabel"] == "Food"
    assert "en" in data["altLabel"]


@pytest.mark.anyio
async def test_get_vocabulary_concept_not_found(client):
    response = await client.get("/api/vocabulary/nonexistent")
    assert response.status_code == 404


@pytest.mark.anyio
async def test_vocabulary_concept_has_source_uris(client):
    response = await client.get("/api/vocabulary/food")
    assert response.status_code == 200
    data = response.json()
    assert "source_uris" in data
    assert isinstance(data["source_uris"], list)
    assert len(data["source_uris"]) >= 1
    # Should contain the tingbok self-URI
    assert any("tingbok.plann.no" in u for u in data["source_uris"])


@pytest.mark.anyio
async def test_vocabulary_concept_has_excluded_sources(client):
    response = await client.get("/api/vocabulary/food")
    assert response.status_code == 200
    data = response.json()
    assert "excluded_sources" in data
    assert isinstance(data["excluded_sources"], list)


@pytest.mark.anyio
async def test_full_vocabulary_has_source_uris(client):
    response = await client.get("/api/vocabulary")
    assert response.status_code == 200
    data = response.json()
    for concept_id, concept in data.items():
        assert "source_uris" in concept, f"Concept {concept_id} missing source_uris"
        assert isinstance(concept["source_uris"], list)
        # Every concept should have at least the tingbok self-URI
        assert len(concept["source_uris"]) >= 1, f"Concept {concept_id} has empty source_uris"
