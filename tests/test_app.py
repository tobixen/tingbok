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
