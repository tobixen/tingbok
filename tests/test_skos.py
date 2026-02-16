"""Tests for SKOS router endpoints."""

import pytest


@pytest.mark.anyio
async def test_skos_lookup_stub(client):
    response = await client.get("/api/skos/lookup", params={"label": "potato"})
    assert response.status_code == 501


@pytest.mark.anyio
async def test_skos_hierarchy_stub(client):
    response = await client.get("/api/skos/hierarchy", params={"label": "potato"})
    assert response.status_code == 501


@pytest.mark.anyio
async def test_skos_labels_stub(client):
    response = await client.get("/api/skos/labels", params={"uri": "http://example.org/potato"})
    assert response.status_code == 501
