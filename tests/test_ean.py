"""Tests for EAN router endpoints."""

import pytest


@pytest.mark.anyio
async def test_ean_lookup_stub(client):
    response = await client.get("/api/ean/7310865004703")
    assert response.status_code == 501
