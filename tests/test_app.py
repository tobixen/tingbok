"""Tests for the main FastAPI application."""

import pytest
from unittest.mock import patch


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


@pytest.mark.anyio
@pytest.mark.parametrize("concept_id,expected_uri", [
    ("food", "http://dbpedia.org/resource/Food"),
    ("tools", "http://dbpedia.org/resource/Tool"),
    ("medicine", "http://dbpedia.org/resource/Medicine"),
    ("bedding", "http://dbpedia.org/resource/Bedding"),
    ("washer", "http://dbpedia.org/resource/Washer_(hardware)"),
    ("seal", "http://dbpedia.org/resource/Hermetic_seal"),
    ("disc", "http://dbpedia.org/resource/Disc"),
    ("tubing", "http://dbpedia.org/resource/Tubing_(material)"),
    ("gps", "http://dbpedia.org/resource/Global_Positioning_System"),
])
async def test_concept_has_external_uri_in_source_uris(client, concept_id, expected_uri):
    """Concepts with a known URI should include it in source_uris."""
    response = await client.get(f"/api/vocabulary/{concept_id}")
    assert response.status_code == 200
    data = response.json()
    assert expected_uri in data["source_uris"], (
        f"Concept '{concept_id}' missing {expected_uri} in source_uris: {data['source_uris']}"
    )


@pytest.mark.anyio
@pytest.mark.parametrize("concept_id", [
    "washer", "seal", "disc", "tubing", "gps", "tool", "snacks",
    "bedding", "peanuts",
])
async def test_agrovoc_excluded_for_mismatch_concepts(client, concept_id):
    """Concepts known to cause AGROVOC mismatches must exclude agrovoc."""
    response = await client.get(f"/api/vocabulary/{concept_id}")
    assert response.status_code == 200
    data = response.json()
    assert "agrovoc" in data["excluded_sources"], (
        f"Concept '{concept_id}' should have agrovoc in excluded_sources"
    )


@pytest.mark.anyio
async def test_discover_source_uris_populates_memory(client):
    """Auto-discovery should add external URIs for concepts with empty source_uris."""
    import tingbok.app as app_module

    fake_uri = "http://dbpedia.org/resource/Electronics"
    def fake_lookup(label, lang, source, cache_dir):
        if source == "dbpedia" and label.lower() == "electronics":
            return {"uri": fake_uri, "prefLabel": "Electronics", "source": "dbpedia", "broader": []}
        return None

    # Reset discovered uris before test
    app_module._discovered_source_uris.clear()

    with patch("tingbok.app.skos_service.lookup_concept", side_effect=fake_lookup):
        await app_module._discover_source_uris_background()

    assert "electronics" in app_module._discovered_source_uris
    assert app_module._discovered_source_uris["electronics"].get("dbpedia") == fake_uri


@pytest.mark.anyio
async def test_discover_skips_concepts_with_known_uris(client):
    """Discovery should skip concepts that already have external URIs in vocabulary.yaml."""
    import tingbok.app as app_module

    app_module._discovered_source_uris.clear()

    call_count = 0
    def counting_lookup(label, lang, source, cache_dir):
        nonlocal call_count
        call_count += 1
        return None

    with patch("tingbok.app.skos_service.lookup_concept", side_effect=counting_lookup):
        await app_module._discover_source_uris_background()

    # "food" already has a DBpedia URI in vocabulary.yaml → should not be looked up
    food_calls = [
        1 for _ in range(call_count)  # just check call count is reduced
    ]
    # food, tools, mushrooms, snacks, peanuts, bedding, washer, medicine, gps, tool,
    # marine_propulsion, seal, tubing, disc all have source_uris → skipped
    # Only concepts without source_uris should be looked up
    concepts_with_uris = sum(
        1 for data in app_module.vocabulary.values() if data.get("source_uris")
    )
    total_concepts = len(app_module.vocabulary)
    expected_max_calls = (total_concepts - concepts_with_uris) * 2  # 2 sources: dbpedia, wikidata
    assert call_count <= expected_max_calls


@pytest.mark.anyio
async def test_discover_skips_excluded_sources(client):
    """Discovery should not query sources listed in excluded_sources."""
    import tingbok.app as app_module

    app_module._discovered_source_uris.clear()
    queried_sources: list[tuple[str, str]] = []

    def recording_lookup(label, lang, source, cache_dir):
        queried_sources.append((label, source))
        return None

    with patch("tingbok.app.skos_service.lookup_concept", side_effect=recording_lookup):
        await app_module._discover_source_uris_background()

    # No concept that has agrovoc in excluded_sources should be queried for agrovoc
    for concept_id, data in app_module.vocabulary.items():
        if "agrovoc" in data.get("excluded_sources", []):
            label = data.get("prefLabel", concept_id)
            assert (label, "agrovoc") not in queried_sources, (
                f"Concept '{concept_id}' with agrovoc excluded was still queried"
            )


@pytest.mark.anyio
async def test_vocabulary_api_includes_discovered_uris(client):
    """API should merge auto-discovered URIs into source_uris response."""
    import tingbok.app as app_module

    fake_uri = "http://dbpedia.org/resource/Electronics"
    app_module._discovered_source_uris["electronics"] = {"dbpedia": fake_uri}

    try:
        response = await client.get("/api/vocabulary/electronics")
        assert response.status_code == 200
        data = response.json()
        assert fake_uri in data["source_uris"]
    finally:
        app_module._discovered_source_uris.pop("electronics", None)
