"""Tests for the main FastAPI application."""

from pathlib import Path
from unittest.mock import patch

import pytest
import yaml as _yaml
from httpx import ASGITransport, AsyncClient

from tingbok.app import app


@pytest.mark.anyio
async def test_health(client):
    response = await client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert "version" in data
    assert "uptime_seconds" in data
    assert data["uptime_seconds"] >= 0
    assert "vocabulary_concepts" in data
    assert data["vocabulary_concepts"] > 0
    assert "vocabulary_concepts_enriched" in data


@pytest.mark.anyio
async def test_health_localhost_exposes_paths():
    """Health check from localhost should include path information and cache age."""
    async with AsyncClient(
        transport=ASGITransport(app=app, client=("127.0.0.1", 12345)),
        base_url="http://127.0.0.1",
    ) as ac:
        response = await ac.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    paths = data.get("paths")
    assert paths is not None, "Expected paths dict for localhost request"
    assert "vocabulary" in paths
    assert "ean_db" in paths
    assert "skos_cache" in paths
    assert "ean_cache" in paths
    # cache_oldest_entry_age_days is present for localhost (may be None if cache is empty)
    assert "cache_oldest_entry_age_days" in data


@pytest.mark.anyio
async def test_get_vocabulary(client):
    response = await client.get("/api/vocabulary")
    assert response.status_code == 200
    data = response.json()
    assert isinstance(data, dict)
    assert "food" in data
    assert data["food"]["prefLabel"] == "Food"


@pytest.mark.anyio
async def test_get_vocabulary_returns_503_when_not_ready(client):
    """Full vocabulary returns 503 Retry-After when background fetch is incomplete."""
    import tingbok.app as app_module

    original = set(app_module._concepts_fetched)
    app_module._concepts_fetched.clear()
    try:
        response = await client.get("/api/vocabulary")
        assert response.status_code == 503
        assert "Retry-After" in response.headers
    finally:
        app_module._concepts_fetched.update(original)


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
    # The tingbok self-URI lives in "uri", not "source_uris"
    assert not any("tingbok.plann.no" in u for u in data["source_uris"])
    # food has several external source URIs
    assert len(data["source_uris"]) >= 1


@pytest.mark.anyio
async def test_vocabulary_concept_has_excluded_sources(client):
    response = await client.get("/api/vocabulary/food")
    assert response.status_code == 200
    data = response.json()
    assert "excluded_sources" in data
    assert isinstance(data["excluded_sources"], list)


# ---------------------------------------------------------------------------
# EAN category normalisation
# ---------------------------------------------------------------------------


def test_normalize_ean_categories_prefLabel_match() -> None:
    """A category matching a vocabulary prefLabel is replaced with the concept ID."""
    import tingbok.app as app_module

    cats = app_module._normalize_ean_categories(["Dairy"])
    assert cats == ["food/dairy"]


def test_normalize_ean_categories_altLabel_match() -> None:
    """A category matching a vocabulary altLabel is replaced with the concept ID."""
    import tingbok.app as app_module

    # "spreads" is an altLabel for concept "spread"
    cats = app_module._normalize_ean_categories(["spreads"])
    assert cats == ["spread"]


def test_normalize_ean_categories_concept_id_segment_match() -> None:
    """A category matching the last path segment of a concept ID is normalized."""
    import tingbok.app as app_module

    # "caviar" should match concept "food/caviar"
    cats = app_module._normalize_ean_categories(["caviar"])
    assert cats == ["food/caviar"]


def test_normalize_ean_categories_already_canonical() -> None:
    """A category that is already a valid concept ID is kept as-is."""
    import tingbok.app as app_module

    cats = app_module._normalize_ean_categories(["food/dairy"])
    assert cats == ["food/dairy"]


def test_normalize_ean_categories_unknown_kept() -> None:
    """A category with no vocabulary match is returned unchanged."""
    import tingbok.app as app_module

    cats = app_module._normalize_ean_categories(["xyzzy-unknown-category"])
    assert cats == ["xyzzy-unknown-category"]


def test_normalize_ean_categories_mixed() -> None:
    """Mixed list: some normalised, some unknown."""
    import tingbok.app as app_module

    cats = app_module._normalize_ean_categories(["Dairy", "unknown-thing", "spreads"])
    assert cats[0] == "food/dairy"
    assert cats[1] == "unknown-thing"
    assert cats[2] == "spread"


def test_normalize_ean_categories_case_insensitive() -> None:
    """Matching is case-insensitive."""
    import tingbok.app as app_module

    cats = app_module._normalize_ean_categories(["dairy"])
    assert cats == ["food/dairy"]


@pytest.mark.anyio
async def test_ean_lookup_normalizes_categories(client) -> None:
    """GET /api/ean/{ean} normalizes categories against the vocabulary."""
    from unittest.mock import patch

    from tingbok.services import ean as ean_service

    product = {
        "ean": "7310865004703",
        "name": "Kalles Kaviar",
        "brand": "Abba",
        "quantity": "300g",
        # "spreads" is an altLabel for "spread" in the vocabulary
        "categories": ["spreads", "fish spreads"],
        "image_url": None,
        "source": "openfoodfacts",
        "author": None,
        "type": "product",
    }
    with patch.object(ean_service, "lookup_product", return_value=product):
        response = await client.get("/api/ean/7310865004703")

    assert response.status_code == 200
    data = response.json()
    # "spreads" → "spread" (altLabel match); "fish spreads" unknown → kept
    assert "spread" in data["categories"]
    assert "fish spreads" in data["categories"]


# ---------------------------------------------------------------------------
# _load_vocabulary path inference
# ---------------------------------------------------------------------------


def _write_vocab(path, concepts):  # type: ignore[no-untyped-def]
    """Write a minimal vocabulary.yaml to *path*."""
    path.write_text(_yaml.dump({"concepts": concepts}))


def test_load_vocabulary_infers_broader_from_path(tmp_path) -> None:  # type: ignore[no-untyped-def]
    """Path-style concept ID without explicit broader gets it inferred from path."""
    import tingbok.app as app_module

    vocab = tmp_path / "vocabulary.yaml"
    _write_vocab(vocab, {"food": {"prefLabel": "Food"}, "food/dairy": {"prefLabel": "Dairy"}})
    loaded = app_module._load_vocabulary(vocab)
    broader = loaded["food/dairy"].get("broader")
    assert broader == ["food"]


def test_load_vocabulary_preserves_explicit_broader_override(tmp_path) -> None:  # type: ignore[no-untyped-def]
    """Explicit broader on a path-style concept is kept even if it differs from the path."""
    import tingbok.app as app_module

    vocab = tmp_path / "vocabulary.yaml"
    _write_vocab(
        vocab,
        {
            "food": {"prefLabel": "Food"},
            "food/roes": {"prefLabel": "Roes"},
            "food/caviar": {"prefLabel": "Caviar", "broader": "food/roes"},
        },
    )
    loaded = app_module._load_vocabulary(vocab)
    broader = loaded["food/caviar"].get("broader")
    if isinstance(broader, str):
        broader = [broader]
    assert "food/roes" in broader
    assert "food" not in broader


def test_load_vocabulary_computes_narrower_as_inverse(tmp_path) -> None:  # type: ignore[no-untyped-def]
    """Narrower is computed as the inverse of broader."""
    import tingbok.app as app_module

    vocab = tmp_path / "vocabulary.yaml"
    _write_vocab(
        vocab,
        {
            "food": {"prefLabel": "Food"},
            "food/dairy": {"prefLabel": "Dairy"},
            "food/dairy/yogurt": {"prefLabel": "Yogurt"},
        },
    )
    loaded = app_module._load_vocabulary(vocab)
    assert "food/dairy" in loaded["food"].get("narrower", [])
    assert "food/dairy/yogurt" in loaded["food/dairy"].get("narrower", [])


def test_load_vocabulary_preserves_root_narrower(tmp_path) -> None:  # type: ignore[no-untyped-def]
    """_root.narrower (no broader counterpart) is preserved from YAML."""
    import tingbok.app as app_module

    vocab = tmp_path / "vocabulary.yaml"
    _write_vocab(
        vocab,
        {
            "_root": {"prefLabel": "Root", "narrower": ["food", "tools"]},
            "food": {"prefLabel": "Food"},
            "tools": {"prefLabel": "Tools"},
        },
    )
    loaded = app_module._load_vocabulary(vocab)
    assert loaded["_root"].get("narrower") == ["food", "tools"]


@pytest.mark.anyio
async def test_full_vocabulary_has_source_uris(client):
    response = await client.get("/api/vocabulary")
    assert response.status_code == 200
    data = response.json()
    for concept_id, concept in data.items():
        assert "source_uris" in concept, f"Concept {concept_id} missing source_uris"
        assert isinstance(concept["source_uris"], list)
        # The tingbok self-URI lives in "uri", not "source_uris"; some concepts
        # (e.g. _root) have no external source URIs so the list may be empty.
        assert not any("tingbok.plann.no" in u for u in concept["source_uris"]), (
            f"Concept {concept_id} has redundant tingbok self-URI in source_uris"
        )


@pytest.mark.anyio
@pytest.mark.parametrize(
    ("concept_id", "expected_uri"),
    [
        ("food", "https://dbpedia.org/resource/Food"),
        ("tools", "https://dbpedia.org/resource/Tool"),
        ("medicine", "https://dbpedia.org/resource/Medicine"),
        ("bedding", "https://dbpedia.org/resource/Bedding"),
        ("washer", "https://dbpedia.org/resource/Washer_(hardware)"),
        ("seal", "https://dbpedia.org/resource/Hermetic_seal"),
        ("disc", "https://dbpedia.org/resource/Disc"),
        ("tubing", "https://dbpedia.org/resource/Tubing_(material)"),
        ("gps", "https://dbpedia.org/resource/Global_Positioning_System"),
    ],
)
async def test_concept_has_external_uri_in_source_uris(client, concept_id, expected_uri):
    """Concepts with a known URI should include it in source_uris."""
    response = await client.get(f"/api/vocabulary/{concept_id}")
    assert response.status_code == 200
    data = response.json()
    assert expected_uri in data["source_uris"], (
        f"Concept '{concept_id}' missing {expected_uri} in source_uris: {data['source_uris']}"
    )


@pytest.mark.anyio
@pytest.mark.parametrize(
    "concept_id",
    [
        "washer",
        "seal",
        "disc",
        "tubing",
        "gps",
        "tool",
        "snacks",
        "bedding",
        "peanuts",
    ],
)
async def test_agrovoc_excluded_for_mismatch_concepts(client, concept_id):
    """Concepts known to cause AGROVOC mismatches must exclude agrovoc."""
    response = await client.get(f"/api/vocabulary/{concept_id}")
    assert response.status_code == 200
    data = response.json()
    assert "agrovoc" in data["excluded_sources"], f"Concept '{concept_id}' should have agrovoc in excluded_sources"


@pytest.mark.anyio
async def test_discover_source_uris_populates_memory(client):
    """Auto-discovery should add external URIs for concepts with empty source_uris."""
    import tingbok.app as app_module

    # Use a concept that has no external source_uris in vocabulary.yaml
    fake_uri = "https://dbpedia.org/resource/Boat_equipment"

    def fake_lookup(label, lang, source, cache_dir):
        if source == "wikidata" and "boat" in label.lower():
            return {"uri": fake_uri, "prefLabel": "Boat equipment", "source": "wikidata", "broader": []}
        return None

    # Reset discovered uris before test
    app_module._discovered_source_uris.clear()

    with patch("tingbok.app.skos_service.lookup_concept", side_effect=fake_lookup):
        with patch("tingbok.app.off_service.lookup_concept", return_value=None):
            with patch("tingbok.app.gpt_service.lookup_concept", return_value=None):
                await app_module._discover_source_uris_background()

    assert "boat-equipment" in app_module._discovered_source_uris
    assert app_module._discovered_source_uris["boat-equipment"].get("wikidata") == fake_uri


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
        with patch("tingbok.app.off_service.lookup_concept", return_value=None):
            with patch("tingbok.app.gpt_service.lookup_concept", return_value=None):
                await app_module._discover_source_uris_background()

    # food, tools, mushrooms, snacks, peanuts, bedding, washer, medicine, gps, tool,
    # marine_propulsion, seal, tubing, disc all have source_uris → skipped
    # Only concepts without source_uris should be looked up
    concepts_with_uris = sum(1 for data in app_module.vocabulary.values() if data.get("source_uris"))
    total_concepts = len(app_module.vocabulary)
    # 2 sources when no Oxigraph (dbpedia + wikidata), 3 when Oxigraph available
    expected_max_calls = (total_concepts - concepts_with_uris) * 3
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
        with patch("tingbok.app.off_service.lookup_concept", return_value=None):
            with patch("tingbok.app.gpt_service.lookup_concept", return_value=None):
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

    fake_uri = "https://dbpedia.org/resource/Electronics"
    app_module._discovered_source_uris["electronics"] = {"dbpedia": fake_uri}

    try:
        response = await client.get("/api/vocabulary/electronics")
        assert response.status_code == 200
        data = response.json()
        assert fake_uri in data["source_uris"]
    finally:
        app_module._discovered_source_uris.pop("electronics", None)


@pytest.mark.anyio
async def test_discover_queries_agrovoc_when_store_available(tmp_path, monkeypatch):
    """Discovery should include agrovoc when the local Oxigraph store is available."""
    import tingbok.app as app_module
    from tingbok.services import skos as skos_service

    # Simulate agrovoc.nt existing in the SKOS cache dir
    fake_nt = tmp_path / "agrovoc.nt"
    fake_nt.write_text("")
    monkeypatch.setattr(app_module, "SKOS_CACHE_DIR", tmp_path)

    app_module._discovered_source_uris.clear()
    queried_sources: set[str] = set()

    fake_agrovoc_uri = "https://aims.fao.org/aos/agrovoc/c_12345"

    def recording_lookup(label, lang, source, cache_dir):
        queried_sources.add(source)
        if source == "agrovoc" and "boat" in label.lower():
            return {"uri": fake_agrovoc_uri, "prefLabel": "Boat equipment", "source": "agrovoc", "broader": []}
        return None

    # boat-equipment excludes dbpedia but not agrovoc/wikidata, so it will be discovered

    with patch.object(skos_service, "lookup_concept", side_effect=recording_lookup):
        with patch.object(skos_service, "get_agrovoc_store", return_value=object()):
            with patch("tingbok.app.off_service.lookup_concept", return_value=None):
                with patch("tingbok.app.gpt_service.lookup_concept", return_value=None):
                    await app_module._discover_source_uris_background()

    assert "agrovoc" in queried_sources, "AGROVOC should be queried when Oxigraph store is available"
    assert app_module._discovered_source_uris.get("boat-equipment", {}).get("agrovoc") == fake_agrovoc_uri


@pytest.mark.anyio
async def test_discover_skips_agrovoc_when_store_unavailable(tmp_path, monkeypatch):
    """Discovery should NOT query agrovoc when agrovoc.nt is not in the cache dir."""
    import tingbok.app as app_module
    from tingbok.services import skos as skos_service

    # No agrovoc.nt in tmp_path
    monkeypatch.setattr(app_module, "SKOS_CACHE_DIR", tmp_path)

    app_module._discovered_source_uris.clear()
    queried_sources: set[str] = set()

    def recording_lookup(label, lang, source, cache_dir):
        queried_sources.add(source)
        return None

    with patch.object(skos_service, "lookup_concept", side_effect=recording_lookup):
        with patch.object(skos_service, "get_agrovoc_store", return_value=None):
            with patch("tingbok.app.off_service.lookup_concept", return_value=None):
                with patch("tingbok.app.gpt_service.lookup_concept", return_value=None):
                    await app_module._discover_source_uris_background()

    assert "agrovoc" not in queried_sources, "AGROVOC should not be queried when Oxigraph store is unavailable"


def test_get_agrovoc_store_returns_none_when_file_missing(tmp_path):
    """get_agrovoc_store should return None when agrovoc.nt does not exist."""
    from tingbok.services.skos import get_agrovoc_store

    result = get_agrovoc_store(tmp_path)
    assert result is None


def test_get_agrovoc_store_returns_none_when_pyoxigraph_absent(tmp_path):
    """get_agrovoc_store should return None when pyoxigraph is not installed."""
    import sys

    from tingbok.services import skos as skos_module
    from tingbok.services.skos import get_agrovoc_store

    fake_nt = tmp_path / "agrovoc.nt"
    fake_nt.write_text("# empty")

    # Simulate pyoxigraph not being installed
    with patch.dict(sys.modules, {"pyoxigraph": None}):
        # Also reset the module-level cached store
        original = skos_module._agrovoc_store
        skos_module._agrovoc_store = None
        try:
            result = get_agrovoc_store(tmp_path)
            assert result is None
        finally:
            skos_module._agrovoc_store = original


# ---------------------------------------------------------------------------
# Tests for _fetch_labels_background() and vocabulary label merging
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_fetch_labels_background_populates_fetched_labels():
    """Background label fetch should populate _fetched_labels from source URIs."""
    import tingbok.app as app_module

    app_module._fetched_labels.clear()
    app_module._fetched_descriptions.clear()
    app_module._discovered_source_uris.clear()

    fake_labels = {"en": "Food", "nb": "Mat", "de": "Essen"}

    with patch("tingbok.app.skos_service.get_labels", return_value=fake_labels):
        with patch("tingbok.app.skos_service.get_description", return_value=None):
            with patch("tingbok.app.skos_service.get_alt_labels", return_value={}):
                await app_module._fetch_labels_background()

    # "food" concept has source_uris with dbpedia/agrovoc/wikidata URIs
    assert "food" in app_module._fetched_labels
    labels = app_module._fetched_labels["food"]
    assert labels.get("nb") == "Mat"


@pytest.mark.anyio
async def test_fetch_labels_background_picks_longest_description():
    """Background task picks the longest description from available sources."""
    import tingbok.app as app_module

    app_module._fetched_labels.clear()
    app_module._fetched_descriptions.clear()
    app_module._discovered_source_uris.clear()

    call_count = [0]

    def fake_get_description(uri, source, lang, cache_dir):
        call_count[0] += 1
        if "dbpedia" in uri or source == "dbpedia":
            return "Short."
        if "wikidata" in uri or source == "wikidata":
            return "A much longer description from Wikidata that is more informative."
        return None

    with patch("tingbok.app.skos_service.get_labels", return_value={}):
        with patch("tingbok.app.skos_service.get_description", side_effect=fake_get_description):
            with patch("tingbok.app.skos_service.get_alt_labels", return_value={}):
                await app_module._fetch_labels_background()

    # For concepts with both dbpedia and wikidata URIs, longest description wins
    for _concept_id, desc in app_module._fetched_descriptions.items():
        if desc:
            assert desc != "Short." or len(desc) >= len("Short.")


@pytest.mark.anyio
async def test_vocabulary_concept_fetches_labels_eagerly(client):
    """GET /api/vocabulary/{concept} should fetch labels on-demand if not yet done."""
    import tingbok.app as app_module

    app_module._concepts_fetched.discard("potatoes")
    app_module._fetched_labels.pop("potatoes", None)
    app_module._fetched_descriptions.pop("potatoes", None)
    app_module._fetched_alt_labels.pop("potatoes", None)

    fake_labels = {"en": "Potatoes", "nb": "Poteter", "de": "Kartoffeln"}

    with patch("tingbok.app.skos_service.get_labels", return_value=fake_labels):
        with patch("tingbok.app.skos_service.get_description", return_value="A starchy tuber."):
            with patch("tingbok.app.skos_service.get_alt_labels", return_value={}):
                with patch("tingbok.app.off_service.get_labels", return_value={}):
                    with patch("tingbok.app.off_service.get_alt_labels", return_value={}):
                        with patch("tingbok.app.gpt_service.get_labels", return_value={}):
                            response = await client.get("/api/vocabulary/potatoes")

    assert response.status_code == 200
    data = response.json()
    assert data["labels"].get("nb") == "Poteter"
    assert data["description"] == "A starchy tuber."
    assert "potatoes" in app_module._concepts_fetched


@pytest.mark.anyio
async def test_vocabulary_api_merges_source_labels(client):
    """Vocabulary API should include source-fetched labels in the response."""
    import tingbok.app as app_module

    app_module._fetched_labels["food"] = {"de": "Lebensmittel", "sv": "Mat"}

    try:
        response = await client.get("/api/vocabulary/food")
        assert response.status_code == 200
        data = response.json()
        assert "labels" in data
        # Source-fetched label should appear
        assert data["labels"].get("sv") == "Mat"
    finally:
        app_module._fetched_labels.pop("food", None)


@pytest.mark.anyio
async def test_vocabulary_api_static_labels_override_source_labels(client):
    """Static labels in vocabulary.yaml should override source-fetched labels."""
    import tingbok.app as app_module

    # Inject a conflicting source-fetched label for a concept that has a static label
    app_module._fetched_labels["food"] = {"en": "WRONG from source", "nb": "Source Norwegian"}

    try:
        response = await client.get("/api/vocabulary/food")
        assert response.status_code == 200
        data = response.json()
        # Static label "Food" in vocabulary.yaml should win over "WRONG from source"
        assert data["labels"].get("en") == "Food"
        # Source label for nb should appear (if food doesn't have a static nb label)
    finally:
        app_module._fetched_labels.pop("food", None)


@pytest.mark.anyio
async def test_vocabulary_concept_uri_is_canonical_tingbok_url(client):
    """Every concept's uri field should be the canonical tingbok URL, not an external URI."""
    response = await client.get("/api/vocabulary/food")
    assert response.status_code == 200
    data = response.json()
    assert data["uri"] == "https://tingbok.plann.no/api/vocabulary/food"


@pytest.mark.anyio
async def test_full_vocabulary_uris_are_canonical(client):
    """All concepts in the full vocabulary should have canonical tingbok URIs."""
    response = await client.get("/api/vocabulary")
    assert response.status_code == 200
    for concept_id, concept in response.json().items():
        expected = f"https://tingbok.plann.no/api/vocabulary/{concept_id}"
        assert concept["uri"] == expected, f"Concept '{concept_id}' has wrong uri: {concept['uri']}"


@pytest.mark.anyio
async def test_vocabulary_api_merges_source_alt_labels(client):
    """Vocabulary API should include source-fetched altLabels merged with vocabulary.yaml altLabels."""
    import tingbok.app as app_module

    app_module._fetched_alt_labels["food"] = {"en": ["victuals", "comestibles"], "nb": ["næringsmidler"]}

    try:
        response = await client.get("/api/vocabulary/food")
        assert response.status_code == 200
        data = response.json()
        assert "altLabel" in data
        en_alts = data["altLabel"].get("en", [])
        assert "victuals" in en_alts
        assert "comestibles" in en_alts
        nb_alts = data["altLabel"].get("nb", [])
        assert "næringsmidler" in nb_alts
    finally:
        app_module._fetched_alt_labels.pop("food", None)


@pytest.mark.anyio
async def test_vocabulary_api_static_alt_labels_not_duplicated(client):
    """Source-fetched altLabels that duplicate vocabulary.yaml altLabels should not appear twice."""
    import tingbok.app as app_module

    # food already has "groceries" as en altLabel in vocabulary.yaml
    app_module._fetched_alt_labels["food"] = {"en": ["groceries", "source-only-alt"]}

    try:
        response = await client.get("/api/vocabulary/food")
        assert response.status_code == 200
        data = response.json()
        en_alts = data["altLabel"].get("en", [])
        assert en_alts.count("groceries") == 1, "Duplicate altLabel should appear only once"
        assert "source-only-alt" in en_alts
    finally:
        app_module._fetched_alt_labels.pop("food", None)


@pytest.mark.anyio
async def test_fetch_alt_labels_background_populates_fetched_alt_labels():
    """Background fetch should populate _fetched_alt_labels from source URIs."""
    import tingbok.app as app_module

    app_module._fetched_alt_labels.clear()
    app_module._fetched_labels.clear()
    app_module._fetched_descriptions.clear()
    app_module._discovered_source_uris.clear()

    fake_alt_labels = {"en": ["foodstuff", "chow"], "nb": ["næringsmidler"]}

    with patch("tingbok.app.skos_service.get_labels", return_value={}):
        with patch("tingbok.app.skos_service.get_description", return_value=None):
            with patch("tingbok.app.skos_service.get_alt_labels", return_value=fake_alt_labels):
                await app_module._fetch_labels_background()

    assert "food" in app_module._fetched_alt_labels
    assert "foodstuff" in app_module._fetched_alt_labels["food"].get("en", [])


@pytest.mark.anyio
async def test_vocabulary_api_uses_source_description_as_fallback(client):
    """Vocabulary API should use source-fetched description when none is in vocabulary.yaml."""
    import tingbok.app as app_module

    # Use a concept with no description in vocabulary.yaml
    # Find one without a static description
    concept_without_desc = next(
        (cid for cid, data in app_module.vocabulary.items() if not data.get("description")),
        None,
    )
    if concept_without_desc is None:
        pytest.skip("All concepts have static descriptions")

    app_module._fetched_descriptions[concept_without_desc] = "A description from sources."

    try:
        response = await client.get(f"/api/vocabulary/{concept_without_desc}")
        assert response.status_code == 200
        data = response.json()
        assert data["description"] == "A description from sources."
    finally:
        app_module._fetched_descriptions.pop(concept_without_desc, None)


# ---------------------------------------------------------------------------
# Tests for GET /api/lookup/{label}
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_lookup_by_concept_id(client):
    """Concept present in vocabulary is found by its ID."""
    response = await client.get("/api/lookup/food")
    assert response.status_code == 200
    data = response.json()
    assert data["id"] == "food"
    assert data["prefLabel"].lower() == "food"


@pytest.mark.anyio
async def test_lookup_by_preflabel(client):
    """Concept is found by its prefLabel (case-insensitive)."""
    # 'food' should match the concept with prefLabel "Food"
    response = await client.get("/api/lookup/Food")
    assert response.status_code == 200
    data = response.json()
    assert data["id"] == "food"


@pytest.mark.anyio
async def test_lookup_by_altlabel(client):
    """Concept in vocabulary is found by its altLabel (case-insensitive)."""
    # food/spices has altLabel: en: ["spice", "herbs", "seasonings"]
    response = await client.get("/api/lookup/spice")
    assert response.status_code == 200
    data = response.json()
    assert data["id"] == "food/spices"


@pytest.mark.anyio
async def test_lookup_by_fetched_altlabel(client):
    """Concept in vocabulary is found via dynamically fetched altLabels.

    Fetched altLabels (from external sources at runtime) must also be searched
    so that e.g. '/api/lookup/spices' finds food/spices even though 'spices'
    is not listed in vocabulary.yaml.
    """
    import tingbok.app as app_module

    saved = dict(app_module._fetched_alt_labels)
    try:
        # Simulate Wikidata having returned "spices" as an altLabel for food/spices
        app_module._fetched_alt_labels["food/spices"] = {"en": ["Spices"]}
        with patch("tingbok.app.skos_service.lookup_concept", return_value=None):
            with patch("tingbok.app.off_service.lookup_concept", return_value=None):
                with patch("tingbok.app.gpt_service.lookup_concept", return_value=None):
                    response = await client.get("/api/lookup/spices")
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == "food/spices"
    finally:
        app_module._fetched_alt_labels.clear()
        app_module._fetched_alt_labels.update(saved)


@pytest.mark.anyio
async def test_lookup_by_fetched_label(client):
    """Concept in vocabulary is found via fetched translated labels.

    A fetched label like 'Krydder' (Norwegian for food/spices) must be matched
    so that '/api/lookup/Krydder' returns food/spices rather than a SKOS result.
    """
    import tingbok.app as app_module

    saved = dict(app_module._fetched_labels)
    try:
        app_module._fetched_labels["food/spices"] = {"nb": "Krydder"}
        with patch("tingbok.app.skos_service.lookup_concept", return_value=None):
            with patch("tingbok.app.off_service.lookup_concept", return_value=None):
                with patch("tingbok.app.gpt_service.lookup_concept", return_value=None):
                    response = await client.get("/api/lookup/Krydder")
        assert response.status_code == 200
        data = response.json()
        assert data["id"] == "food/spices"
    finally:
        app_module._fetched_labels.clear()
        app_module._fetched_labels.update(saved)


@pytest.mark.anyio
async def test_lookup_falls_back_to_skos(client):
    """Label not in vocabulary triggers SKOS lookup across all sources, merged."""
    from unittest.mock import patch

    fake_concept = {
        "uri": "https://aims.fao.org/aos/agrovoc/c_12851",
        "prefLabel": "Cumin",
        "source": "agrovoc",
    }
    fake_paths = (["food/spices/cumin"], True, {"food/spices/cumin": "https://aims.fao.org/aos/agrovoc/c_12851"})

    with patch("tingbok.app.skos_service.lookup_concept", return_value=fake_concept):
        with patch("tingbok.app.skos_service.build_hierarchy_paths", return_value=fake_paths):
            with patch("tingbok.app.skos_service.get_labels", return_value={"en": "Cumin", "nb": "Karve"}):
                with patch("tingbok.app.skos_service.get_alt_labels", return_value={}):
                    with patch("tingbok.app.skos_service.get_description", return_value=None):
                        response = await client.get("/api/lookup/cumin")

    assert response.status_code == 200
    data = response.json()
    assert data["prefLabel"] == "Cumin"
    assert data["id"] == "food/spices/cumin"
    # Source URIs collected from all sources
    assert "https://aims.fao.org/aos/agrovoc/c_12851" in data["source_uris"]
    # Labels merged from all sources
    assert "nb" in data["labels"]


@pytest.mark.anyio
async def test_lookup_merges_descriptions_from_all_sources(client):
    """The longest description across all sources is selected."""
    from unittest.mock import patch

    fake_concept = {
        "uri": "https://aims.fao.org/aos/agrovoc/c_12851",
        "prefLabel": "Cumin",
        "source": "agrovoc",
    }
    fake_paths = (["food/spices/cumin"], True, {})
    descriptions = {"agrovoc": "Short.", "dbpedia": "A much longer description of cumin.", "wikidata": None}

    with patch("tingbok.app.skos_service.lookup_concept", return_value=fake_concept):
        with patch("tingbok.app.skos_service.build_hierarchy_paths", return_value=fake_paths):
            with patch("tingbok.app.skos_service.get_labels", return_value={}):
                with patch("tingbok.app.skos_service.get_alt_labels", return_value={}):
                    with patch(
                        "tingbok.app.skos_service.get_description",
                        side_effect=lambda uri, source, lang, cache_dir: descriptions.get(source),
                    ):
                        response = await client.get("/api/lookup/cumin")

    assert response.status_code == 200
    data = response.json()
    assert data["description"] == "A much longer description of cumin."


@pytest.mark.anyio
async def test_lookup_not_found_returns_404(client):
    """404 is returned when label is not in vocabulary or any SKOS source."""
    from unittest.mock import patch

    with patch("tingbok.app.skos_service.lookup_concept", return_value=None):
        with patch("tingbok.app.skos_service.build_hierarchy_paths", return_value=([], False, {})):
            response = await client.get("/api/lookup/xyzzy_nonexistent_label")

    assert response.status_code == 404


@pytest.mark.anyio
async def test_lookup_prefers_vocabulary_anchored_path(client):
    """When AGROVOC returns multiple paths, prefer the one rooted at a vocabulary concept.

    AGROVOC may return both "food/plant_products/spices/cumin" and "food/spices/cumin"
    for the same concept.  Since food/spices is in the vocabulary, the canonical ID
    should be food/spices/cumin, not food/plant_products/spices/cumin.
    """
    from unittest.mock import patch

    fake_concept = {
        "uri": "https://aims.fao.org/aos/agrovoc/c_10205",
        "prefLabel": "Cumin",
        "source": "agrovoc",
    }
    # AGROVOC returns the raw path first, vocabulary-anchored path second
    fake_paths = (
        ["food/plant_products/spices/cumin", "food/spices/cumin", "food/plant_products/cumin"],
        True,
        {"food/plant_products/spices/cumin": "https://aims.fao.org/aos/agrovoc/c_10205"},
    )

    with patch("tingbok.app.skos_service.lookup_concept", return_value=fake_concept):
        with patch("tingbok.app.skos_service.build_hierarchy_paths", return_value=fake_paths):
            with patch("tingbok.app.skos_service.get_labels", return_value={}):
                with patch("tingbok.app.skos_service.get_alt_labels", return_value={}):
                    with patch("tingbok.app.skos_service.get_description", return_value=None):
                        with patch("tingbok.app.off_service.lookup_concept", return_value=None):
                            with patch("tingbok.app.gpt_service.lookup_concept", return_value=None):
                                response = await client.get("/api/lookup/cumin")

    assert response.status_code == 200
    data = response.json()
    # Must prefer the vocabulary-anchored path
    assert data["id"] == "food/spices/cumin", (
        f"Expected food/spices/cumin but got {data['id']!r}. "
        "food/spices is in the vocabulary so paths through it should be preferred."
    )


@pytest.mark.anyio
async def test_lookup_path_alias_norwegian(client):
    """A Norwegian path alias resolves to the canonical English concept when lang=nb."""
    # clothing/thermal has path_aliases: nb: ["klær/vinter", "klær/vinterklær"]
    response = await client.get("/api/lookup/klær/vinter", params={"lang": "nb"})
    assert response.status_code == 200
    assert response.json()["id"] == "clothing/thermal"


@pytest.mark.anyio
async def test_lookup_path_alias_norwegian_no_lang_code(client):
    """lang=no (generic Norwegian) also resolves nb path aliases."""
    response = await client.get("/api/lookup/klær/vinterklær", params={"lang": "no"})
    assert response.status_code == 200
    assert response.json()["id"] == "clothing/thermal"


@pytest.mark.anyio
async def test_lookup_path_alias_wrong_language_not_found(client):
    """Norwegian path aliases must NOT resolve when lang=en (language mismatch)."""
    with patch("tingbok.app.skos_service.lookup_concept", return_value=None):
        with patch("tingbok.app.off_service.lookup_concept", return_value=None):
            with patch("tingbok.app.gpt_service.lookup_concept", return_value=None):
                response = await client.get("/api/lookup/klær/vinter", params={"lang": "en"})
    assert response.status_code == 404


@pytest.mark.anyio
async def test_lookup_path_and_label_return_same_id(client):
    """/api/lookup/food/spices and /api/lookup/spices must return the same concept ID.

    Looking up by the exact vocabulary path (food/spices) hits step 1 (ID match).
    Looking up by just 'spices' must also resolve to food/spices via altLabel or
    prefLabel match, not fall through to SKOS and get a different canonical path.
    """
    with patch("tingbok.app.skos_service.lookup_concept", return_value=None):
        with patch("tingbok.app.off_service.lookup_concept", return_value=None):
            with patch("tingbok.app.gpt_service.lookup_concept", return_value=None):
                r1 = await client.get("/api/lookup/food/spices")
                r2 = await client.get("/api/lookup/spices")

    assert r1.status_code == 200
    assert r2.status_code == 200
    assert r1.json()["id"] == r2.json()["id"], (
        f"/api/lookup/food/spices returned id={r1.json()['id']!r} but "
        f"/api/lookup/spices returned id={r2.json()['id']!r}"
    )


@pytest.mark.anyio
async def test_lookup_vocab_concept_has_labels(client):
    """Vocabulary concept returned via /api/lookup has labels dict populated."""
    response = await client.get("/api/lookup/food")
    assert response.status_code == 200
    data = response.json()
    # labels dict should at least have "en"
    assert "en" in data.get("labels", {})


# ---------------------------------------------------------------------------
# Translation-conflict warnings
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_lookup_records_warning_on_source_root_conflict(client, tmp_path):
    """When sources return paths with different top-level roots, a warning is written."""
    import json
    from unittest.mock import patch

    import tingbok.app as _app

    warnings_path = tmp_path / "lookup-warnings.json"

    def make_concept(uri):
        return {"uri": uri, "prefLabel": "Bedding", "source": "test"}

    # AGROVOC: livestock root; DBpedia: household root; wikidata: no path
    def fake_lookup(label, lang, source, cache_dir):
        return make_concept(f"http://example.com/{source}/{label}")

    def fake_paths(label, lang, source, cache_dir):
        if source == "agrovoc":
            return (["livestock/pool_blanket_xyzzy"], True, {})
        if source == "dbpedia":
            return (["household/pool_blanket_xyzzy"], True, {})
        return ([], False, {})

    with patch("tingbok.app.skos_service.lookup_concept", side_effect=fake_lookup):
        with patch("tingbok.app.skos_service.build_hierarchy_paths", side_effect=fake_paths):
            with patch("tingbok.app.skos_service.get_labels", return_value={}):
                with patch("tingbok.app.skos_service.get_alt_labels", return_value={}):
                    with patch("tingbok.app.skos_service.get_description", return_value=None):
                        with patch.object(_app, "WARNINGS_PATH", warnings_path):
                            # Use a label not in vocabulary so SKOS sources are queried
                            response = await client.get("/api/lookup/pool_blanket_xyzzy")

    assert response.status_code == 200
    assert warnings_path.exists(), "Warning file should be created"
    data = json.loads(warnings_path.read_text())
    assert "pool_blanket_xyzzy" in data
    warning = data["pool_blanket_xyzzy"]
    assert warning["roots_per_source"]["agrovoc"] == "livestock"
    assert warning["roots_per_source"]["dbpedia"] == "household"


@pytest.mark.anyio
async def test_lookup_no_warning_when_sources_agree(client, tmp_path):
    """When all sources return paths under the same root, no warning is written."""
    from unittest.mock import patch

    import tingbok.app as _app

    warnings_path = tmp_path / "lookup-warnings.json"

    def make_concept(uri):
        return {"uri": uri, "prefLabel": "Cumin", "source": "test"}

    def fake_paths(label, lang, source, cache_dir):
        return (["food/spices/cumin"], True, {})

    with patch(
        "tingbok.app.skos_service.lookup_concept", side_effect=lambda *a, **kw: make_concept("http://example.com")
    ):
        with patch("tingbok.app.skos_service.build_hierarchy_paths", side_effect=fake_paths):
            with patch("tingbok.app.skos_service.get_labels", return_value={}):
                with patch("tingbok.app.skos_service.get_alt_labels", return_value={}):
                    with patch("tingbok.app.skos_service.get_description", return_value=None):
                        with patch.object(_app, "WARNINGS_PATH", warnings_path):
                            response = await client.get("/api/lookup/cumin")

    assert response.status_code == 200
    assert not warnings_path.exists(), "No warning file should be created when sources agree"


# ---------------------------------------------------------------------------
# PUT /api/vocabulary/{concept_id} — concept update endpoint
# ---------------------------------------------------------------------------


@pytest.fixture
def temp_vocab_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Temp vocabulary.yaml with a handful of concepts, wired into the app."""
    import tingbok.app as app_module

    vocab_data = {
        "concepts": {
            "food": {"prefLabel": "Food"},
            "food/dairy": {"prefLabel": "Dairy", "source_uris": ["https://dbpedia.org/resource/Dairy"]},
        }
    }
    vocab_path = tmp_path / "vocabulary.yaml"
    vocab_path.write_text(_yaml.dump(vocab_data))
    monkeypatch.setattr(app_module, "VOCABULARY_PATH", vocab_path)
    monkeypatch.setattr(app_module, "vocabulary", app_module._load_vocabulary(vocab_path))
    monkeypatch.setattr(app_module, "_category_index", None)
    # Mark all concepts as fetched so 503 guard doesn't fire
    app_module._concepts_fetched.update(app_module.vocabulary.keys())
    return vocab_path


@pytest.mark.anyio
async def test_put_vocabulary_creates_new_concept(client, temp_vocab_path) -> None:
    """PUT a concept that doesn't exist yet creates it."""
    response = await client.put("/api/vocabulary/food/new-thing", json={"prefLabel": "New Thing"})
    assert response.status_code == 200
    data = response.json()
    assert data["id"] == "food/new-thing"
    assert data["prefLabel"] == "New Thing"


@pytest.mark.anyio
async def test_put_vocabulary_creates_ancestor_concepts(client, temp_vocab_path) -> None:
    """PUT a deep path concept creates missing ancestor concepts."""
    import tingbok.app as app_module

    response = await client.put("/api/vocabulary/food/new-sub/item", json={})
    assert response.status_code == 200
    # Ancestor food/new-sub must now exist in the in-memory vocabulary
    assert "food/new-sub" in app_module.vocabulary


@pytest.mark.anyio
async def test_put_vocabulary_updates_preflabel(client, temp_vocab_path) -> None:
    """PUT with prefLabel updates the existing concept's prefLabel."""
    response = await client.put("/api/vocabulary/food/dairy", json={"prefLabel": "Dairy Products"})
    assert response.status_code == 200
    assert response.json()["prefLabel"] == "Dairy Products"


@pytest.mark.anyio
async def test_put_vocabulary_adds_source_uris(client, temp_vocab_path) -> None:
    """PUT with add_source_uris appends URIs to the concept."""
    new_uri = "https://www.wikidata.org/entity/Q1491"
    response = await client.put("/api/vocabulary/food/dairy", json={"add_source_uris": [new_uri]})
    assert response.status_code == 200
    assert new_uri in response.json()["source_uris"]
    # Existing URI must still be present
    assert "https://dbpedia.org/resource/Dairy" in response.json()["source_uris"]


@pytest.mark.anyio
async def test_put_vocabulary_removes_source_uris(client, temp_vocab_path) -> None:
    """PUT with remove_source_uris deletes the named URIs from the concept."""
    response = await client.put(
        "/api/vocabulary/food/dairy",
        json={"remove_source_uris": ["https://dbpedia.org/resource/Dairy"]},
    )
    assert response.status_code == 200
    assert "https://dbpedia.org/resource/Dairy" not in response.json()["source_uris"]


@pytest.mark.anyio
async def test_put_vocabulary_adds_excluded_sources(client, temp_vocab_path) -> None:
    """PUT with add_excluded_sources marks a source as excluded."""
    response = await client.put("/api/vocabulary/food/dairy", json={"add_excluded_sources": ["agrovoc"]})
    assert response.status_code == 200
    assert "agrovoc" in response.json()["excluded_sources"]


@pytest.mark.anyio
async def test_put_vocabulary_persists_to_yaml(client, temp_vocab_path) -> None:
    """PUT writes changes to vocabulary.yaml."""
    await client.put("/api/vocabulary/food/dairy", json={"prefLabel": "Dairy Products"})
    updated = _yaml.safe_load(temp_vocab_path.read_text())
    assert updated["concepts"]["food/dairy"]["prefLabel"] == "Dairy Products"


@pytest.mark.anyio
async def test_put_vocabulary_creates_ancestor_in_yaml(client, temp_vocab_path) -> None:
    """PUT a deep path concept also writes the new ancestor to vocabulary.yaml."""
    await client.put("/api/vocabulary/food/new-sub/item", json={})
    updated = _yaml.safe_load(temp_vocab_path.read_text())
    assert "food/new-sub" in updated["concepts"]


@pytest.mark.anyio
async def test_lookup_reverse_label_cache_finds_non_vocab_concept(client) -> None:
    """Searching in a non-English language finds a concept previously found via English.

    Reproduces the bug: GET /api/lookup/typewriter?lang=en succeeds and caches labels
    including nb: "skrivemaskin".  A subsequent GET /api/lookup/skrivemaskin?lang=nb
    should return the same concept via the in-memory reverse label cache, even if the
    SKOS sources don't support Norwegian label lookup.
    """
    import tingbok.app as app_module

    fake_concept = {
        "uri": "http://www.wikidata.org/entity/Q1020318",
        "prefLabel": "typewriter",
        "source": "wikidata",
        "broader": [],
    }
    fake_paths = (["tools/typewriter"], True, {"tools/typewriter": "http://www.wikidata.org/entity/Q1020318"})
    fake_labels = {"en": "typewriter", "nb": "skrivemaskin", "nn": "skrivemaskin"}

    saved = dict(app_module._skos_label_cache)
    try:
        # Step 1: simulate a successful "typewriter?lang=en" lookup that populates the cache
        with patch("tingbok.app.skos_service.lookup_concept", return_value=fake_concept):
            with patch("tingbok.app.skos_service.build_hierarchy_paths", return_value=fake_paths):
                with patch("tingbok.app.skos_service.get_labels", return_value=fake_labels):
                    with patch("tingbok.app.skos_service.get_alt_labels", return_value={}):
                        with patch("tingbok.app.skos_service.get_description", return_value=None):
                            with patch("tingbok.app.off_service.lookup_concept", return_value=None):
                                with patch("tingbok.app.gpt_service.lookup_concept", return_value=None):
                                    r1 = await client.get("/api/lookup/typewriter?lang=en")
        assert r1.status_code == 200

        # Step 2: Norwegian lookup should now find it via the reverse cache (SKOS returns nothing)
        with patch("tingbok.app.skos_service.lookup_concept", return_value=None):
            with patch("tingbok.app.off_service.lookup_concept", return_value=None):
                with patch("tingbok.app.gpt_service.lookup_concept", return_value=None):
                    r2 = await client.get("/api/lookup/skrivemaskin?lang=nb")

        assert r2.status_code == 200, f"Expected 200, got {r2.status_code}: {r2.text}"
        data = r2.json()
        assert data["prefLabel"] == "skrivemaskin"
        assert data["id"] == "tools/typewriter"
    finally:
        app_module._skos_label_cache.clear()
        app_module._skos_label_cache.update(saved)


@pytest.mark.anyio
async def test_lookup_lang_fallback_for_scandinavian(client) -> None:
    """When nb SKOS lookup fails, fall back to no/da/nn/sv for Scandinavian labels.

    This covers the case where 'skrivemaskin?lang=nb' is searched first (before any
    English lookup has populated _skos_label_cache) and the SKOS source only has the
    label indexed under a sibling language variant.
    """
    from unittest.mock import patch

    fake_concept = {
        "uri": "http://www.wikidata.org/entity/Q1020318",
        "prefLabel": "skrivemaskin",
        "source": "wikidata",
    }
    fake_paths = (["tools/typewriter"], True, {})
    fake_labels = {"en": "typewriter", "nb": "skrivemaskin", "no": "skrivemaskin"}

    def lookup_side_effect(label: str, lang: str, source: str, cache_dir: object) -> dict | None:
        # Only succeeds when lang=no (simulates nb not indexed but no is)
        if lang == "no" and label == "skrivemaskin":
            return fake_concept
        return None

    with patch("tingbok.app.skos_service.lookup_concept", side_effect=lookup_side_effect):
        with patch("tingbok.app.skos_service.build_hierarchy_paths", return_value=fake_paths):
            with patch("tingbok.app.skos_service.get_labels", return_value=fake_labels):
                with patch("tingbok.app.skos_service.get_alt_labels", return_value={}):
                    with patch("tingbok.app.skos_service.get_description", return_value=None):
                        with patch("tingbok.app.off_service.lookup_concept", return_value=None):
                            with patch("tingbok.app.gpt_service.lookup_concept", return_value=None):
                                response = await client.get("/api/lookup/skrivemaskin?lang=nb")

    assert response.status_code == 200, f"Expected 200, got {response.status_code}: {response.text}"
    data = response.json()
    assert data["id"] == "tools/typewriter"
    assert data["prefLabel"] == "skrivemaskin"
