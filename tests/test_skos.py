"""Tests for SKOS router endpoints and service layer."""

import json
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from tingbok.services.skos import (
    UpstreamError,
    _add_to_not_found_cache,
    _get_cache_path,
    _is_in_not_found_cache,
    _load_from_cache,
    _save_to_cache,
    build_hierarchy_paths,
    cache_stats,
    get_description,
    get_labels,
    get_labels_batch,
    lookup_concept,
    uri_to_source,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_concept_cache(cache_dir: Path, label: str, lang: str, source: str, concept: dict) -> None:
    """Write a concept cache file in the format expected by the service."""
    cache_key = f"concept:{source}:{lang}:{label.lower()}"
    cache_path = _get_cache_path(cache_dir, cache_key)
    data = {**concept, "_cached_at": time.time()}
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(data, f)


def _write_labels_cache(cache_dir: Path, uri: str, source: str, labels: dict[str, str]) -> None:
    """Write a labels cache file in the format expected by the service."""
    import hashlib

    uri_hash = hashlib.md5(uri.encode()).hexdigest()[:16]  # noqa: S324
    cache_key = f"labels:{source}:{uri_hash}"
    cache_path = _get_cache_path(cache_dir, cache_key)
    data = {"uri": uri, "source": source, "labels": labels, "_cached_at": time.time()}
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(data, f)


# ---------------------------------------------------------------------------
# Unit tests for cache utility functions
# ---------------------------------------------------------------------------


def test_cache_path_is_deterministic(tmp_path: Path) -> None:
    """Same key always produces same cache path."""
    p1 = _get_cache_path(tmp_path, "concept:agrovoc:en:potatoes")
    p2 = _get_cache_path(tmp_path, "concept:agrovoc:en:potatoes")
    assert p1 == p2


def test_cache_path_differs_by_key(tmp_path: Path) -> None:
    p1 = _get_cache_path(tmp_path, "concept:agrovoc:en:potatoes")
    p2 = _get_cache_path(tmp_path, "concept:agrovoc:en:carrots")
    assert p1 != p2


def test_save_and_load_cache(tmp_path: Path) -> None:
    cache_path = tmp_path / "test.json"
    _save_to_cache(cache_path, {"uri": "http://example.org/potato", "prefLabel": "potato"})
    data = _load_from_cache(cache_path)
    assert data is not None
    assert data["uri"] == "http://example.org/potato"
    assert "_cached_at" in data


def test_load_cache_returns_none_for_missing_file(tmp_path: Path) -> None:
    result = _load_from_cache(tmp_path / "nonexistent.json")
    assert result is None


def test_load_cache_respects_ttl(tmp_path: Path) -> None:
    cache_path = tmp_path / "old.json"
    data = {"uri": "http://example.org/potato", "_cached_at": time.time() - 999999}
    with open(cache_path, "w") as f:
        json.dump(data, f)
    result = _load_from_cache(cache_path, ttl=3600)
    assert result is None


def test_not_found_cache_add_and_check(tmp_path: Path) -> None:
    key = "concept:agrovoc:en:xyzzy"
    assert not _is_in_not_found_cache(tmp_path, key)
    _add_to_not_found_cache(tmp_path, key)
    assert _is_in_not_found_cache(tmp_path, key)


def test_not_found_cache_respects_ttl(tmp_path: Path) -> None:
    key = "concept:agrovoc:en:xyzzy"
    nf_path = tmp_path / "_not_found.json"
    old_entry = {"entries": {key: {"cached_at": time.time() - 999999}}}
    with open(nf_path, "w") as f:
        json.dump(old_entry, f)
    assert not _is_in_not_found_cache(tmp_path, key, ttl=3600)


def test_lookup_concept_cache_hit(tmp_path: Path) -> None:
    """lookup_concept returns cached data without hitting upstream."""
    concept = {
        "uri": "http://aims.fao.org/aos/agrovoc/c_13551",
        "prefLabel": "potatoes",
        "source": "agrovoc",
        "broader": [{"uri": "http://aims.fao.org/aos/agrovoc/c_8079", "label": "vegetables"}],
    }
    _write_concept_cache(tmp_path, "potatoes", "en", "agrovoc", concept)

    with patch("tingbok.services.skos._upstream_lookup") as mock_upstream:
        result = lookup_concept("potatoes", "en", "agrovoc", tmp_path)

    mock_upstream.assert_not_called()
    assert result is not None
    assert result["uri"] == "http://aims.fao.org/aos/agrovoc/c_13551"
    assert result["prefLabel"] == "potatoes"


def test_lookup_concept_not_found_cache(tmp_path: Path) -> None:
    """lookup_concept returns None immediately for cached not-found entries."""
    cache_key = "concept:agrovoc:en:xyzzy"
    _add_to_not_found_cache(tmp_path, cache_key)

    with patch("tingbok.services.skos._upstream_lookup") as mock_upstream:
        result = lookup_concept("xyzzy", "en", "agrovoc", tmp_path)

    mock_upstream.assert_not_called()
    assert result is None


def test_lookup_concept_cache_miss_upstream_found(tmp_path: Path) -> None:
    """Cache miss triggers upstream, result is cached."""
    upstream_concept = {
        "uri": "http://aims.fao.org/aos/agrovoc/c_13551",
        "prefLabel": "potatoes",
        "source": "agrovoc",
        "broader": [],
    }

    with patch("tingbok.services.skos._upstream_lookup", return_value=(upstream_concept, False)):
        result = lookup_concept("potatoes", "en", "agrovoc", tmp_path)

    assert result is not None
    assert result["uri"] == "http://aims.fao.org/aos/agrovoc/c_13551"

    # Verify it was saved to cache
    cache_key = "concept:agrovoc:en:potatoes"
    cache_path = _get_cache_path(tmp_path, cache_key)
    assert cache_path.exists()


def test_lookup_concept_cache_miss_upstream_not_found(tmp_path: Path) -> None:
    """Cache miss with upstream returning None adds to not-found cache."""
    with patch("tingbok.services.skos._upstream_lookup", return_value=(None, False)):
        result = lookup_concept("xyzzy", "en", "agrovoc", tmp_path)

    assert result is None
    assert _is_in_not_found_cache(tmp_path, "concept:agrovoc:en:xyzzy")


def test_lookup_concept_query_failed_raises_upstream_error(tmp_path: Path) -> None:
    """When upstream query fails transiently, UpstreamError is raised.

    The failure IS cached with a short TTL so repeated calls within that
    window skip the upstream and return None quickly.
    """
    with patch("tingbok.services.skos._upstream_lookup", return_value=(None, True)):
        with pytest.raises(UpstreamError):
            lookup_concept("potato", "en", "agrovoc", tmp_path)

    # Transient failure cached with short TTL — subsequent call returns None without retrying
    assert _is_in_not_found_cache(tmp_path, "concept:agrovoc:en:potato")

    with patch("tingbok.services.skos._upstream_lookup") as mock_upstream:
        result = lookup_concept("potato", "en", "agrovoc", tmp_path)
    assert result is None
    mock_upstream.assert_not_called()


def test_lookup_concept_transient_failure_retried_after_ttl(tmp_path: Path) -> None:
    """Transient not-found entries expire after TRANSIENT_TTL_SECONDS, not the full 60-day TTL."""
    from tingbok.services.skos import TRANSIENT_TTL_SECONDS

    with patch("tingbok.services.skos._upstream_lookup", return_value=(None, True)):
        with pytest.raises(UpstreamError):
            lookup_concept("spanner", "en", "dbpedia", tmp_path)

    # Simulate the transient TTL having expired
    with patch("tingbok.services.skos.time") as mock_time:
        mock_time.time.return_value = time.time() + TRANSIENT_TTL_SECONDS + 1
        assert not _is_in_not_found_cache(tmp_path, "concept:dbpedia:en:spanner")


# ---------------------------------------------------------------------------
# Unit tests for hierarchy building
# ---------------------------------------------------------------------------


def test_hierarchy_not_found(tmp_path: Path) -> None:
    """Returns empty paths and found=False when concept not in cache and upstream fails."""
    with patch("tingbok.services.skos.lookup_concept", return_value=None):
        paths, found, uri_map = build_hierarchy_paths("xyzzy", "en", "agrovoc", tmp_path)
    assert paths == []
    assert found is False
    assert uri_map == {}


def test_hierarchy_root_concept(tmp_path: Path) -> None:
    """A concept with no broader becomes a single-element path."""
    concept = {
        "uri": "http://aims.fao.org/aos/agrovoc/c_001",
        "prefLabel": "food",
        "source": "agrovoc",
        "broader": [],
    }
    _write_concept_cache(tmp_path, "food", "en", "agrovoc", concept)

    paths, found, uri_map = build_hierarchy_paths("food", "en", "agrovoc", tmp_path)
    assert found is True
    assert paths == ["food"]
    assert "food" in uri_map


def test_hierarchy_one_level(tmp_path: Path) -> None:
    """A concept one level below a root produces a two-part path."""
    root = {
        "uri": "http://aims.fao.org/aos/agrovoc/c_001",
        "prefLabel": "food",
        "source": "agrovoc",
        "broader": [],
    }
    child = {
        "uri": "http://aims.fao.org/aos/agrovoc/c_002",
        "prefLabel": "vegetables",
        "source": "agrovoc",
        "broader": [{"uri": "http://aims.fao.org/aos/agrovoc/c_001", "label": "food"}],
    }
    _write_concept_cache(tmp_path, "food", "en", "agrovoc", root)
    _write_concept_cache(tmp_path, "vegetables", "en", "agrovoc", child)

    paths, found, uri_map = build_hierarchy_paths("vegetables", "en", "agrovoc", tmp_path)
    assert found is True
    assert "food/vegetables" in paths
    assert "food/vegetables" in uri_map
    assert uri_map["food/vegetables"] == "http://aims.fao.org/aos/agrovoc/c_002"


def test_hierarchy_two_levels(tmp_path: Path) -> None:
    """A concept two levels deep produces the correct three-part path."""
    root = {"uri": "http://x/root", "prefLabel": "food", "source": "agrovoc", "broader": []}
    mid = {
        "uri": "http://x/mid",
        "prefLabel": "vegetables",
        "source": "agrovoc",
        "broader": [{"uri": "http://x/root", "label": "food"}],
    }
    leaf = {
        "uri": "http://x/leaf",
        "prefLabel": "potatoes",
        "source": "agrovoc",
        "broader": [{"uri": "http://x/mid", "label": "vegetables"}],
    }
    for label, concept in [("food", root), ("vegetables", mid), ("potatoes", leaf)]:
        _write_concept_cache(tmp_path, label, "en", "agrovoc", concept)

    paths, found, uri_map = build_hierarchy_paths("potatoes", "en", "agrovoc", tmp_path)
    assert found is True
    assert "food/vegetables/potatoes" in paths
    assert uri_map["food/vegetables/potatoes"] == "http://x/leaf"


def test_hierarchy_root_mapping(tmp_path: Path) -> None:
    """AGROVOC verbose roots are mapped to concise ones (e.g., 'plant products' → 'food')."""
    root = {
        "uri": "http://x/plant_products",
        "prefLabel": "Plant products",
        "source": "agrovoc",
        "broader": [],
    }
    child = {
        "uri": "http://x/vegetables",
        "prefLabel": "Vegetables",
        "source": "agrovoc",
        "broader": [{"uri": "http://x/plant_products", "label": "Plant products"}],
    }
    _write_concept_cache(tmp_path, "plant products", "en", "agrovoc", root)
    _write_concept_cache(tmp_path, "vegetables", "en", "agrovoc", child)

    paths, found, uri_map = build_hierarchy_paths("vegetables", "en", "agrovoc", tmp_path)
    assert found is True
    # "plant products" is mapped to "food"
    assert any(p.startswith("food/") for p in paths)
    assert all(not p.startswith("plant_products/") for p in paths)


def test_hierarchy_multiple_broader_produces_multiple_paths(tmp_path: Path) -> None:
    """A concept with two broader concepts produces two hierarchy paths."""
    root_a = {"uri": "http://x/a", "prefLabel": "food", "source": "agrovoc", "broader": []}
    root_b = {"uri": "http://x/b", "prefLabel": "nutrition", "source": "agrovoc", "broader": []}
    leaf = {
        "uri": "http://x/leaf",
        "prefLabel": "potatoes",
        "source": "agrovoc",
        "broader": [
            {"uri": "http://x/a", "label": "food"},
            {"uri": "http://x/b", "label": "nutrition"},
        ],
    }
    _write_concept_cache(tmp_path, "food", "en", "agrovoc", root_a)
    _write_concept_cache(tmp_path, "nutrition", "en", "agrovoc", root_b)
    _write_concept_cache(tmp_path, "potatoes", "en", "agrovoc", leaf)

    paths, found, uri_map = build_hierarchy_paths("potatoes", "en", "agrovoc", tmp_path)
    assert found is True
    assert len(paths) == 2
    assert "food/potatoes" in paths
    assert "nutrition/potatoes" in paths


def test_hierarchy_cycle_detection(tmp_path: Path) -> None:
    """Cycles in the broader graph are detected and don't loop forever."""
    # A → B → A (cycle)
    concept_a = {
        "uri": "http://x/a",
        "prefLabel": "alpha",
        "source": "agrovoc",
        "broader": [{"uri": "http://x/b", "label": "beta"}],
    }
    concept_b = {
        "uri": "http://x/b",
        "prefLabel": "beta",
        "source": "agrovoc",
        "broader": [{"uri": "http://x/a", "label": "alpha"}],
    }
    _write_concept_cache(tmp_path, "alpha", "en", "agrovoc", concept_a)
    _write_concept_cache(tmp_path, "beta", "en", "agrovoc", concept_b)

    # Should return without infinite looping
    paths, found, uri_map = build_hierarchy_paths("alpha", "en", "agrovoc", tmp_path)
    # Cycle terminates; may or may not produce paths but must not hang
    assert isinstance(paths, list)


def test_hierarchy_label_normalisation(tmp_path: Path) -> None:
    """Spaces and hyphens are replaced with underscores in path segments."""
    root = {"uri": "http://x/r", "prefLabel": "root vegetables", "source": "agrovoc", "broader": []}
    _write_concept_cache(tmp_path, "root vegetables", "en", "agrovoc", root)

    paths, found, uri_map = build_hierarchy_paths("root vegetables", "en", "agrovoc", tmp_path)
    assert found is True
    assert "root_vegetables" in paths[0]


def test_hierarchy_dead_end_broader_discards_branch(tmp_path: Path) -> None:
    """A broader concept matching _HIERARCHY_DEAD_ENDS silently discards that branch."""
    # "cushion" has a broader of "property" (a dead-end) — the branch should be dropped
    cushion = {
        "uri": "http://dbpedia.org/resource/Cushion",
        "prefLabel": "Cushion",
        "source": "dbpedia",
        "broader": [{"uri": "http://dbpedia.org/resource/Property", "label": "property"}],
    }
    _write_concept_cache(tmp_path, "cushion", "en", "dbpedia", cushion)

    paths, found, uri_map = build_hierarchy_paths("cushion", "en", "dbpedia", tmp_path)
    # found=True because cushion itself was found; but no complete path emitted via "property"
    assert found is True
    assert not any("property" in p for p in paths)


# ---------------------------------------------------------------------------
# Unit tests for batch label fetching
# ---------------------------------------------------------------------------


def test_get_labels_batch_all_cached(tmp_path: Path) -> None:
    """All URIs served from cache without upstream calls."""
    uri1 = "http://aims.fao.org/aos/agrovoc/c_1"
    uri2 = "http://aims.fao.org/aos/agrovoc/c_2"
    _write_labels_cache(tmp_path, uri1, "agrovoc", {"en": "potato", "nb": "potet"})
    _write_labels_cache(tmp_path, uri2, "agrovoc", {"en": "carrot", "nb": "gulrot"})

    with patch("tingbok.services.skos._upstream_get_labels") as mock_upstream:
        result = get_labels_batch([uri1, uri2], ["en", "nb"], "agrovoc", tmp_path)

    mock_upstream.assert_not_called()
    assert result[uri1] == {"en": "potato", "nb": "potet"}
    assert result[uri2] == {"en": "carrot", "nb": "gulrot"}


def test_get_labels_batch_partial_cache(tmp_path: Path) -> None:
    """Cached URIs returned immediately; uncached ones trigger upstream."""
    uri1 = "http://aims.fao.org/aos/agrovoc/c_1"
    uri2 = "http://aims.fao.org/aos/agrovoc/c_2"
    _write_labels_cache(tmp_path, uri1, "agrovoc", {"en": "potato"})

    with patch("tingbok.services.skos._upstream_get_labels", return_value={"en": "carrot"}) as mock_upstream:
        result = get_labels_batch([uri1, uri2], ["en"], "agrovoc", tmp_path)

    mock_upstream.assert_called_once_with(uri2, "agrovoc", ["en"])
    assert result[uri1] == {"en": "potato"}
    assert result[uri2] == {"en": "carrot"}


def test_get_labels_batch_empty_input(tmp_path: Path) -> None:
    result = get_labels_batch([], ["en"], "agrovoc", tmp_path)
    assert result == {}


# ---------------------------------------------------------------------------
# Unit tests for cache stats
# ---------------------------------------------------------------------------


def test_cache_stats_empty(tmp_path: Path) -> None:
    stats = cache_stats(tmp_path)
    assert stats["concept_count"] == 0
    assert stats["labels_count"] == 0
    assert stats["not_found_count"] == 0


def test_cache_stats_counts_files(tmp_path: Path) -> None:
    _write_concept_cache(tmp_path, "potato", "en", "agrovoc", {"uri": "http://x/p", "prefLabel": "potato"})
    _write_concept_cache(tmp_path, "carrot", "en", "agrovoc", {"uri": "http://x/c", "prefLabel": "carrot"})
    _write_labels_cache(tmp_path, "http://x/p", "agrovoc", {"en": "potato"})
    _add_to_not_found_cache(tmp_path, "concept:agrovoc:en:xyzzy")

    stats = cache_stats(tmp_path)
    assert stats["concept_count"] == 2
    assert stats["labels_count"] == 1
    assert stats["not_found_count"] == 1


# ---------------------------------------------------------------------------
# HTTP endpoint tests — lookup and labels
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_skos_lookup_cache_hit(client, skos_cache_dir: Path) -> None:
    """GET /api/skos/lookup returns 200 when concept is in cache."""
    concept = {
        "uri": "http://aims.fao.org/aos/agrovoc/c_13551",
        "prefLabel": "potatoes",
        "source": "agrovoc",
        "broader": [{"uri": "http://aims.fao.org/aos/agrovoc/c_8079", "label": "vegetables"}],
    }
    _write_concept_cache(skos_cache_dir, "potatoes", "en", "agrovoc", concept)

    response = await client.get("/api/skos/lookup", params={"label": "potatoes", "lang": "en", "source": "agrovoc"})
    assert response.status_code == 200
    data = response.json()
    assert data["uri"] == "http://aims.fao.org/aos/agrovoc/c_13551"
    assert data["prefLabel"] == "potatoes"
    assert data["source"] == "agrovoc"
    broader_uris = [b["uri"] for b in data["broader"]]
    assert "http://aims.fao.org/aos/agrovoc/c_8079" in broader_uris


@pytest.mark.anyio
async def test_skos_lookup_not_found_cache(client, skos_cache_dir: Path) -> None:
    """GET /api/skos/lookup returns 404 when concept is in not-found cache."""
    _add_to_not_found_cache(skos_cache_dir, "concept:agrovoc:en:xyzzy")

    with patch("tingbok.services.skos._upstream_lookup") as mock_upstream:
        response = await client.get("/api/skos/lookup", params={"label": "xyzzy", "lang": "en", "source": "agrovoc"})
    mock_upstream.assert_not_called()
    assert response.status_code == 404


@pytest.mark.anyio
async def test_skos_lookup_cache_miss_upstream_found(client, skos_cache_dir: Path) -> None:
    """Cache miss triggers upstream fetch; returns 200 when upstream finds concept."""
    upstream_concept = {
        "uri": "http://aims.fao.org/aos/agrovoc/c_13551",
        "prefLabel": "potatoes",
        "source": "agrovoc",
        "broader": [],
    }
    with patch("tingbok.services.skos._upstream_lookup", return_value=(upstream_concept, False)):
        response = await client.get("/api/skos/lookup", params={"label": "potatoes", "lang": "en", "source": "agrovoc"})
    assert response.status_code == 200
    assert response.json()["uri"] == "http://aims.fao.org/aos/agrovoc/c_13551"


@pytest.mark.anyio
async def test_skos_lookup_cache_miss_upstream_not_found(client, skos_cache_dir: Path) -> None:
    """Cache miss with upstream not finding concept returns 404."""
    with patch("tingbok.services.skos._upstream_lookup", return_value=(None, False)):
        response = await client.get("/api/skos/lookup", params={"label": "xyzzy", "lang": "en", "source": "agrovoc"})
    assert response.status_code == 404


@pytest.mark.anyio
async def test_skos_lookup_upstream_error_returns_502(client, skos_cache_dir: Path) -> None:
    """Transient upstream error (e.g. 403/timeout) returns 502, not 404.

    The failure IS cached with a short TTL so it is not immediately retried on the
    next request.
    """
    with patch("tingbok.services.skos._upstream_lookup", return_value=(None, True)):
        response = await client.get("/api/skos/lookup", params={"label": "cumin", "lang": "nb", "source": "wikidata"})
    assert response.status_code == 502
    assert _is_in_not_found_cache(skos_cache_dir, "concept:wikidata:nb:cumin")


@pytest.mark.anyio
async def test_skos_lookup_missing_label_param(client, skos_cache_dir: Path) -> None:
    """GET /api/skos/lookup without label returns 422."""
    response = await client.get("/api/skos/lookup")
    assert response.status_code == 422


@pytest.mark.anyio
async def test_skos_labels_cache_hit(client, skos_cache_dir: Path) -> None:
    """GET /api/skos/labels returns cached labels."""
    uri = "http://aims.fao.org/aos/agrovoc/c_13551"
    _write_labels_cache(skos_cache_dir, uri, "agrovoc", {"en": "potatoes", "nb": "poteter", "de": "Kartoffeln"})

    response = await client.get("/api/skos/labels", params={"uri": uri, "languages": "en,nb,de", "source": "agrovoc"})
    assert response.status_code == 200
    data = response.json()
    assert data["labels"]["en"] == "potatoes"
    assert data["labels"]["nb"] == "poteter"
    assert data["labels"]["de"] == "Kartoffeln"


@pytest.mark.anyio
async def test_skos_labels_cache_miss_upstream(client, skos_cache_dir: Path) -> None:
    """Cache miss triggers upstream; returns available labels."""
    uri = "http://aims.fao.org/aos/agrovoc/c_13551"
    with patch("tingbok.services.skos._upstream_get_labels", return_value={"en": "potatoes", "nb": "poteter"}):
        response = await client.get("/api/skos/labels", params={"uri": uri, "languages": "en,nb", "source": "agrovoc"})
    assert response.status_code == 200
    data = response.json()
    assert data["labels"]["en"] == "potatoes"
    assert data["labels"]["nb"] == "poteter"


@pytest.mark.anyio
async def test_skos_labels_filters_requested_languages(client, skos_cache_dir: Path) -> None:
    """Only requested languages are returned even if cache has more."""
    uri = "http://aims.fao.org/aos/agrovoc/c_13551"
    _write_labels_cache(skos_cache_dir, uri, "agrovoc", {"en": "potatoes", "nb": "poteter", "de": "Kartoffeln"})

    response = await client.get("/api/skos/labels", params={"uri": uri, "languages": "en", "source": "agrovoc"})
    assert response.status_code == 200
    data = response.json()
    assert "en" in data["labels"]
    assert "nb" not in data["labels"]
    assert "de" not in data["labels"]


# ---------------------------------------------------------------------------
# HTTP endpoint tests — hierarchy
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_skos_hierarchy_found(client, skos_cache_dir: Path) -> None:
    """GET /api/skos/hierarchy returns paths when concept is cached."""
    root = {"uri": "http://x/food", "prefLabel": "food", "source": "agrovoc", "broader": []}
    child = {
        "uri": "http://x/veg",
        "prefLabel": "vegetables",
        "source": "agrovoc",
        "broader": [{"uri": "http://x/food", "label": "food"}],
    }
    leaf = {
        "uri": "http://x/potato",
        "prefLabel": "potatoes",
        "source": "agrovoc",
        "broader": [{"uri": "http://x/veg", "label": "vegetables"}],
    }
    for label, concept in [("food", root), ("vegetables", child), ("potatoes", leaf)]:
        _write_concept_cache(skos_cache_dir, label, "en", "agrovoc", concept)

    response = await client.get("/api/skos/hierarchy", params={"label": "potatoes", "lang": "en", "source": "agrovoc"})
    assert response.status_code == 200
    data = response.json()
    assert data["found"] is True
    assert "food/vegetables/potatoes" in data["paths"]
    assert "uri_map" in data
    assert data["uri_map"].get("food/vegetables/potatoes") == "http://x/potato"


@pytest.mark.anyio
async def test_skos_hierarchy_not_found(client, skos_cache_dir: Path) -> None:
    """GET /api/skos/hierarchy returns found=False when concept doesn't exist."""
    with patch("tingbok.services.skos._upstream_lookup", return_value=(None, False)):
        response = await client.get("/api/skos/hierarchy", params={"label": "xyzzy", "lang": "en", "source": "agrovoc"})
    assert response.status_code == 200
    data = response.json()
    assert data["found"] is False
    assert data["paths"] == []


@pytest.mark.anyio
async def test_skos_hierarchy_missing_label(client, skos_cache_dir: Path) -> None:
    """GET /api/skos/hierarchy without label returns 422."""
    response = await client.get("/api/skos/hierarchy")
    assert response.status_code == 422


# ---------------------------------------------------------------------------
# HTTP endpoint tests — batch labels
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_skos_labels_batch(client, skos_cache_dir: Path) -> None:
    """POST /api/skos/labels/batch returns labels for all requested URIs."""
    uri1 = "http://aims.fao.org/aos/agrovoc/c_1"
    uri2 = "http://aims.fao.org/aos/agrovoc/c_2"
    _write_labels_cache(skos_cache_dir, uri1, "agrovoc", {"en": "potato", "nb": "potet"})
    _write_labels_cache(skos_cache_dir, uri2, "agrovoc", {"en": "carrot", "nb": "gulrot"})

    response = await client.post(
        "/api/skos/labels/batch",
        json={"uris": [uri1, uri2], "languages": ["en", "nb"], "source": "agrovoc"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["labels"][uri1]["en"] == "potato"
    assert data["labels"][uri2]["nb"] == "gulrot"


@pytest.mark.anyio
async def test_skos_labels_batch_empty(client, skos_cache_dir: Path) -> None:
    """POST /api/skos/labels/batch with empty uris list returns empty dict."""
    response = await client.post(
        "/api/skos/labels/batch",
        json={"uris": [], "languages": ["en"], "source": "agrovoc"},
    )
    assert response.status_code == 200
    data = response.json()
    assert data["labels"] == {}


# ---------------------------------------------------------------------------
# HTTP endpoint tests — cache stats
# ---------------------------------------------------------------------------


# ---------------------------------------------------------------------------
# Tests for uri_to_source()
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("uri", "expected"),
    [
        ("http://aims.fao.org/aos/agrovoc/c_3032", "agrovoc"),
        ("https://aims.fao.org/aos/agrovoc/c_3032", "agrovoc"),
        ("http://dbpedia.org/resource/Food", "dbpedia"),
        ("https://dbpedia.org/resource/Food", "dbpedia"),
        ("http://www.wikidata.org/entity/Q2095", "wikidata"),
        ("https://www.wikidata.org/entity/Q2095", "wikidata"),
        ("off:en:potatoes", "off"),
        ("gpt:632", "gpt"),
        ("https://tingbok.plann.no/api/vocabulary/food", None),
        ("https://example.com/unknown", None),
        ("", None),
    ],
)
def test_uri_to_source(uri: str, expected: str | None) -> None:
    """uri_to_source should map URIs to their source names."""
    assert uri_to_source(uri) == expected


# ---------------------------------------------------------------------------
# Tests for get_description()
# ---------------------------------------------------------------------------


def test_get_description_returns_none_when_not_cached(tmp_path: Path) -> None:
    """get_description returns None when no cache file exists."""
    result = get_description("http://dbpedia.org/resource/Food", "dbpedia", "en", tmp_path)
    assert result is None


def test_get_description_reads_from_description_cache(tmp_path: Path) -> None:
    """get_description reads from its own description: cache key (not the labels cache)."""
    import hashlib
    import json
    import time

    uri = "http://dbpedia.org/resource/Food"
    source = "dbpedia"
    uri_hash = hashlib.md5(uri.encode()).hexdigest()[:16]  # noqa: S324
    cache_key = f"description:{source}:{uri_hash}"
    cache_path = _get_cache_path(tmp_path, cache_key)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "uri": uri,
        "source": source,
        "labels": {"en": "Food"},
        "description": "Food is...",
        "_cached_at": time.time(),
    }
    with open(cache_path, "w", encoding="utf-8") as f:
        json.dump(data, f)

    result = get_description(uri, source, "en", tmp_path)
    assert result == "Food is..."


def test_get_description_dbpedia_fetches_from_api(tmp_path: Path) -> None:
    """get_description fetches from DBpedia data API and caches the description."""
    uri = "http://dbpedia.org/resource/Food"
    fake_response = {
        uri: {
            "http://www.w3.org/2000/01/rdf-schema#label": [{"lang": "en", "value": "Food", "type": "literal"}],
            "http://www.w3.org/2000/01/rdf-schema#comment": [
                {"lang": "en", "value": "Food is any substance consumed to provide nutrition.", "type": "literal"},
                {"lang": "de", "value": "Essen ist...", "type": "literal"},
            ],
        }
    }

    mock_resp = type(
        "R", (), {"raise_for_status": lambda self: None, "json": lambda self: fake_response, "status_code": 200}
    )()

    with patch("niquests.Session") as mock_session_cls:
        mock_session_cls.return_value.__enter__ = lambda s: mock_session_cls.return_value
        mock_session_cls.return_value.__exit__ = lambda s, *a: None
        mock_session_cls.return_value.get.return_value = mock_resp
        result = get_description(uri, "dbpedia", "en", tmp_path)

    assert result == "Food is any substance consumed to provide nutrition."

    # Should also be cached now
    result2 = get_description(uri, "dbpedia", "en", tmp_path)
    assert result2 == "Food is any substance consumed to provide nutrition."


def test_get_description_wikidata_fetches_from_api(tmp_path: Path) -> None:
    """get_description fetches from Wikidata descriptions API and caches."""
    uri = "http://www.wikidata.org/entity/Q2095"
    fake_response = {"en": "any nutritional substance consumed to provide energy", "nb": "mat"}

    mock_resp = type(
        "R", (), {"raise_for_status": lambda self: None, "json": lambda self: fake_response, "status_code": 200}
    )()

    with patch("niquests.Session") as mock_session_cls:
        mock_session_cls.return_value.__enter__ = lambda s: mock_session_cls.return_value
        mock_session_cls.return_value.__exit__ = lambda s, *a: None
        mock_session_cls.return_value.get.return_value = mock_resp
        result = get_description(uri, "wikidata", "en", tmp_path)

    assert result == "any nutritional substance consumed to provide energy"


def test_get_description_unsupported_source_returns_none(tmp_path: Path) -> None:
    """get_description returns None for sources without description support."""
    result = get_description("http://aims.fao.org/aos/agrovoc/c_3032", "agrovoc", "en", tmp_path)
    assert result is None


def test_get_description_independent_of_labels_cache(tmp_path: Path) -> None:
    """get_description must not be blocked by a labels cache entry without description."""
    import hashlib
    import json
    import time

    uri = "http://dbpedia.org/resource/Food"
    source = "dbpedia"
    uri_hash = hashlib.md5(uri.encode()).hexdigest()[:16]  # noqa: S324

    # Simulate labels having been cached (without description key)
    labels_cache_key = f"labels:{source}:{uri_hash}"
    labels_cache_path = _get_cache_path(tmp_path, labels_cache_key)
    labels_cache_path.parent.mkdir(parents=True, exist_ok=True)
    with open(labels_cache_path, "w", encoding="utf-8") as f:
        json.dump({"uri": uri, "source": source, "labels": {"en": "Food"}, "_cached_at": time.time()}, f)

    # get_description should still fetch from API (different cache key)
    fake_response = {
        uri: {
            "http://www.w3.org/2000/01/rdf-schema#comment": [
                {"lang": "en", "value": "Food is nutrition.", "type": "literal"},
            ],
        }
    }
    mock_resp = type(
        "R", (), {"raise_for_status": lambda self: None, "json": lambda self: fake_response, "status_code": 200}
    )()

    with patch("niquests.Session") as mock_session_cls:
        mock_session_cls.return_value.__enter__ = lambda s: mock_session_cls.return_value
        mock_session_cls.return_value.__exit__ = lambda s, *a: None
        mock_session_cls.return_value.get.return_value = mock_resp
        result = get_description(uri, source, "en", tmp_path)

    assert result == "Food is nutrition."


# ---------------------------------------------------------------------------
# Tests for get_labels() caching on HTTP errors
# ---------------------------------------------------------------------------


def test_get_labels_caches_empty_result_on_http_error(tmp_path: Path) -> None:
    """get_labels should cache empty result on HTTP error (e.g. 404) to avoid re-querying."""
    import niquests

    uri = "http://dbpedia.org/resource/Nonexistent"
    source = "dbpedia"

    call_count = [0]

    def raise_http_error(self):
        call_count[0] += 1
        raise niquests.exceptions.HTTPError("404 Not Found")

    mock_resp = type("R", (), {"raise_for_status": raise_http_error, "status_code": 404})()

    with patch("niquests.Session") as mock_session_cls:
        mock_session_cls.return_value.__enter__ = lambda s: mock_session_cls.return_value
        mock_session_cls.return_value.__exit__ = lambda s, *a: None
        mock_session_cls.return_value.get.return_value = mock_resp

        result1 = get_labels(uri, ["en", "nb"], source, tmp_path)
        result2 = get_labels(uri, ["en", "nb"], source, tmp_path)

    assert result1 == {}
    assert result2 == {}
    # API should only be called once — second call served from cache
    assert call_count[0] == 1


def test_get_labels_does_not_cache_on_transient_error(tmp_path: Path) -> None:
    """get_labels should NOT cache on transient errors (timeout, connection failure)."""
    import niquests

    uri = "http://dbpedia.org/resource/Food"
    source = "dbpedia"

    call_count = [0]

    def raise_timeout(self):
        call_count[0] += 1
        raise niquests.exceptions.ConnectionError("Connection refused")

    mock_resp = type("R", (), {"raise_for_status": raise_timeout})()

    with patch("niquests.Session") as mock_session_cls:
        mock_session_cls.return_value.__enter__ = lambda s: mock_session_cls.return_value
        mock_session_cls.return_value.__exit__ = lambda s, *a: None
        mock_session_cls.return_value.get.return_value = mock_resp

        result1 = get_labels(uri, ["en"], source, tmp_path)
        result2 = get_labels(uri, ["en"], source, tmp_path)

    assert result1 == {}
    assert result2 == {}
    # API called both times — transient error should not be cached
    assert call_count[0] == 2


def test_get_labels_caches_empty_result_when_no_labels_found(tmp_path: Path) -> None:
    """get_labels should cache even when the API returns 200 but no matching labels."""
    uri = "http://dbpedia.org/resource/Food"
    source = "dbpedia"

    call_count = [0]

    def counting_get(url, **kwargs):
        call_count[0] += 1
        # Return response with empty data for this URI
        return type(
            "R",
            (),
            {
                "raise_for_status": lambda self: None,
                "json": lambda self: {uri: {}},  # URI present but no labels
                "status_code": 200,
            },
        )()

    with patch("niquests.Session") as mock_session_cls:
        mock_session_cls.return_value.__enter__ = lambda s: mock_session_cls.return_value
        mock_session_cls.return_value.__exit__ = lambda s, *a: None
        mock_session_cls.return_value.get.side_effect = counting_get

        result1 = get_labels(uri, ["en"], source, tmp_path)
        result2 = get_labels(uri, ["en"], source, tmp_path)

    assert result1 == {}
    assert result2 == {}
    # API should only be called once
    assert call_count[0] == 1


@pytest.mark.anyio
async def test_skos_cache_stats(client, skos_cache_dir: Path) -> None:
    """GET /api/skos/cache returns cache statistics."""
    _write_concept_cache(skos_cache_dir, "potato", "en", "agrovoc", {"uri": "http://x/p", "prefLabel": "potato"})
    _write_labels_cache(skos_cache_dir, "http://x/p", "agrovoc", {"en": "potato"})
    _add_to_not_found_cache(skos_cache_dir, "concept:agrovoc:en:xyzzy")

    response = await client.get("/api/skos/cache")
    assert response.status_code == 200
    data = response.json()
    assert data["concept_count"] >= 1
    assert data["labels_count"] >= 1
    assert data["not_found_count"] >= 1
    assert "cache_dir" in data


# ---------------------------------------------------------------------------
# DBpedia label quality: HTML stripping and similarity threshold
# ---------------------------------------------------------------------------


def _make_dbpedia_response(label: str, resource_uri: str) -> dict:
    """Build a minimal DBpedia Lookup API response dict."""
    return {"docs": [{"resource": [resource_uri], "label": [label]}]}


def test_lookup_dbpedia_strips_html_from_label(tmp_path: Path) -> None:
    """DBpedia labels with HTML highlight markup are stripped before caching."""
    from tingbok.services.skos import _lookup_dbpedia

    response_data = _make_dbpedia_response("<b>Plastic</b> <b>bag</b>", "http://dbpedia.org/resource/Plastic_bag")
    with patch("tingbok.services.skos.niquests.Session") as mock_sess:
        sess = mock_sess.return_value.__enter__.return_value
        sess.get.return_value.raise_for_status.return_value = None
        sess.get.return_value.json.return_value = response_data
        with patch("tingbok.services.skos._get_broader_dbpedia", return_value=[]):
            result, failed = _lookup_dbpedia("plastic bag", "en")

    assert not failed
    assert result is not None
    assert "<b>" not in result["prefLabel"]
    assert result["prefLabel"] == "Plastic bag"


def test_lookup_dbpedia_rejects_low_similarity_fallback(tmp_path: Path) -> None:
    """When no exact match, a first result unrelated to the query returns None."""
    from tingbok.services.skos import _lookup_dbpedia

    # "mounting tool" → DBpedia happens to return "List of Naruto episodes" first
    response_data = _make_dbpedia_response(
        "List of Naruto episodes", "http://dbpedia.org/resource/List_of_Naruto_episodes"
    )
    with patch("tingbok.services.skos.niquests.Session") as mock_sess:
        sess = mock_sess.return_value.__enter__.return_value
        sess.get.return_value.raise_for_status.return_value = None
        sess.get.return_value.json.return_value = response_data
        result, failed = _lookup_dbpedia("mounting tool", "en")

    assert not failed
    assert result is None  # Low-similarity first result must be rejected


def test_lookup_dbpedia_accepts_similar_fallback(tmp_path: Path) -> None:
    """A first result sufficiently similar to the query is accepted."""
    from tingbok.services.skos import _lookup_dbpedia

    # "bread crumbs" → DBpedia returns "Bread crumb" (singular, high similarity)
    response_data = _make_dbpedia_response("Bread crumb", "http://dbpedia.org/resource/Bread_crumb")
    with patch("tingbok.services.skos.niquests.Session") as mock_sess:
        sess = mock_sess.return_value.__enter__.return_value
        sess.get.return_value.raise_for_status.return_value = None
        sess.get.return_value.json.return_value = response_data
        with patch("tingbok.services.skos._get_broader_dbpedia", return_value=[]):
            result, failed = _lookup_dbpedia("bread crumbs", "en")

    assert not failed
    assert result is not None  # High-similarity result accepted


def test_lookup_dbpedia_filters_list_articles_from_hierarchy(tmp_path: Path) -> None:
    """DBpedia 'List_of_*' URIs are skipped during broader-concept traversal."""
    from tingbok.services.skos import _lookup_dbpedia

    resource_uri = "http://dbpedia.org/resource/Bread_crumbs"
    response_data = _make_dbpedia_response("Bread crumbs", resource_uri)
    broader_with_list = [
        {"uri": "http://dbpedia.org/resource/List_of_ancient_dishes", "label": "List of ancient dishes"},
        {"uri": "http://dbpedia.org/resource/Bread", "label": "Bread"},
    ]
    with patch("tingbok.services.skos.niquests.Session") as mock_sess:
        sess = mock_sess.return_value.__enter__.return_value
        sess.get.return_value.raise_for_status.return_value = None
        sess.get.return_value.json.return_value = response_data
        with patch("tingbok.services.skos._get_broader_dbpedia", return_value=broader_with_list):
            result, failed = _lookup_dbpedia("bread crumbs", "en")

    assert not failed
    assert result is not None
    broader_uris = [b["uri"] for b in result["broader"]]
    assert "http://dbpedia.org/resource/List_of_ancient_dishes" not in broader_uris
    assert "http://dbpedia.org/resource/Bread" in broader_uris
