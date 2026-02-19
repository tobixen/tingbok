"""Tests for SKOS router endpoints and service layer."""

import json
import time
from pathlib import Path
from unittest.mock import patch

import pytest

from tingbok.services.skos import (
    _add_to_not_found_cache,
    _get_cache_path,
    _is_in_not_found_cache,
    _load_from_cache,
    _save_to_cache,
    lookup_concept,
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

    uri_hash = hashlib.md5(uri.encode()).hexdigest()[:16]
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


def test_lookup_concept_query_failed_not_cached(tmp_path: Path) -> None:
    """When upstream query fails (network error), result is not cached."""
    with patch("tingbok.services.skos._upstream_lookup", return_value=(None, True)):
        result = lookup_concept("potato", "en", "agrovoc", tmp_path)

    assert result is None
    assert not _is_in_not_found_cache(tmp_path, "concept:agrovoc:en:potato")


# ---------------------------------------------------------------------------
# HTTP endpoint tests
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
    # broader should be list of URI strings
    assert "http://aims.fao.org/aos/agrovoc/c_8079" in data["broader"]


@pytest.mark.anyio
async def test_skos_lookup_not_found_cache(client, skos_cache_dir: Path) -> None:
    """GET /api/skos/lookup returns 404 when concept is in not-found cache."""
    cache_key = "concept:agrovoc:en:xyzzy"
    _add_to_not_found_cache(skos_cache_dir, cache_key)

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
    data = response.json()
    assert data["uri"] == "http://aims.fao.org/aos/agrovoc/c_13551"


@pytest.mark.anyio
async def test_skos_lookup_cache_miss_upstream_not_found(client, skos_cache_dir: Path) -> None:
    """Cache miss with upstream not finding concept returns 404."""
    with patch("tingbok.services.skos._upstream_lookup", return_value=(None, False)):
        response = await client.get("/api/skos/lookup", params={"label": "xyzzy", "lang": "en", "source": "agrovoc"})

    assert response.status_code == 404


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
    assert data["uri"] == uri
    assert data["labels"]["en"] == "potatoes"
    assert data["labels"]["nb"] == "poteter"
    assert data["labels"]["de"] == "Kartoffeln"


@pytest.mark.anyio
async def test_skos_labels_cache_miss_upstream(client, skos_cache_dir: Path) -> None:
    """Cache miss triggers upstream; returns available labels."""
    uri = "http://aims.fao.org/aos/agrovoc/c_13551"
    upstream_labels = {"en": "potatoes", "nb": "poteter"}

    with patch("tingbok.services.skos._upstream_get_labels", return_value=upstream_labels):
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


@pytest.mark.anyio
async def test_skos_hierarchy_stub(client) -> None:
    response = await client.get("/api/skos/hierarchy", params={"label": "potato"})
    assert response.status_code == 501
