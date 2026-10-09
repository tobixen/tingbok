"""Tests for Open Food Facts (OFF) lookup service."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

# Bound before conftest's autouse fixture stubs the loader out, so the loader
# itself can still be tested.
from tingbok.services.off import _get_taxonomy as _real_get_taxonomy


def _make_node(node_id: str, names: dict, synonyms: dict | None = None, parents: list | None = None) -> MagicMock:
    node = MagicMock()
    node.id = node_id
    node.names = names
    node.synonyms = synonyms or {}
    node.parents = parents or []
    node.get_localized_name.side_effect = lambda lang: names.get(lang, node_id)
    return node


def _make_taxonomy(nodes: list) -> MagicMock:
    taxonomy = MagicMock()
    taxonomy.__len__ = MagicMock(return_value=len(nodes))
    taxonomy.iter_nodes.side_effect = lambda: iter(nodes)
    node_map = {n.id: n for n in nodes}
    taxonomy.__getitem__ = lambda self, key: node_map[key]
    taxonomy.__contains__ = lambda self, key: key in node_map
    return taxonomy


# Shared test fixtures
_PARENT_NODE = _make_node("en:food", {"en": "Food"}, parents=[])
_CHILD_NODE = _make_node(
    "en:potatoes",
    {"en": "Potatoes", "nb": "Poteter"},
    synonyms={"en": ["potato"]},
    parents=[_PARENT_NODE],
)
_SAMPLE_TAXONOMY = _make_taxonomy([_PARENT_NODE, _CHILD_NODE])


def test_off_lookup_by_exact_label() -> None:
    """Exact label match returns the correct concept with off: URI."""
    from tingbok.services import off as off_service

    with patch("tingbok.services.off._get_taxonomy", return_value=_SAMPLE_TAXONOMY):
        with patch("tingbok.services.off._label_index", None):
            result = off_service.lookup_concept("Potatoes", "en")

    assert result is not None
    assert result["uri"] == "off:en:potatoes"
    assert result["prefLabel"] == "Potatoes"
    assert result["source"] == "off"


def test_off_lookup_case_insensitive() -> None:
    """Lookup is case-insensitive."""
    from tingbok.services import off as off_service

    with patch("tingbok.services.off._get_taxonomy", return_value=_SAMPLE_TAXONOMY):
        with patch("tingbok.services.off._label_index", None):
            result = off_service.lookup_concept("potatoes", "en")

    assert result is not None
    assert result["uri"] == "off:en:potatoes"


def test_off_lookup_synonym() -> None:
    """Lookup via synonym (e.g. 'potato' → 'en:potatoes') works."""
    from tingbok.services import off as off_service

    with patch("tingbok.services.off._get_taxonomy", return_value=_SAMPLE_TAXONOMY):
        with patch("tingbok.services.off._label_index", None):
            result = off_service.lookup_concept("potato", "en")

    assert result is not None
    assert result["uri"] == "off:en:potatoes"


def test_off_lookup_variation() -> None:
    """Singular/plural variations are tried when exact match fails."""
    from tingbok.services import off as off_service

    with patch("tingbok.services.off._get_taxonomy", return_value=_SAMPLE_TAXONOMY):
        with patch("tingbok.services.off._label_index", None):
            # "potato" is a synonym but also reachable via variation from "potatoes"
            result = off_service.lookup_concept("potato", "en")

    assert result is not None


def test_off_lookup_unknown_label_returns_none() -> None:
    """Unknown label returns None."""
    from tingbok.services import off as off_service

    with patch("tingbok.services.off._get_taxonomy", return_value=_SAMPLE_TAXONOMY):
        with patch("tingbok.services.off._label_index", None):
            result = off_service.lookup_concept("Electronics", "en")

    assert result is None


def test_off_lookup_includes_broader() -> None:
    """Result includes broader parents."""
    from tingbok.services import off as off_service

    with patch("tingbok.services.off._get_taxonomy", return_value=_SAMPLE_TAXONOMY):
        with patch("tingbok.services.off._label_index", None):
            result = off_service.lookup_concept("Potatoes", "en")

    assert result is not None
    broader = result.get("broader", [])
    assert len(broader) == 1
    assert broader[0]["uri"] == "off:en:food"
    assert broader[0]["label"] == "Food"


def test_off_lookup_returns_none_when_package_missing() -> None:
    """Returns None gracefully when openfoodfacts is not installed."""
    from tingbok.services import off as off_service

    with patch("tingbok.services.off._get_taxonomy", return_value=None):
        result = off_service.lookup_concept("Potatoes", "en")

    assert result is None


def test_off_lookup_non_food_label_returns_none() -> None:
    """OFF is food-only; non-food labels should not match."""
    from tingbok.services import off as off_service

    with patch("tingbok.services.off._get_taxonomy", return_value=_SAMPLE_TAXONOMY):
        with patch("tingbok.services.off._label_index", None):
            result = off_service.lookup_concept("Hammer", "en")

    assert result is None


def test_off_lookup_result_is_written_to_cache(tmp_path: Path) -> None:
    """A found concept is written to the file cache for future lookups."""

    from tingbok.services import off as off_service

    with patch("tingbok.services.off._get_taxonomy", return_value=_SAMPLE_TAXONOMY):
        with patch("tingbok.services.off._label_index", None):
            result = off_service.lookup_concept("Potatoes", "en", tmp_path)

    assert result is not None
    # A cache file should have been created
    cache_files = list(tmp_path.glob("concept_off_*.json"))
    assert len(cache_files) == 1


def test_off_lookup_not_found_is_cached(tmp_path: Path) -> None:
    """A not-found result is added to the not-found cache."""

    from tingbok.services import off as off_service

    with patch("tingbok.services.off._get_taxonomy", return_value=_SAMPLE_TAXONOMY):
        with patch("tingbok.services.off._label_index", None):
            result = off_service.lookup_concept("Hammer", "en", tmp_path)

    assert result is None
    not_found_file = tmp_path / "_not_found.json"
    assert not_found_file.exists()


def test_off_lookup_served_from_cache(tmp_path: Path) -> None:
    """Second lookup is served from cache without touching the taxonomy."""

    from tingbok.services import off as off_service

    # Prime the cache
    with patch("tingbok.services.off._get_taxonomy", return_value=_SAMPLE_TAXONOMY):
        with patch("tingbok.services.off._label_index", None):
            off_service.lookup_concept("Potatoes", "en", tmp_path)

    # Second call — taxonomy should not be consulted
    with patch("tingbok.services.off._get_taxonomy") as mock_get:
        result = off_service.lookup_concept("Potatoes", "en", tmp_path)

    mock_get.assert_not_called()
    assert result is not None
    assert result["uri"] == "off:en:potatoes"


# ---------------------------------------------------------------------------
# Tests for get_labels()
# ---------------------------------------------------------------------------


def test_off_get_labels_returns_names() -> None:
    """get_labels returns node names in requested languages."""
    from tingbok.services import off as off_service

    with patch("tingbok.services.off._get_taxonomy", return_value=_SAMPLE_TAXONOMY):
        result = off_service.get_labels("off:en:potatoes", ["en", "nb"])

    assert result == {"en": "Potatoes", "nb": "Poteter"}


def test_off_get_labels_partial_languages() -> None:
    """get_labels returns only the languages that exist in the node names."""
    from tingbok.services import off as off_service

    with patch("tingbok.services.off._get_taxonomy", return_value=_SAMPLE_TAXONOMY):
        result = off_service.get_labels("off:en:potatoes", ["en", "zh", "ar"])

    assert result == {"en": "Potatoes"}
    assert "zh" not in result


def test_off_get_labels_unknown_node() -> None:
    """get_labels returns empty dict for unknown node IDs."""
    from tingbok.services import off as off_service

    with patch("tingbok.services.off._get_taxonomy", return_value=_SAMPLE_TAXONOMY):
        result = off_service.get_labels("off:en:nonexistent", ["en"])

    assert result == {}


def test_off_get_labels_non_off_uri() -> None:
    """get_labels returns empty dict for URIs that are not off: scheme."""
    from tingbok.services import off as off_service

    result = off_service.get_labels("http://dbpedia.org/resource/Potato", ["en"])
    assert result == {}


def test_off_get_labels_taxonomy_unavailable() -> None:
    """get_labels returns empty dict when taxonomy cannot be loaded."""
    from tingbok.services import off as off_service

    with patch("tingbok.services.off._get_taxonomy", return_value=None):
        result = off_service.get_labels("off:en:potatoes", ["en"])

    assert result == {}


# ---------------------------------------------------------------------------
# broader_graph — walking OFF parents up to concepts the caller knows
# ---------------------------------------------------------------------------

# Mirrors the real OFF chain: peeled → canned tomatoes → canned tomato products,
# with side branches that do and do not lead anywhere the vocabulary knows.
_PLANT = _make_node("en:plant-based-foods", {"en": "Plant-based foods"})
_CANNED_FOODS = _make_node("en:canned-foods", {"en": "Canned foods"})
_VEG = _make_node("en:vegetables", {"en": "Vegetables"}, parents=[_PLANT])
_TOMATOES = _make_node("en:tomatoes", {"en": "Tomatoes"}, parents=[_VEG])
_TOMATO_PRODUCTS = _make_node("en:tomatoes-and-their-products", {"en": "Tomatoes and their products"}, parents=[_PLANT])
_CANNED_VEG = _make_node("en:canned-vegetables", {"en": "Canned vegetables"}, parents=[_CANNED_FOODS])
_CANNED_TOMATO_PRODUCTS = _make_node(
    "en:canned-tomato-products", {"en": "Canned tomato products"}, parents=[_TOMATO_PRODUCTS, _CANNED_FOODS]
)
_CANNED_TOMATOES = _make_node(
    "en:canned-tomatoes", {"en": "Canned tomatoes"}, parents=[_CANNED_TOMATO_PRODUCTS, _CANNED_VEG, _TOMATOES]
)
_PEELED = _make_node("en:peeled-tomatoes", {"en": "Peeled tomatoes"}, parents=[_CANNED_TOMATOES])
_TOMATO_TAXONOMY = _make_taxonomy(
    [
        _PLANT,
        _CANNED_FOODS,
        _VEG,
        _TOMATOES,
        _TOMATO_PRODUCTS,
        _CANNED_VEG,
        _CANNED_TOMATO_PRODUCTS,
        _CANNED_TOMATOES,
        _PEELED,
    ]
)


def test_off_broader_graph_stops_at_known_concepts() -> None:
    """The walk goes up until it meets a known URI and does not continue past it."""
    from tingbok.services import off as off_service

    known = {"off:en:canned-tomato-products", "off:en:tomatoes"}
    with patch("tingbok.services.off._get_taxonomy", return_value=_TOMATO_TAXONOMY):
        graph = off_service.broader_graph("off:en:peeled-tomatoes", known.__contains__)

    assert graph["off:en:peeled-tomatoes"]["broader"] == ["off:en:canned-tomatoes"]
    assert graph["off:en:canned-tomatoes"]["label"] == "Canned tomatoes"
    assert set(graph["off:en:canned-tomatoes"]["broader"]) == known
    # Known nodes are terminal: present, but not walked further
    assert graph["off:en:canned-tomato-products"]["broader"] == []
    assert graph["off:en:tomatoes"]["broader"] == []
    assert "off:en:tomatoes-and-their-products" not in graph
    assert "off:en:vegetables" not in graph


def test_off_broader_graph_prunes_branches_without_a_bridge() -> None:
    """A branch that never reaches a known URI (canned-vegetables → canned-foods) is dropped."""
    from tingbok.services import off as off_service

    known = {"off:en:canned-tomato-products"}
    with patch("tingbok.services.off._get_taxonomy", return_value=_TOMATO_TAXONOMY):
        graph = off_service.broader_graph("off:en:peeled-tomatoes", known.__contains__)

    assert graph["off:en:canned-tomatoes"]["broader"] == ["off:en:canned-tomato-products"]
    assert "off:en:canned-vegetables" not in graph
    assert "off:en:canned-foods" not in graph


def test_off_broader_graph_empty_without_bridge() -> None:
    """Nothing known above the node → empty graph, so callers fall back to other sources."""
    from tingbok.services import off as off_service

    with patch("tingbok.services.off._get_taxonomy", return_value=_TOMATO_TAXONOMY):
        assert off_service.broader_graph("off:en:peeled-tomatoes", lambda _uri: False) == {}


def test_off_broader_graph_unavailable_or_unknown() -> None:
    """Missing package, unknown node or non-OFF URI → empty graph, never an exception."""
    from tingbok.services import off as off_service

    with patch("tingbok.services.off._get_taxonomy", return_value=None):
        assert off_service.broader_graph("off:en:peeled-tomatoes", lambda _uri: True) == {}
    with patch("tingbok.services.off._get_taxonomy", return_value=_TOMATO_TAXONOMY):
        assert off_service.broader_graph("off:en:no-such-node", lambda _uri: True) == {}
        assert off_service.broader_graph("https://example.org/x", lambda _uri: True) == {}


def test_off_get_taxonomy_without_download_skips_missing_cache(tmp_path: Path) -> None:
    """allow_download=False must not call get_taxonomy() when the taxonomy file is not cached.

    openfoodfacts' get_taxonomy() downloads the file when it is missing, which
    offline mode must never trigger.
    """
    import sys
    import types

    fake_mod = types.ModuleType("openfoodfacts.taxonomy")
    fake_mod.DEFAULT_CACHE_DIR = tmp_path  # type: ignore[attr-defined]
    fake_mod.get_taxonomy = MagicMock(return_value=_TOMATO_TAXONOMY)  # type: ignore[attr-defined]
    fake_pkg = types.ModuleType("openfoodfacts")
    fake_pkg.taxonomy = fake_mod  # type: ignore[attr-defined]

    with patch.dict(sys.modules, {"openfoodfacts": fake_pkg, "openfoodfacts.taxonomy": fake_mod}):
        with patch("tingbok.services.off._taxonomy", None):
            assert _real_get_taxonomy(allow_download=False) is None
            fake_mod.get_taxonomy.assert_not_called()

            (tmp_path / "category.json").write_text("{}")
            assert _real_get_taxonomy(allow_download=False) is _TOMATO_TAXONOMY
            fake_mod.get_taxonomy.assert_called_once()


def test_off_get_taxonomy_loads_once_under_concurrency(tmp_path: Path) -> None:
    """Parallel first calls (resolve runs one OFF lookup per label in threads) load the taxonomy once."""
    import sys
    import threading
    import time
    import types

    calls: list[int] = []

    def _slow_get_taxonomy(_name: str) -> object:
        calls.append(1)
        time.sleep(0.05)
        return _TOMATO_TAXONOMY

    fake_mod = types.ModuleType("openfoodfacts.taxonomy")
    fake_mod.DEFAULT_CACHE_DIR = tmp_path  # type: ignore[attr-defined]
    fake_mod.get_taxonomy = _slow_get_taxonomy  # type: ignore[attr-defined]
    fake_pkg = types.ModuleType("openfoodfacts")
    fake_pkg.taxonomy = fake_mod  # type: ignore[attr-defined]

    results: list[object] = []
    with patch.dict(sys.modules, {"openfoodfacts": fake_pkg, "openfoodfacts.taxonomy": fake_mod}):
        with patch("tingbok.services.off._taxonomy", None):
            threads = [threading.Thread(target=lambda: results.append(_real_get_taxonomy())) for _ in range(8)]
            for t in threads:
                t.start()
            for t in threads:
                t.join()

    assert len(calls) == 1
    assert all(r is _TOMATO_TAXONOMY for r in results)
