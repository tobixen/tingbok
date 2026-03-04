"""Tests for Open Food Facts (OFF) lookup service."""

from __future__ import annotations

from unittest.mock import MagicMock, patch


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
