"""Category hierarchy from the Open Food Facts taxonomy in resolve and lookup.

The OFF taxonomy is mocked (see ``tests/test_off.py``); no network.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from unittest.mock import patch

import pytest

import tingbok.app as app_module
from tests.test_off import (
    _CANNED_FOODS,
    _CANNED_TOMATO_PRODUCTS,
    _CANNED_TOMATOES,
    _CANNED_VEG,
    _PEELED,
    _PLANT,
    _TOMATO_PRODUCTS,
    _TOMATOES,
    _VEG,
    _make_node,
    _make_taxonomy,
)

_PASTES = _make_node(
    "en:tomato-pastes",
    {"en": "Tomato pastes"},
    synonyms={"en": ["tomato paste"]},
    parents=[_CANNED_TOMATO_PRODUCTS],
)
_STRAINED = _make_node(
    "en:strained-tomatoes",
    {"en": "Strained tomatoes", "fr": "Tomates passées"},
    parents=[_CANNED_TOMATO_PRODUCTS],
)
_ORPHAN = _make_node("en:canned-mystery", {"en": "Canned mystery"}, parents=[_CANNED_FOODS])
_TAXONOMY = _make_taxonomy(
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
        _PASTES,
        _STRAINED,
        _ORPHAN,
    ]
)

#: Vocabulary entries whose place OFF also knows.  They stay in vocabulary.yaml
#: for installs without the ``off`` extra and for their bg/de labels; these
#: tests drop them to show OFF alone gets the hierarchy right.
_MIRRORED = ("canned-tomatoes", "peeled-tomatoes", "chopped-tomatoes", "tomato-paste")


@pytest.fixture
def without_mirrored_tomatoes() -> Iterator[None]:
    """Drop the vocabulary entries that duplicate OFF's canned-tomato subtree."""
    saved = {cid: app_module.vocabulary.pop(cid) for cid in _MIRRORED if cid in app_module.vocabulary}
    app_module._vocab_uri_index = app_module._build_vocab_uri_index(app_module.vocabulary)
    app_module._category_index = None
    try:
        yield
    finally:
        app_module.vocabulary.update(saved)
        app_module._vocab_uri_index = app_module._build_vocab_uri_index(app_module.vocabulary)
        app_module._category_index = None


@pytest.fixture
def off_taxonomy(skos_cache_dir: Path) -> Iterator[None]:
    """Mock OFF taxonomy; no SKOS source finds anything."""
    with (
        patch("tingbok.services.off._get_taxonomy", return_value=_TAXONOMY),
        patch("tingbok.services.off._label_index", None),
        patch("tingbok.app.skos_service.lookup_concept", return_value=None),
        patch("tingbok.app.gpt_service.lookup_concept", return_value=None),
    ):
        yield


async def _resolve(client, labels: list[str], **extra: object) -> dict:
    response = await client.post("/api/vocabulary/resolve", json={"labels": labels, "lang": "en", **extra})
    assert response.status_code == 200
    return response.json()


@pytest.mark.anyio
@pytest.mark.usefixtures("without_mirrored_tomatoes", "off_taxonomy")
async def test_resolve_takes_hierarchy_from_off(client) -> None:
    """peeled-tomatoes → canned-tomatoes (from OFF) → canned-tomato-products/tomatoes (vocabulary)."""
    data = await _resolve(client, ["peeled-tomatoes"])
    concepts = data["concepts"]

    assert "peeled-tomatoes" not in data["unresolved"]
    peeled = concepts["peeled-tomatoes"]
    assert "off:en:peeled-tomatoes" in peeled["source_uris"]
    assert peeled["broader"] == ["canned-tomatoes"]

    canned = concepts["canned-tomatoes"]
    assert canned["source_uris"] == ["off:en:canned-tomatoes"]
    assert canned["prefLabel"] == "Canned tomatoes"
    # OFF also lists en:tomatoes as a parent, but that is a vocabulary ancestor
    # of canned-tomato-products already, so it is redundant
    assert canned["broader"] == ["canned-tomato-products"]

    # Vocabulary ancestors of the bridge are included
    assert "canned-tomato-products" in concepts
    assert "preserved-vegetables" in concepts
    # Branches that never reach the vocabulary are not emitted
    assert "canned-vegetables" not in concepts
    assert "canned-foods" not in concepts


@pytest.mark.anyio
@pytest.mark.usefixtures("without_mirrored_tomatoes", "off_taxonomy")
async def test_resolve_off_synonym_bridges_directly(client) -> None:
    """tomato-paste matches OFF's en:tomato-pastes, whose parent is in the vocabulary."""
    data = await _resolve(client, ["tomato-paste"])
    paste = data["concepts"]["tomato-paste"]
    assert "off:en:tomato-pastes" in paste["source_uris"]
    assert paste["broader"] == ["canned-tomato-products"]


@pytest.mark.anyio
@pytest.mark.usefixtures("without_mirrored_tomatoes", "off_taxonomy")
async def test_resolve_off_chain_replaces_junk_ontology_paths(client) -> None:
    """When OFF bridges into the vocabulary, deep SKOS ontology paths are not used for broader."""
    junk = (
        {"uri": "https://www.wikidata.org/entity/Q1", "prefLabel": "tomato paste"},
        ["good/non_durable_goods/food_paste/tomato_paste"],
        ["https://www.wikidata.org/entity/Q1"],
        {"good/non_durable_goods/food_paste": "https://www.wikidata.org/entity/Q2"},
    )

    async def _fake_fetch(lookup_label: str, source: str, lang: str):
        return junk if source == "wikidata" else (None, [], [], {})

    with patch("tingbok.app._fetch_one_skos_source", side_effect=_fake_fetch):
        data = await _resolve(client, ["tomato-paste"])

    concepts = data["concepts"]
    paste = concepts["tomato-paste"]
    assert paste["broader"] == ["canned-tomato-products"]
    # The SKOS source URI is still recorded
    assert "https://www.wikidata.org/entity/Q1" in paste["source_uris"]
    assert "off:en:tomato-pastes" in paste["source_uris"]
    assert "food-paste" not in concepts
    assert not any(cid.startswith("good") for cid in concepts)


@pytest.mark.anyio
@pytest.mark.usefixtures("off_taxonomy")
async def test_resolve_off_without_bridge_stays_unresolved(client) -> None:
    """An OFF node with no route into the vocabulary stays unresolved (so clients retry
    later), but carries its OFF URI and gets no invented parents."""
    data = await _resolve(client, ["canned-mystery"])
    mystery = data["concepts"]["canned-mystery"]
    assert "canned-mystery" in data["unresolved"]
    assert mystery["source_uris"] == ["off:en:canned-mystery"]
    assert mystery["broader"] == []
    assert "canned-foods" not in data["concepts"]


@pytest.mark.anyio
@pytest.mark.usefixtures("off_taxonomy")
async def test_resolve_off_node_in_vocabulary_folds_into_concept(client) -> None:
    """A label OFF maps to a node the vocabulary already has (en:strained-tomatoes → passata)
    resolves to that concept, with the input recorded as an altLabel."""
    data = await _resolve(client, ["tomates-passées"])
    assert "tomates-passées" not in data["unresolved"]
    assert "tomates-passées" not in data["concepts"]
    assert "tomates-passées" in data["concepts"]["passata"]["altLabel"].get("en", [])


@pytest.mark.anyio
@pytest.mark.usefixtures("without_mirrored_tomatoes", "off_taxonomy")
async def test_resolve_batch_sibling_is_used_as_off_parent(client) -> None:
    """When the OFF parent is itself a label in the batch, that concept is the parent."""
    data = await _resolve(client, ["peeled-tomatoes", "canned-tomatoes"])
    concepts = data["concepts"]
    assert concepts["peeled-tomatoes"]["broader"] == ["canned-tomatoes"]
    assert concepts["canned-tomatoes"]["broader"] == ["canned-tomato-products"]
    assert data["unresolved"] == []


@pytest.mark.anyio
@pytest.mark.usefixtures("without_mirrored_tomatoes", "skos_cache_dir")
async def test_resolve_offline_uses_off_without_download(client) -> None:
    """offline: true still uses a locally available OFF taxonomy, but never lets it download."""
    with (
        patch("tingbok.services.off._get_taxonomy", return_value=_TAXONOMY) as get_tax,
        patch("tingbok.services.off._label_index", None),
        patch("tingbok.app.skos_service.lookup_concept") as skos_lookup,
    ):
        data = await _resolve(client, ["peeled-tomatoes"], offline=True)

    skos_lookup.assert_not_called()
    assert get_tax.call_count >= 1
    for call in get_tax.call_args_list:
        assert call.kwargs.get("allow_download") is False
    assert data["concepts"]["peeled-tomatoes"]["broader"] == ["canned-tomatoes"]


@pytest.mark.anyio
@pytest.mark.usefixtures("without_mirrored_tomatoes", "skos_cache_dir")
async def test_resolve_without_off_package_degrades(client) -> None:
    """No openfoodfacts package → unknown label stays an unresolved stub, as before."""
    with (
        patch("tingbok.services.off._get_taxonomy", return_value=None),
        patch("tingbok.app.skos_service.lookup_concept", return_value=None),
    ):
        data = await _resolve(client, ["peeled-tomatoes"])
    assert data["unresolved"] == ["peeled-tomatoes"]


@pytest.mark.anyio
@pytest.mark.usefixtures("without_mirrored_tomatoes", "off_taxonomy")
async def test_lookup_takes_broader_from_off(client) -> None:
    """GET /api/lookup uses the OFF chain for broader as well."""
    response = await client.get("/api/lookup/peeled-tomatoes")
    assert response.status_code == 200
    data = response.json()
    assert "off:en:peeled-tomatoes" in data["source_uris"]
    # Lookup emits no stubs, so broader skips the OFF-only canned-tomatoes
    # node and names the nearest vocabulary concept — an id the client can fetch.
    assert data["broader"] == ["canned-tomato-products"]


@pytest.mark.anyio
@pytest.mark.usefixtures("off_taxonomy")
async def test_resolve_mirrored_entries_still_resolve_from_vocabulary(client) -> None:
    """With the vocabulary entries present, they win (vocabulary hit, no OFF needed)."""
    data = await _resolve(client, ["peeled-tomatoes"])
    assert data["concepts"]["peeled-tomatoes"]["broader"] == ["canned-tomatoes"]
    assert data["concepts"]["canned-tomatoes"]["broader"] == ["canned-tomato-products"]


@pytest.fixture
def gizmos_without_off_uri() -> Iterator[None]:
    """A vocabulary concept whose id equals an OFF node id but which does not list that OFF URI."""
    app_module.vocabulary["gizmos"] = {"prefLabel": "Gizmos", "broader": "food", "source_uris": []}
    app_module._vocab_uri_index = app_module._build_vocab_uri_index(app_module.vocabulary)
    app_module._category_index = None
    try:
        yield
    finally:
        app_module.vocabulary.pop("gizmos", None)
        app_module._vocab_uri_index = app_module._build_vocab_uri_index(app_module.vocabulary)
        app_module._category_index = None


_GIZMOS = _make_node("en:gizmos", {"en": "Gizmos"}, parents=[_PLANT])
_GIZMO_BITS = _make_node("en:gizmo-bits", {"en": "Gizmo bits"}, parents=[_GIZMOS])


@pytest.mark.anyio
@pytest.mark.usefixtures("gizmos_without_off_uri", "skos_cache_dir")
async def test_resolve_bridges_only_by_declared_off_uri(client) -> None:
    """A same-named vocabulary id is not a bridge: only a concept listing the off: URI is."""
    with (
        patch("tingbok.services.off._get_taxonomy", return_value=_make_taxonomy([_PLANT, _GIZMOS, _GIZMO_BITS])),
        patch("tingbok.services.off._label_index", None),
        patch("tingbok.app.skos_service.lookup_concept", return_value=None),
        patch("tingbok.app.gpt_service.lookup_concept", return_value=None),
    ):
        data = await _resolve(client, ["gizmo-bits"])
    assert "gizmo-bits" in data["unresolved"]
    assert data["concepts"]["gizmo-bits"]["broader"] == []
