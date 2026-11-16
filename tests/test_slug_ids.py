"""Concept ids are slugs; a path prefix is used only to tell apart concepts that share a slug."""

from __future__ import annotations

from unittest.mock import patch

import pytest

from tingbok.app import _slug_id

_WD = "https://www.wikidata.org/entity/"


def test_slug_is_last_segment() -> None:
    assert _slug_id("materials/substances/frying_oil", None) == "frying-oil"


def test_uri_bridging_into_vocabulary_wins() -> None:
    """A segment whose URI a vocabulary concept lists is that concept."""
    with patch.dict("tingbok.app._vocab_uri_index", {_WD + "Q1": "food/condiments"}):
        assert _slug_id("entity/whatever/sauces", _WD + "Q1") == "food/condiments"


def test_slug_matching_vocabulary_concept_under_same_root_is_that_concept() -> None:
    """food/…/condiments is the vocabulary's food/condiments."""
    assert _slug_id("food/condiments", None) == "food/condiments"


def test_slug_matching_vocabulary_concept_is_number_insensitive() -> None:
    assert _slug_id("food/seeds/nut", None) == "food/nuts"


def test_slug_colliding_with_concept_under_other_root_keeps_its_root() -> None:
    """A nut under tools is not hardware/nut, so it is spelled out as tools/nut."""
    assert _slug_id("tools/fasteners/nut", None) == "tools/nut"


@pytest.mark.anyio
async def test_resolve_broader_is_direct_parent_slug(client) -> None:
    """sunflower-oil's parent is cooking-oil (bridged by URI), not a path string."""
    from tests.test_app import _skos_hierarchy_mock, _skos_lookup_mock

    with (
        patch("tingbok.app.skos_service.lookup_concept", side_effect=_skos_lookup_mock),
        patch("tingbok.app.skos_service.build_hierarchy_paths", side_effect=_skos_hierarchy_mock),
        patch("tingbok.app.gpt_service.lookup_concept", return_value=None),
        patch("tingbok.app._lookup_off", return_value=(None, {})),
    ):
        response = await client.post("/api/vocabulary/resolve", json={"labels": ["sunflower-oil"], "lang": "en"})
    concepts = response.json()["concepts"]
    assert concepts["sunflower-oil"]["broader"] == ["cooking-oil"]
    assert not any("/" in b and b not in concepts for c in concepts.values() for b in c["broader"])


# --- review fixes ------------------------------------------------------------

import tingbok.app as app_module  # noqa: E402

_Q_OIL = _WD + "Q427457"


def _sources(per_source: dict[str, tuple[list[str], dict[str, str]]]):
    """A ``_fetch_one_skos_source`` fake: ``{source: (paths, uri_map)}``."""

    async def _fake(lookup_label: str, source: str, lang: str):
        if source not in per_source:
            return None, [], [], {}
        paths, uri_map = per_source[source]
        return {"uri": f"https://{source}.example.com/x", "prefLabel": lookup_label}, paths, [], uri_map

    return _fake


def _quiet():
    return (
        patch("tingbok.app.gpt_service.lookup_concept", return_value=None),
        patch("tingbok.app._lookup_off", return_value=(None, {})),
        patch("tingbok.app.skos_service.get_labels", return_value={}),
        patch("tingbok.app.skos_service.get_alt_labels", return_value={}),
        patch("tingbok.app.skos_service.get_description", return_value=None),
    )


async def _resolve(client, label: str, fake) -> dict:
    a, b, c, d, e = _quiet()
    with a, b, c, d, e, patch("tingbok.app._fetch_one_skos_source", side_effect=fake):
        response = await client.post("/api/vocabulary/resolve", json={"labels": [label], "lang": "en"})
    assert response.status_code == 200
    return response.json()


async def _lookup(client, label: str, fake, gpt=None) -> dict:
    a, b, c, d, e = _quiet()
    with a, b, c, d, e, patch("tingbok.app._fetch_one_skos_source", side_effect=fake):
        if gpt is not None:
            with (
                patch("tingbok.app.gpt_service.lookup_concept", return_value=gpt),
                patch("tingbok.app.gpt_service.get_labels", return_value={}),
            ):
                response = await client.get(f"/api/lookup/{label}")
        else:
            response = await client.get(f"/api/lookup/{label}")
    assert response.status_code == 200
    return response.json()


def _assert_closed(concepts: dict) -> None:
    """Every parent named in a resolve response is a concept in it, and nothing is its own parent."""
    for cid, concept in concepts.items():
        for parent in concept["broader"]:
            assert parent in concepts, f"{cid} -> {parent} has no concept"
            assert parent != cid


_WALNUT = {
    "agrovoc": (["food/plant_products/walnut_oil"], {}),
    "wikidata": (["entity/substance/oil/cooking_oil/walnut_oil"], {"entity/substance/oil/cooking_oil": _Q_OIL}),
}


@pytest.mark.anyio
async def test_resolve_keeps_a_non_root_path_cut_at_its_bridged_parent(client) -> None:
    """A Wikidata path ending in upper ontologies still gives its URI-bridged parent."""
    with patch.dict("tingbok.app._vocab_uri_index", {_Q_OIL: "food/condiments"}):
        data = await _resolve(client, "walnut-oil", _sources(_WALNUT))
    broader = data["concepts"]["walnut-oil"]["broader"]
    assert "food/condiments" in broader
    assert "plant-products" in broader
    _assert_closed(data["concepts"])


@pytest.mark.anyio
async def test_lookup_keeps_a_non_root_path_cut_at_its_bridged_parent(client) -> None:
    with patch.dict("tingbok.app._vocab_uri_index", {_Q_OIL: "food/condiments"}):
        data = await _lookup(client, "walnut-oil", _sources(_WALNUT))
    assert "food/condiments" in data["broader"]


@pytest.mark.anyio
async def test_resolve_does_not_stub_over_a_vocabulary_concept(client) -> None:
    """A path segment that is a vocabulary concept by slug comes with its vocabulary data."""
    data = await _resolve(client, "x-widget", _sources({"wikidata": (["food/condiments/oilz/x_widget"], {})}))
    concepts = data["concepts"]
    assert concepts["food/condiments"]["prefLabel"] == app_module.vocabulary["food/condiments"]["prefLabel"]
    assert concepts["oilz"]["broader"] == ["food/condiments"]
    _assert_closed(concepts)


@pytest.mark.anyio
async def test_resolve_never_makes_a_segment_its_own_parent(client) -> None:
    data = await _resolve(client, "x-kernel", _sources({"wikidata": (["food/nut/nuts/x_kernel"], {})}))
    _assert_closed(data["concepts"])
    assert data["concepts"]["x-kernel"]["broader"] == ["food/nuts"]


@pytest.mark.anyio
async def test_resolve_stubs_uri_less_and_gpt_segments(client) -> None:
    gpt = {
        "uri": "gpt:9",
        "prefLabel": "Widget",
        "source": "gpt",
        "broader": [],
        "path_parts": ["Home & Garden", "Kitchen & Dining", "Widget"],
    }
    a, b, c, d, e = _quiet()
    with (
        a,
        b,
        c,
        d,
        e,
        patch("tingbok.app._fetch_one_skos_source", side_effect=_sources({"wikidata": (["food/pastes/widget"], {})})),
    ):
        with patch("tingbok.app.gpt_service.lookup_concept", return_value=gpt):
            response = await client.post("/api/vocabulary/resolve", json={"labels": ["widget"], "lang": "en"})
    concepts = response.json()["concepts"]
    assert "kitchen-and-dining" in concepts
    assert "pastes" in concepts
    _assert_closed(concepts)


@pytest.mark.anyio
async def test_lookup_found_only_in_gpt_has_a_slug_id(client) -> None:
    gpt = {
        "uri": "gpt:9",
        "prefLabel": "Widgetq",
        "source": "gpt",
        "broader": [],
        "path_parts": ["Home & Garden", "Kitchen & Dining", "Widgetq"],
    }
    data = await _lookup(client, "widgetq", _sources({}), gpt=gpt)
    assert data["id"] == "widgetq"


@pytest.mark.anyio
async def test_lookup_whose_slug_is_a_vocabulary_concept_returns_that_concept(client) -> None:
    """A foreign label whose SKOS concept is 'hammer' is the vocabulary's hammer, not a SKOS copy."""
    data = await _lookup(client, "zhammerz", _sources({"wikidata": (["tools/hand_tools/hammer"], {})}))
    assert data["id"] == "hammer"
    assert data["broader"] == ["tools/hand"]
    assert "zhammerz" in [a for alts in data["altLabel"].values() for a in alts]


@pytest.mark.anyio
async def test_lookup_id_does_not_come_from_an_upper_ontology_path(client) -> None:
    data = await _lookup(client, "nut", _sources({"wikidata": (["entity/stuff/nut", "pets/stuff/nut"], {})}))
    assert not data["id"].startswith("entity")


@pytest.mark.anyio
async def test_lookup_adds_nearest_vocabulary_ancestor(client) -> None:
    """Lookup emits no stubs, so a concept under a source-only parent also names its vocabulary anchor."""
    data = await _lookup(client, "walnut-oil", _sources({"wikidata": (["food/condiments/foo_oils/walnut_oil"], {})}))
    assert "foo-oils" in data["broader"]
    assert "food/condiments" in data["broader"]


def test_organisms_is_not_an_inventory_root() -> None:
    assert "organisms" not in app_module._inventory_roots()


def test_slug_index_follows_a_same_length_vocabulary_edit() -> None:
    vocab = app_module.vocabulary
    saved = vocab.pop("hammer")
    vocab["sledgehammer-x"] = saved
    try:
        app_module._vocab_uri_index = app_module._build_vocab_uri_index(vocab)
        index = app_module._vocab_slug_index()
        assert "sledgehammer-x" in {c for ids in index.values() for c in ids}
        assert "hammer" not in {c for ids in index.values() for c in ids}
    finally:
        del vocab["sledgehammer-x"]
        vocab["hammer"] = saved
        app_module._vocab_uri_index = app_module._build_vocab_uri_index(vocab)
