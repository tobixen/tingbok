"""Tests for ``GET /api/vocabulary/{id}/ancestors``.

The flat concept list served by ``/api/vocabulary`` made every client
re-implement hierarchy walking.  This endpoint gives "is soybeans food?" a
single authoritative answer.
"""

import pytest

import tingbok.app as app_module


@pytest.mark.anyio
async def test_ancestors_of_a_nested_concept(client) -> None:
    response = await client.get("/api/ancestors/food/nuts")
    assert response.status_code == 200
    data = response.json()
    assert data["id"] == "food/nuts"
    # food/nuts declares broader: food/snacks, which is itself under food.
    assert "food/snacks" in data["ancestors"]
    assert "food" in data["ancestors"]
    # Nearest first.
    assert data["ancestors"].index("food/snacks") < data["ancestors"].index("food")


@pytest.mark.anyio
async def test_ancestors_excludes_self(client) -> None:
    response = await client.get("/api/ancestors/food/nuts")
    assert "food/nuts" not in response.json()["ancestors"]


@pytest.mark.anyio
async def test_ancestors_of_a_root_is_empty(client) -> None:
    response = await client.get("/api/ancestors/food")
    assert response.status_code == 200
    assert response.json()["ancestors"] == []


@pytest.mark.anyio
async def test_ancestors_of_unknown_concept_is_404(client) -> None:
    response = await client.get("/api/ancestors/no-such-concept")
    assert response.status_code == 404


@pytest.mark.anyio
async def test_ancestors_falls_back_to_the_path_parent(client, monkeypatch) -> None:
    """A concept with no ``broader`` but a path parent in the vocabulary is not a root.

    ``epoxy/filler`` in a real inventory had neither, and ended up at the top
    level of the category browser next to ``epoxy/hardener``.
    """
    vocab = dict(app_module.vocabulary)
    vocab["widgets"] = {"prefLabel": "Widgets"}
    vocab["widgets/blue"] = {"prefLabel": "Blue widgets"}
    monkeypatch.setattr(app_module, "vocabulary", vocab)

    response = await client.get("/api/ancestors/widgets/blue")
    assert response.status_code == 200
    assert response.json()["ancestors"] == ["widgets"]


@pytest.mark.anyio
async def test_declared_broader_wins_over_the_path_parent(client, monkeypatch) -> None:
    vocab = dict(app_module.vocabulary)
    vocab["widgets"] = {"prefLabel": "Widgets"}
    vocab["gadgets"] = {"prefLabel": "Gadgets"}
    vocab["widgets/blue"] = {"prefLabel": "Blue widgets", "broader": "gadgets"}
    monkeypatch.setattr(app_module, "vocabulary", vocab)

    response = await client.get("/api/ancestors/widgets/blue")
    assert response.json()["ancestors"] == ["gadgets"]


@pytest.mark.anyio
async def test_ancestors_survives_a_cycle(client, monkeypatch) -> None:
    """Upstream SKOS data contains contradictory broader/narrower pairs."""
    vocab = dict(app_module.vocabulary)
    vocab["loop-a"] = {"prefLabel": "A", "broader": "loop-b"}
    vocab["loop-b"] = {"prefLabel": "B", "broader": "loop-a"}
    monkeypatch.setattr(app_module, "vocabulary", vocab)

    response = await client.get("/api/ancestors/loop-a")
    assert response.status_code == 200
    assert response.json()["ancestors"] == ["loop-b"]


@pytest.mark.anyio
async def test_multiple_parents_are_all_reported(client, monkeypatch) -> None:
    vocab = dict(app_module.vocabulary)
    vocab["staples"] = {"prefLabel": "Staples"}
    vocab["vegetables"] = {"prefLabel": "Vegetables"}
    vocab["potatoes"] = {"prefLabel": "Potatoes", "broader": ["staples", "vegetables"]}
    monkeypatch.setattr(app_module, "vocabulary", vocab)

    response = await client.get("/api/ancestors/potatoes")
    assert sorted(response.json()["ancestors"]) == ["staples", "vegetables"]


@pytest.mark.anyio
async def test_a_concept_named_ancestors_is_reachable_and_so_are_its_ancestors(client, monkeypatch) -> None:
    """The route lives in its own namespace, so nothing shadows anything.

    Under the old ``/api/vocabulary/{id}/ancestors`` shape these two collided:
    the concept won, and there was then no URL at all that gave the ancestors
    of ``genealogy`` — while the route's declared response_model said otherwise.
    """
    vocab = dict(app_module.vocabulary)
    vocab["genealogy"] = {"prefLabel": "Genealogy"}
    vocab["genealogy/ancestors"] = {"prefLabel": "Ancestors", "broader": "genealogy"}
    monkeypatch.setattr(app_module, "vocabulary", vocab)

    concept = await client.get("/api/vocabulary/genealogy/ancestors")
    assert concept.status_code == 200
    assert concept.json()["prefLabel"] == "Ancestors"

    ancestors = await client.get("/api/ancestors/genealogy")
    assert ancestors.status_code == 200
    assert ancestors.json() == {"id": "genealogy", "ancestors": []}

    nested = await client.get("/api/ancestors/genealogy/ancestors")
    assert nested.status_code == 200
    assert nested.json() == {"id": "genealogy/ancestors", "ancestors": ["genealogy"]}


@pytest.mark.anyio
async def test_a_trailing_slash_is_tolerated(client) -> None:
    """A concept id never ends in a slash, so this can only be a typo."""
    response = await client.get("/api/ancestors/food/nuts/")
    assert response.status_code == 200
    assert response.json()["id"] == "food/nuts"


@pytest.mark.anyio
async def test_an_url_encoded_id_resolves(client, monkeypatch) -> None:
    vocab = dict(app_module.vocabulary)
    vocab["smør"] = {"prefLabel": "Smør"}
    vocab["smør/brunost"] = {"prefLabel": "Brunost"}
    monkeypatch.setattr(app_module, "vocabulary", vocab)

    response = await client.get("/api/ancestors/sm%C3%B8r/brunost")
    assert response.status_code == 200
    assert response.json()["ancestors"] == ["smør"]


@pytest.mark.anyio
async def test_the_endpoint_and_the_embedded_helper_agree(client) -> None:
    """One BFS, two entry points — they must not drift apart."""
    from tingbok import embedded

    for concept_id in ("food/nuts", "food", "food/snacks"):
        over_http = (await client.get(f"/api/ancestors/{concept_id}")).json()["ancestors"]
        assert over_http == embedded.get_ancestors(concept_id)
