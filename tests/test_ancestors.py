"""Tests for ``GET /api/ancestors/{id}``.

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


def _loaded(tmp_path, yaml_text: str) -> dict:
    """Run *yaml_text* through the real loader, as a served vocabulary would be.

    Hand-building the dict instead skips ``_load_vocabulary``'s path-parent
    inference, which is where hierarchy actually comes from — a test that
    bypasses it proves nothing about what the endpoint serves.
    """
    path = tmp_path / "vocabulary.yaml"
    path.write_text(yaml_text)
    return app_module._load_vocabulary(path)


@pytest.mark.anyio
async def test_a_path_parent_becomes_the_parent(client, monkeypatch, tmp_path) -> None:
    """A concept with no ``broader`` but a path parent is not a root.

    ``epoxy/filler`` in a real inventory had neither, and ended up at the top
    level of the category browser next to ``epoxy/hardener``.  The inference
    that fixes it belongs to the loader, so this goes through the loader.
    """
    vocab = _loaded(
        tmp_path,
        """
concepts:
  widgets:
    prefLabel: Widgets
  widgets/blue:
    prefLabel: Blue widgets
""",
    )
    monkeypatch.setattr(app_module, "vocabulary", vocab)

    response = await client.get("/api/ancestors/widgets/blue")
    assert response.status_code == 200
    assert response.json()["ancestors"] == ["widgets"]


@pytest.mark.anyio
async def test_declared_broader_wins_over_the_path_parent(client, monkeypatch, tmp_path) -> None:
    vocab = _loaded(
        tmp_path,
        """
concepts:
  widgets:
    prefLabel: Widgets
  gadgets:
    prefLabel: Gadgets
  widgets/blue:
    prefLabel: Blue widgets
    broader: gadgets
""",
    )
    monkeypatch.setattr(app_module, "vocabulary", vocab)

    response = await client.get("/api/ancestors/widgets/blue")
    assert response.json()["ancestors"] == ["gadgets"]


def test_parents_are_only_what_the_vocabulary_records(tmp_path) -> None:
    """``_parents_of`` reports declared hierarchy; it does not invent any.

    The loader already turns a path prefix into a ``broader`` entry, so a
    second copy of that rule down here would be dead for anything loaded
    normally — and live only for a concept whose declared parents are all
    dangling, where it would make ``/api/ancestors`` report a parent that
    ``GET /api/vocabulary/{id}`` does not list in ``broader``.
    """
    vocab = {
        "widgets": {"prefLabel": "Widgets"},
        "widgets/blue": {"prefLabel": "Blue widgets"},
        "widgets/red": {"prefLabel": "Red widgets", "broader": ["no-such-concept"]},
    }

    assert app_module._parents_of("widgets/blue", vocab) == []
    assert app_module._parents_of("widgets/red", vocab) == []


def test_resolve_returns_the_concept_and_every_ancestor(monkeypatch) -> None:
    """Characterise what the resolve walk collects, not that it delegates.

    Asserting it against ``ancestors_of`` would be circular now that it calls
    it: the equality would hold however wrong both were.  This pins the
    property callers actually depend on — the concept itself plus the closure
    of its parents, each one a real concept.
    """
    vocab = {
        "food": {"prefLabel": "Food"},
        "food/dairy": {"prefLabel": "Dairy", "broader": ["food"]},
        "food/dairy/cheese": {"prefLabel": "Cheese", "broader": ["food/dairy"]},
    }
    monkeypatch.setattr(app_module, "vocabulary", vocab)

    collected: dict = {}
    app_module._collect_with_ancestors("food/dairy/cheese", collected)

    assert set(collected) == {"food", "food/dairy", "food/dairy/cheese"}
    assert all(c.id in vocab for c in collected.values())


def test_a_null_concept_entry_is_not_a_parent(monkeypatch) -> None:
    """A bare ``food:`` key in the YAML is a concept id with no data.

    ``_load_vocabulary`` and ``_write_vocabulary_concept_update`` both guard for
    it, so it is an anticipated shape rather than a corrupt file.  Membership
    alone is not enough to call such an id a parent: resolving through it used
    to hand ``None`` to ``_vocabulary_concept_from_data`` and 500.
    """
    vocab = {"food": None, "food/nuts": {"prefLabel": "Nuts", "broader": ["food"]}}
    monkeypatch.setattr(app_module, "vocabulary", vocab)

    assert app_module._parents_of("food/nuts", vocab) == []
    assert app_module.ancestors_of("food/nuts", vocab) == []

    collected: dict = {}
    app_module._collect_with_ancestors("food/nuts", collected)
    assert set(collected) == {"food/nuts"}


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
async def test_an_url_encoded_id_resolves(client, monkeypatch, tmp_path) -> None:
    vocab = _loaded(
        tmp_path,
        """
concepts:
  smør:
    prefLabel: Smør
  smør/brunost:
    prefLabel: Brunost
""",
    )
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
