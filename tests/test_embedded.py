"""Tests for the in-process (embedded) entry point.

A client with the tingbok package installed should be able to get an answer
without an HTTP server, so an unreachable tingbok.plann.no is not the end of the
story.  Everything here is read-only: writes belong to the service that owns the
data file.
"""

import pytest

import tingbok.app as app_module
from tingbok import embedded


def test_vocabulary_is_loaded_on_first_use():
    vocab = embedded.get_vocabulary()
    assert vocab
    assert "food" in vocab
    assert vocab["food"]["prefLabel"]


def test_concept():
    concept = embedded.get_concept("food/nuts")
    assert concept is not None
    assert concept["id"] == "food/nuts"


def test_unknown_concept_is_none():
    assert embedded.get_concept("no-such-concept-anywhere") is None


def test_ancestors():
    ancestors = embedded.get_ancestors("food/nuts")
    assert ancestors is not None
    assert "food" in ancestors


def test_ancestors_of_unknown_concept_is_none():
    assert embedded.get_ancestors("no-such-concept-anywhere") is None


def test_sources():
    sources = embedded.get_sources()
    by_name = {s["name"]: s for s in sources}
    assert by_name["off"]["label"] == "OpenFoodFacts"
    assert by_name["tingbok"]["is_self"] is True


def test_resolve_returns_the_requested_labels():
    result = embedded.resolve_vocabulary(["food/nuts"], lang="en")
    assert "concepts" in result
    assert "food/nuts" in result["concepts"]


UNKNOWN_LABEL = "a-label-no-source-has-ever-heard-of"


def test_resolve_offline_does_not_reach_upstream(monkeypatch):
    """An unknown label must not trigger a SKOS lookup when offline is asked for.

    The embedded path exists because the network is unavailable; the whole
    point would be lost if resolving fell through to DBpedia.  Asserting only
    that the response has a ``concepts`` key proves nothing — that is true with
    the flag inverted or removed — so this counts the upstream calls.
    """
    calls: list[tuple] = []

    async def _record(lookup_label, source, lang):
        calls.append((lookup_label, source, lang))
        return None, [], [], {}

    monkeypatch.setattr(app_module, "_fetch_one_skos_source", _record)

    result = embedded.resolve_vocabulary([UNKNOWN_LABEL], lang="en", offline=True)
    assert calls == []
    assert UNKNOWN_LABEL in result["unresolved"]
    assert result["concepts"][UNKNOWN_LABEL]["source_uris"] == []


def test_resolve_without_offline_does_consult_upstream(monkeypatch):
    """The counterpart, so the test above cannot pass for the wrong reason."""
    calls: list[tuple] = []

    async def _record(lookup_label, source, lang):
        calls.append((lookup_label, source, lang))
        return None, [], [], {}

    monkeypatch.setattr(app_module, "_fetch_one_skos_source", _record)

    embedded.resolve_vocabulary([UNKNOWN_LABEL], lang="en", offline=False)
    assert {source for _, source, _ in calls} == {"agrovoc", "dbpedia", "wikidata"}


def test_resolve_offline_still_resolves_what_the_vocabulary_knows():
    result = embedded.resolve_vocabulary(["food/nuts"], lang="en", offline=True)
    assert "food/nuts" in result["concepts"]
    assert "food/nuts" not in result["unresolved"]


def test_calling_from_inside_an_event_loop_raises_without_a_stray_coroutine():
    """A coroutine built as an argument and abandoned emits a RuntimeWarning.

    tingbok's pytest config turns warnings into errors, so a regression here
    fails the suite rather than merely printing.
    """
    import asyncio
    import gc
    import warnings

    async def main():
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            with pytest.raises(RuntimeError, match="running event loop"):
                embedded.resolve_vocabulary(["food"], offline=True)
            gc.collect()
        return [str(w.message) for w in caught]

    assert asyncio.run(main()) == []


def test_first_use_bootstraps_the_vocabulary_into_a_configured_data_dir(tmp_path, monkeypatch: pytest.MonkeyPatch):
    """A TINGBOK_DATA_DIR that has no vocabulary.yaml yet must not be fatal.

    The service's lifespan seeds the data dir from the packaged copy before
    loading it.  The embedded entry point has to do the same: a client reaches
    for it precisely when no service has ever run on that host, so there is
    nobody else to have created the file.
    """
    data_dir = tmp_path / "data"
    monkeypatch.setattr(app_module, "_DATA_BASE", data_dir)
    monkeypatch.setattr(app_module, "VOCABULARY_PATH", data_dir / "vocabulary.yaml")
    monkeypatch.setattr(app_module, "vocabulary", {})

    vocab = embedded.get_vocabulary()

    assert "food" in vocab
    assert (data_dir / "vocabulary.yaml").exists()


def test_first_use_loads_when_the_app_globals_are_empty(monkeypatch: pytest.MonkeyPatch):
    """Cover the load branch itself, which conftest's autouse fixture hides.

    ``_load_vocabulary`` runs for every test in the session, so by the time
    anything here executes ``app_module.vocabulary`` is already populated and
    the ``if not _app.vocabulary`` branch never runs.  Clearing it first is the
    only way this module's own loading path is exercised at all.
    """
    monkeypatch.setattr(app_module, "vocabulary", {})
    monkeypatch.setattr(app_module, "_vocab_uri_index", {})

    vocab = embedded.get_vocabulary()

    assert "food" in vocab
    assert app_module.vocabulary, "the load must have populated the app module's global"
    assert app_module._vocab_uri_index, "the URI index must have been rebuilt alongside it"
