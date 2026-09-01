"""In-process access to tingbok's vocabulary, for clients that have it installed.

A client normally talks to a running tingbok over HTTP.  When ``tingbok.plann.no``
does not answer, a client that happens to have this package installed should get
an answer anyway rather than degrading — that is what this module is for.  It is
the supported entry point for that: reaching into :mod:`tingbok.app`'s private
globals from another project would break on any refactor here.

Everything is read-only and synchronous.  Writes are deliberately absent: an EAN
observation belongs in the service's data file, and a client writing to a local
copy of it would create a divergence nobody asked for.

The vocabulary is loaded on first use.  What you get is the vocabulary *as
declared* — the background label-fetching that enriches concepts from upstream
sources needs the network and a running service, so ``labels`` and
``description`` may be thinner than the same concept served over HTTP.  Ids,
hierarchy and source URIs are complete either way.
"""

from __future__ import annotations

import asyncio
from typing import Any

from tingbok import app as _app
from tingbok.models import VocabularyResolveRequest
from tingbok.sources import SOURCES


def _ensure_loaded() -> dict[str, Any]:
    """Load the vocabulary into the app module if it is not there yet."""
    if not _app.vocabulary:
        _app.vocabulary = _app._load_vocabulary()
        _app._vocab_uri_index = _app._build_vocab_uri_index(_app.vocabulary)
    return _app.vocabulary


def _run(make_coro):
    """Call *make_coro* and run the coroutine, refusing to nest inside a loop.

    Takes a factory rather than a coroutine so that nothing is created when the
    check fails — a coroutine built as an argument and then abandoned raises
    ``RuntimeWarning: coroutine ... was never awaited``.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(make_coro())
    raise RuntimeError(
        "tingbok.embedded is synchronous and cannot be called from inside a "
        "running event loop; await the tingbok.app coroutine directly instead"
    )


def get_vocabulary() -> dict[str, dict[str, Any]]:
    """Return the whole vocabulary, keyed by concept id.

    Mirrors ``GET /api/vocabulary`` but without its enrichment gate: that
    endpoint returns 503 until the background label fetch has been round every
    concept, which never happens in a process that is not running the service.
    """
    vocab = _ensure_loaded()
    return {
        concept_id: _app._vocabulary_concept_from_data(concept_id, data).model_dump()
        for concept_id, data in vocab.items()
    }


def get_concept(concept_id: str) -> dict[str, Any] | None:
    """Return one concept, or ``None`` if the vocabulary does not have it."""
    vocab = _ensure_loaded()
    data = vocab.get(concept_id)
    if data is None:
        return None
    return _app._vocabulary_concept_from_data(concept_id, data).model_dump()


def get_ancestors(concept_id: str) -> list[str] | None:
    """Return every transitive ancestor of *concept_id*, nearest first.

    ``None`` when the vocabulary has no such concept — the same distinction the
    HTTP endpoint draws between 404 and an empty list for a root concept.
    """
    vocab = _ensure_loaded()
    if concept_id not in vocab:
        return None
    return _app.ancestors_of(concept_id, vocab)


def get_sources() -> list[dict[str, Any]]:
    """Return the category-source registry as a list.

    The entries are what ``GET /api/sources`` serves inside its ``sources``
    key; this returns the bare list, not the wrapper.
    """
    return [
        {
            "name": s.name,
            "label": s.label,
            "uri_prefixes": list(s.uri_prefixes),
            "hosts": list(s.hosts),
            "homepage": s.homepage,
            "is_self": s.is_self,
        }
        for s in SOURCES
    ]


def resolve_vocabulary(labels: list[str], lang: str = "en", offline: bool = False) -> dict[str, Any]:
    """Resolve *labels* to a tailored vocabulary, as ``POST /api/vocabulary/resolve`` does.

    Args:
        labels:  Category labels or concept ids as the inventory writes them.
        lang:    Primary language of the inventory.
        offline: Resolve against the vocabulary only.  A caller that reached for
            this module because the network was unavailable wants this: the
            default would otherwise fall through to DBpedia and friends for
            every label the vocabulary does not have, which is the one thing
            that cannot work here.
    """
    _ensure_loaded()
    request = VocabularyResolveRequest(labels=labels, lang=lang, offline=offline)
    response = _run(lambda: _app.resolve_vocabulary(request))
    return response.model_dump()
