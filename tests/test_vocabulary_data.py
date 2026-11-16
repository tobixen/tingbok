"""Sanity checks on the bundled vocabulary.yaml itself."""

from __future__ import annotations

from importlib.resources import files

import yaml

_KNOWN_KEYS = {
    "prefLabel",
    "altLabel",
    "labels",
    "broader",
    "narrower",
    "uri",
    "source_uris",
    "excluded_sources",
    "path_aliases",
}


def _concepts() -> dict:
    return yaml.safe_load(files("tingbok").joinpath("data/vocabulary.yaml").read_text(encoding="utf-8"))["concepts"]


def test_concepts_use_only_known_keys() -> None:
    """A misspelt key (``sources:`` for ``source_uris:``) is silently ignored, so refuse it."""
    unknown = {cid: sorted(set(c) - _KNOWN_KEYS) for cid, c in _concepts().items() if c and set(c) - _KNOWN_KEYS}
    assert unknown == {}


def test_source_uri_lists_are_lists() -> None:
    """An empty ``source_uris:`` header parses as None and breaks every consumer."""
    bad = [cid for cid, c in _concepts().items() if c and "source_uris" in c and not isinstance(c["source_uris"], list)]
    assert bad == []


def test_wikidata_uris_use_entity_form() -> None:
    bad = [u for c in _concepts().values() if c for u in c.get("source_uris") or [] if "wikidata.org/wiki/" in u]
    assert bad == []


def _source_of(uri: str) -> str:
    for host, name in (("dbpedia.org", "dbpedia"), ("wikidata.org", "wikidata"), ("aims.fao.org", "agrovoc")):
        if host in uri:
            return name
    return uri.split(":", 1)[0]


def test_no_excluded_source_is_also_listed() -> None:
    """Excluding a source a concept still links to reads as a contradiction."""
    bad = {
        cid: sorted(set(c.get("excluded_sources") or []) & {_source_of(u) for u in c.get("source_uris") or []})
        for cid, c in _concepts().items()
        if c
    }
    assert {k: v for k, v in bad.items() if v} == {}


def test_every_concept_reaches_a_root() -> None:
    """No orphans: each concept's broader chain (or its path prefix) ends at a ``_root`` child."""
    concepts = _concepts()
    roots = set(concepts["_root"]["narrower"])

    def parents(cid: str) -> list[str]:
        broader = (concepts.get(cid) or {}).get("broader") or []
        broader = [broader] if isinstance(broader, str) else broader
        if not broader and "/" in cid:
            broader = [cid.rsplit("/", 1)[0]]
        return broader

    def reaches(cid: str, seen: frozenset[str] = frozenset()) -> bool:
        if cid in roots:
            return True
        if cid in seen:
            return False
        return any(reaches(p, seen | {cid}) for p in parents(cid))

    orphans = sorted(c for c in concepts if not c.startswith("_") and c not in roots and not reaches(c))
    assert orphans == []


def test_narrow_curated_concepts_do_not_take_generic_ids() -> None:
    """A source path ending in .../engine or .../water must not become the boat engine or drinking water."""
    concepts = _concepts()
    for generic in ("water", "engine", "roller", "grip", "cover", "optics", "jack"):
        assert generic not in concepts, generic


def test_ambiguous_labels_are_not_used() -> None:
    concepts = _concepts()
    labels = {a.lower() for c in concepts.values() if c for alts in (c.get("altLabel") or {}).values() for a in alts}
    for ambiguous in ("dispenser", "jelly", "crispbread", "cord"):
        assert ambiguous not in labels, ambiguous
