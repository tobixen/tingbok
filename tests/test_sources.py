"""Tests for the source registry and the ``/api/sources`` endpoint.

tingbok is the authority on which category sources exist; clients used to carry
their own copy of the URI-prefix table and the human-readable names, so adding a
source here silently required a client release.
"""

import pytest

from tingbok.sources import SOURCES, source_by_name, uri_to_source


def test_registry_names_are_unique() -> None:
    names = [s.name for s in SOURCES]
    assert len(names) == len(set(names))


def test_every_source_is_recognisable() -> None:
    for source in SOURCES:
        assert source.label, f"{source.name} has no human-readable label"
        assert source.uri_prefixes or source.hosts, f"{source.name} has nothing to match a URI on"
        assert source.homepage, f"{source.name} has no homepage"


def test_registry_prefixes_round_trip_through_uri_to_source() -> None:
    """Every declared prefix must resolve back to its own source (self excluded)."""
    for source in SOURCES:
        if source.is_self:
            continue
        for prefix in source.uri_prefixes:
            assert uri_to_source(prefix + "x") == source.name


def test_registry_hosts_round_trip_through_uri_to_source() -> None:
    for source in SOURCES:
        if source.is_self:
            continue
        for host in source.hosts:
            assert uri_to_source(f"https://{host}/resource/X") == source.name


@pytest.mark.parametrize(
    ("uri", "expected"),
    [
        # Language subdomains are real in the data: four concepts in one
        # inventory carry de./fr. DBpedia URIs, and a startswith test on
        # "https://dbpedia.org/" silently drops them.
        ("https://de.dbpedia.org/resource/Cornflakes", "dbpedia"),
        ("https://fr.dbpedia.org/resource/Carbonated_Soft_Drink", "dbpedia"),
        ("http://de.dbpedia.org/resource/Saucen", "dbpedia"),
        ("https://wikidata.org/entity/Q1", "wikidata"),
        ("https://www.wikidata.org/entity/Q1", "wikidata"),
        ("http://aims.fao.org/aos/agrovoc/c_1", "agrovoc"),
        # A host that merely ends in the same letters is not the same host.
        ("https://notdbpedia.org/resource/X", None),
        ("https://dbpedia.org.evil.example/x", None),
    ],
)
def test_host_matching(uri: str, expected: str | None) -> None:
    assert uri_to_source(uri) == expected


def test_self_source_is_not_an_external_source() -> None:
    """tingbok's own concept URIs are in the registry but are not a lookup source.

    ``uri_to_source`` feeds the label/description fetchers, which must not try to
    resolve tingbok's own URIs upstream.
    """
    tingbok = source_by_name("tingbok")
    assert tingbok is not None
    assert tingbok.is_self
    assert uri_to_source("https://tingbok.plann.no/api/vocabulary/food") is None


def test_known_sources_are_present() -> None:
    for name in ("agrovoc", "dbpedia", "wikidata", "off", "gpt", "tingbok"):
        assert source_by_name(name) is not None, f"{name} missing from the registry"


@pytest.mark.anyio
async def test_get_sources(client) -> None:
    response = await client.get("/api/sources")
    assert response.status_code == 200
    data = response.json()
    by_name = {s["name"]: s for s in data["sources"]}
    assert by_name["off"]["label"] == "OpenFoodFacts"
    assert by_name["gpt"]["label"] == "Google Product Taxonomy"
    assert "off:" in by_name["off"]["uri_prefixes"]
    assert "dbpedia.org" in by_name["dbpedia"]["hosts"]
    assert by_name["tingbok"]["is_self"] is True


@pytest.mark.anyio
async def test_get_sources_covers_the_registry(client) -> None:
    """The endpoint must not be a second, hand-maintained list."""
    response = await client.get("/api/sources")
    served = {s["name"] for s in response.json()["sources"]}
    assert served == {s.name for s in SOURCES}


def test_skos_lookup_is_a_registry_fact() -> None:
    """Which sources support SKOS lookup belongs on the Source, not in a set.

    It was written as a fresh frozenset in cli.py and as an inline tuple in
    five more places, which is the duplication tingbok/sources.py exists to
    end — a new SKOS-capable source would have had to be added in six.
    """
    from tingbok.sources import skos_lookup_sources, source_by_name

    assert skos_lookup_sources() == ("agrovoc", "dbpedia", "wikidata")
    assert source_by_name("dbpedia").skos_lookup is True
    assert source_by_name("off").skos_lookup is False, "OFF carries labels in its own download"


def test_a_lookalike_host_is_not_classified_as_a_known_source() -> None:
    """``notdbpedia.org`` and ``dbpedia.org.evil.example`` are not DBpedia.

    A substring test says they are.  This is the reason the registry matches on
    host with a dot boundary, so anything classifying a URI has to come through
    here rather than writing ``"dbpedia.org" in uri``.
    """
    from tingbok.sources import uri_to_source

    assert uri_to_source("https://notdbpedia.org/resource/Food") is None
    assert uri_to_source("https://dbpedia.org.evil.example/resource/Food") is None
    assert uri_to_source("https://de.dbpedia.org/resource/Saucen") == "dbpedia"
