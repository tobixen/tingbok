"""The DBpedia resource-URI → data-URI transform.

Every DBpedia fetcher needs the JSON behind a resource URI, and each one used
to spell the transform out as a literal ``str.replace`` of the http:// form.
The vocabulary stores the https:// form, so that replace was a no-op for all
284 DBpedia URIs actually shipped, and for the language-subdomain URIs that
turn up in client data it was a no-op too.
"""

import pytest

from tingbok.services.skos import _dbpedia_data_uri


@pytest.mark.parametrize(
    ("uri", "expected"),
    [
        ("http://dbpedia.org/resource/Food", "https://dbpedia.org/data/Food.json"),
        ("https://dbpedia.org/resource/Food", "https://dbpedia.org/data/Food.json"),
        ("https://de.dbpedia.org/resource/Saucen", "https://de.dbpedia.org/data/Saucen.json"),
        ("http://fr.dbpedia.org/resource/Fromage", "https://fr.dbpedia.org/data/Fromage.json"),
    ],
)
def test_resource_uris_become_data_uris(uri: str, expected: str) -> None:
    assert _dbpedia_data_uri(uri) == expected


def test_the_local_name_keeps_its_own_encoding() -> None:
    """Percent-escapes and parentheses are part of the DBpedia local name."""
    assert (
        _dbpedia_data_uri("https://dbpedia.org/resource/Mercury_(element)")
        == "https://dbpedia.org/data/Mercury_(element).json"
    )


def test_a_non_dbpedia_uri_is_refused() -> None:
    """Callers reach here only for URIs the registry called DBpedia."""
    assert _dbpedia_data_uri("https://www.wikidata.org/wiki/Q1") is None


def test_the_response_is_read_under_the_canonical_http_key() -> None:
    """DBpedia keys its data documents by the ``http://`` IRI.

    The vocabulary stores the ``https://`` spelling for all 284 of its DBpedia
    concepts, so looking the resource up under the URI as given finds nothing —
    the fetch succeeds and the parse returns empty, which ``get_labels`` then
    caches as a definitive answer.  ``_fetch_dbpedia_types`` has always tried
    both spellings for this reason.
    """
    from tingbok.services.skos import _dbpedia_resource_keys

    keys = _dbpedia_resource_keys("https://dbpedia.org/resource/Food")
    assert "http://dbpedia.org/resource/Food" in keys
    assert "https://dbpedia.org/resource/Food" in keys
    assert keys[0].startswith("http://"), "the canonical spelling is tried first"


def test_resource_keys_keep_the_source_host() -> None:
    from tingbok.services.skos import _dbpedia_resource_keys

    keys = _dbpedia_resource_keys("https://de.dbpedia.org/resource/Saucen")
    assert "http://de.dbpedia.org/resource/Saucen" in keys
