"""The registry of category sources tingbok knows about.

One table, in one place, describing every source a concept URI can come from:
its short name, a human-readable label, and what identifies one of its URIs —
a host for the http(s) sources, a literal prefix for ``off:`` and ``gpt:``.

Clients used to keep their own copy of both halves — inventory-md carried a
``_SOURCE_LABELS`` dict and a ``_uri_to_source()`` if-chain — so adding a source
to tingbok silently required a client release before the new source appeared in
the category browser.  :func:`tingbok.app.get_sources` serves this table over
``GET /api/sources`` instead.

Adding a source means adding one entry here.  Nothing else in tingbok, and
nothing in any client, should hardcode a host, a URI prefix or a display name.
"""

from __future__ import annotations

from dataclasses import dataclass
from urllib.parse import urlsplit


@dataclass(frozen=True)
class Source:
    """One category source.

    Attributes:
        name: Short identifier used throughout the API (``"agrovoc"``, ``"off"``).
        label: Human-readable name for display (``"OpenFoodFacts"``).
        uri_prefixes: Literal prefixes for sources whose URIs are not http(s)
            at all — ``off:en:potatoes``, ``gpt:632``.
        hosts: Domain names identifying an http(s) source URI, matched against
            the URI's host *and its subdomains*.  A prefix test is not enough:
            upstream data carries language subdomains (``de.dbpedia.org``,
            ``fr.dbpedia.org``), ``wikidata.org`` with and without ``www.``,
            and both http and https spellings of all of them.  Matching the
            host also means the http/https distinction stops mattering here,
            which is what the TODO item about normalising source URIs to https
            is really about.
        homepage: Where a human can go to read about the source.
        is_self: True for tingbok's own concept URIs.  Such a URI identifies a
            tingbok concept rather than an upstream one, so it is not something
            to look up upstream — :func:`uri_to_source` returns ``None`` for it.
            It is still served over the API because clients group concepts by
            source and need a name and a label for tingbok-native concepts.
    """

    name: str
    label: str
    uri_prefixes: tuple[str, ...] = ()
    hosts: tuple[str, ...] = ()
    homepage: str | None = None
    is_self: bool = False


#: Every source tingbok knows about.  Order is display order.
SOURCES: tuple[Source, ...] = (
    Source(
        name="agrovoc",
        label="AGROVOC",
        hosts=("aims.fao.org",),
        homepage="https://agrovoc.fao.org/",
    ),
    Source(
        name="dbpedia",
        label="DBpedia",
        hosts=("dbpedia.org",),
        homepage="https://www.dbpedia.org/",
    ),
    Source(
        name="wikidata",
        label="Wikidata",
        hosts=("wikidata.org",),
        homepage="https://www.wikidata.org/",
    ),
    Source(
        name="off",
        label="OpenFoodFacts",
        uri_prefixes=("off:",),
        homepage="https://world.openfoodfacts.org/",
    ),
    Source(
        name="gpt",
        label="Google Product Taxonomy",
        uri_prefixes=("gpt:",),
        homepage="https://www.google.com/basepages/producttype/taxonomy-with-ids.en-US.txt",
    ),
    Source(
        name="tingbok",
        label="Tingbok",
        hosts=("tingbok.plann.no",),
        homepage="https://tingbok.plann.no/",
        is_self=True,
    ),
)


def source_by_name(name: str) -> Source | None:
    """Return the registry entry called *name*, or ``None``."""
    for source in SOURCES:
        if source.name == name:
            return source
    return None


def host_matches(host: str, domain: str) -> bool:
    """True if *host* is *domain* or a subdomain of it.

    A suffix test alone would accept ``notdbpedia.org`` for ``dbpedia.org``,
    and ``dbpedia.org.evil.example`` for anything, so the boundary has to be a
    dot.
    """
    host = host.lower().rstrip(".")
    return host == domain or host.endswith("." + domain)


def uri_to_source(uri: str) -> str | None:
    """Map a concept URI to the name of the *upstream* source it came from.

    Args:
        uri: Any URI stored in ``source_uris`` (e.g.
            ``"http://dbpedia.org/resource/Food"``, ``"off:en:potatoes"``,
            ``"gpt:632"``).

    Returns:
        The source name, or ``None`` for an unrecognised URI **and** for
        tingbok's own concept URIs — callers use this to decide where to fetch
        labels and descriptions from, and there is nothing upstream to fetch for
        a concept tingbok itself defines.  Use :func:`source_by_name` or
        :data:`SOURCES` when you want the self source included.
    """
    host = ""
    if "://" in uri:
        host = (urlsplit(uri).hostname or "").lower()
    for source in SOURCES:
        if source.is_self:
            continue
        if source.uri_prefixes and uri.startswith(source.uri_prefixes):
            return source.name
        if host and any(host_matches(host, domain) for domain in source.hosts):
            return source.name
    return None
