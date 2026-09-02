"""Pydantic models for tingbok API responses."""

from pydantic import BaseModel


class BroaderRef(BaseModel):
    """A broader (parent) concept reference."""

    uri: str
    label: str = ""


class ConceptResponse(BaseModel):
    """A SKOS concept with labels, hierarchy, and metadata."""

    uri: str | None = None
    prefLabel: str
    altLabels: dict[str, list[str]] = {}
    broader: list[BroaderRef] = []
    narrower: list[str] = []
    source: str
    labels: dict[str, str] = {}
    description: str | None = None
    wikipediaUrl: str | None = None


class HierarchyResponse(BaseModel):
    """Hierarchy paths for a concept label."""

    label: str
    paths: list[str] = []
    found: bool
    source: str
    #: Maps each path segment (e.g. ``"food/vegetables/potatoes"``) to its source URI.
    uri_map: dict[str, str] = {}


class LabelsResponse(BaseModel):
    """Translations for a URI."""

    uri: str
    labels: dict[str, str] = {}
    source: str


class BatchLabelsRequest(BaseModel):
    """Request body for batch label fetching."""

    uris: list[str]
    languages: list[str]
    source: str = "agrovoc"


class BatchLabelsResponse(BaseModel):
    """Translations for multiple URIs."""

    #: Maps each URI to a ``{lang: label}`` dict.
    labels: dict[str, dict[str, str]] = {}
    source: str


class CacheStatsResponse(BaseModel):
    """Statistics about the SKOS cache."""

    concept_count: int
    labels_count: int
    not_found_count: int
    cache_dir: str


class PriceObservation(BaseModel):
    """A single observed price for a product at a shop on a date."""

    shop: str | None = None
    date: str | None = None
    price: float
    currency: str = "NOK"
    unit: str | None = None


class ReceiptNameObservation(BaseModel):
    """A product name as printed on a shop receipt, with observation period."""

    shop: str | None = None
    name: str
    first_seen: str | None = None
    last_seen: str | None = None


class ReceiptNameMatch(BaseModel):
    """A candidate EAN matched against a printed receipt name."""

    ean: str
    #: Observed product name for this EAN (may be ``None``).
    name: str | None = None
    #: Similarity score in ``[0, 1]``; ``1.0`` is an exact (normalised) match.
    score: float
    #: The stored receipt name that matched the query.
    matched_name: str
    #: Shop the matched receipt name was observed at (may be ``None``).
    shop: str | None = None


class ReceiptNameSearchResponse(BaseModel):
    """Ranked EAN candidates for a reverse receipt-name lookup."""

    query: str
    results: list[ReceiptNameMatch] = []


class ProductResponse(BaseModel):
    """Product data from an EAN/barcode lookup."""

    ean: str
    name: str | None = None
    brand: str | None = None
    quantity: str | None = None
    categories: list[str] = []
    image_url: str | None = None
    source: str
    #: Author(s) — populated for books/ISBNs.
    author: str | None = None
    #: Product type: ``"product"``, ``"book"``, etc.
    type: str = "product"
    #: Locally observed shop prices.
    prices: list[PriceObservation] = []
    #: Receipt name observations (may differ by shop/locale).
    receipt_names: list[ReceiptNameObservation] = []
    #: Free-text note (e.g. "Lidl internal barcode").
    note: str | None = None


class EanObservationRequest(BaseModel):
    """Inventory-sourced observation for an EAN product."""

    #: Category path(s) as classified in the inventory (e.g. ``["food/dairy"]``).
    categories: list[str] = []
    #: Clean product name extracted from the inventory item text.
    name: str | None = None
    #: Weight or volume string (e.g. ``"140g"``, ``"1l"``).
    quantity: str | None = None
    #: Observed prices (e.g. from the inventory price: tag).
    prices: list[PriceObservation] = []
    #: Receipt name observations (e.g. Lidl receipt names in local language).
    receipt_names: list[ReceiptNameObservation] = []


class VocabularyConcept(BaseModel):
    """A single concept from the tingbok vocabulary."""

    id: str
    prefLabel: str
    altLabel: dict[str, list[str]] = {}
    broader: list[str] = []
    narrower: list[str] = []
    uri: str | None = None
    source_uris: list[str] = []
    excluded_sources: list[str] = []
    labels: dict[str, str] = {}
    description: str | None = None
    wikipediaUrl: str | None = None
    #: Source-specific hierarchy paths, keyed by source name.  E.g.
    #: ``{"gpt": "food/food_items/fruit/bananas"}`` gives the tingbok-normalised
    #: path for this concept within the GPT taxonomy, letting clients build
    #: proper per-source subtrees rather than a flat list.
    source_paths: dict[str, str] = {}
    #: Language-keyed path aliases.  E.g. ``{"nb": ["klær/vinter"]}`` means
    #: ``GET /api/lookup/klær/vinter?lang=nb`` resolves to this concept.
    path_aliases: dict[str, list[str]] = {}


class VocabularyResolveRequest(BaseModel):
    """Body for ``POST /api/vocabulary/resolve``."""

    #: Category labels or concept IDs as they appear in the inventory.
    labels: list[str]
    #: Primary language of the inventory (affects label resolution order).
    lang: str = "en"
    #: Resolve against the vocabulary only, never the upstream SKOS sources.
    #: A label the vocabulary does not have comes back as an unresolved stub
    #: rather than triggering a DBpedia/Wikidata/AGROVOC lookup.  Set by the
    #: in-process (embedded) path, where by definition there is no network.
    offline: bool = False


class VocabularyResolveResponse(BaseModel):
    """Response from ``POST /api/vocabulary/resolve``."""

    #: Concept ID → concept, covering all requested labels plus their ancestors.
    concepts: dict[str, "VocabularyConcept"]
    #: Labels from the request that could not be matched to any concept.
    unresolved: list[str] = []


class VocabularyConceptUpdateRequest(BaseModel):
    """Body for ``PUT /api/vocabulary/{concept_id}`` — partial concept update.

    All fields are optional; omitted fields leave the existing concept data
    unchanged.  When the concept (or any ancestor in the path) does not yet
    exist, a minimal entry is created automatically.
    """

    #: New English preferred label.  Overrides the current ``prefLabel``.
    prefLabel: str | None = None
    #: Language-keyed labels to add or overwrite (merged into ``labels``).
    labels: dict[str, str] = {}
    #: Language-keyed lists of alternative labels to append (merged).
    altLabel: dict[str, list[str]] = {}
    #: External source URIs to add to ``source_uris``.
    add_source_uris: list[str] = []
    #: External source URIs to remove from ``source_uris``.
    remove_source_uris: list[str] = []
    #: Source names to add to ``excluded_sources``.
    add_excluded_sources: list[str] = []
    #: Source names to remove from ``excluded_sources``.
    remove_excluded_sources: list[str] = []


class HealthResponse(BaseModel):
    """Health check response."""

    status: str = "ok"
    version: str
    uptime_seconds: float | None = None
    #: Total number of concepts in the vocabulary.
    vocabulary_concepts: int | None = None
    #: Number of concepts whose labels have been fetched from external sources.
    vocabulary_concepts_enriched: int | None = None
    #: Age in days of the oldest entry in the SKOS/EAN cache (localhost only).
    cache_oldest_entry_age_days: float | None = None
    #: Seconds until the next scheduled cache refresh (localhost only; 0 means in progress).
    cache_next_refresh_in_seconds: float | None = None
    #: File paths (localhost clients only).
    paths: dict[str, str] | None = None


class SourceInfo(BaseModel):
    """One entry from tingbok's category-source registry.

    Served so that clients stop carrying their own copy of the URI-prefix table
    and the display names; adding a source to tingbok used to require a client
    release before the source appeared in a category browser.
    """

    #: Short identifier used throughout the API (``"agrovoc"``, ``"off"``).
    name: str
    #: Human-readable name for display (``"OpenFoodFacts"``).
    label: str
    #: Literal prefixes for sources whose URIs are not http(s) (``off:``, ``gpt:``).
    uri_prefixes: list[str] = []
    #: Domains identifying an http(s) source URI, matched against the URI's host
    #: and its subdomains — upstream data carries ``de.dbpedia.org`` as well as
    #: ``dbpedia.org``, and both http and https spellings of each.
    hosts: list[str] = []
    #: Where a human can read about the source.
    homepage: str | None = None
    #: True for tingbok's own concept URIs — a concept tingbok defines itself
    #: rather than one resolved upstream.  Such URIs are never looked up
    #: externally, but clients still group and label concepts by this source.
    is_self: bool = False


class SourcesResponse(BaseModel):
    """Response for ``GET /api/sources``."""

    sources: list[SourceInfo] = []


class AncestorsResponse(BaseModel):
    """Response for ``GET /api/ancestors/{concept_id}``.

    Its own namespace rather than ``/api/vocabulary/{id}/ancestors``: concept
    ids are themselves paths, so that shape collides with a concept genuinely
    called ``x/ancestors``.
    """

    #: The concept asked about.
    id: str
    #: Every transitive ancestor, nearest first, excluding the concept itself.
    #: Deduplicated: a concept with several parents can reach one ancestor by
    #: more than one route, and cycles in upstream SKOS data are broken.
    ancestors: list[str] = []
