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
    """A single concept from the package vocabulary."""

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
    #: File paths (localhost clients only).
    paths: dict[str, str] | None = None
