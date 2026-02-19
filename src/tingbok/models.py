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


class ProductResponse(BaseModel):
    """Product data from an EAN/barcode lookup."""

    ean: str
    name: str | None = None
    brand: str | None = None
    quantity: str | None = None
    categories: list[str] = []
    image_url: str | None = None
    source: str


class VocabularyConcept(BaseModel):
    """A single concept from the package vocabulary."""

    id: str
    prefLabel: str
    altLabel: dict[str, list[str]] = {}
    broader: list[str] = []
    narrower: list[str] = []
    uri: str | None = None
    labels: dict[str, str] = {}
    description: str | None = None
    wikipediaUrl: str | None = None


class HealthResponse(BaseModel):
    """Health check response."""

    status: str = "ok"
    version: str
