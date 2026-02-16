"""Pydantic models for tingbok API responses."""

from pydantic import BaseModel


class ConceptResponse(BaseModel):
    """A SKOS concept with labels, hierarchy, and metadata."""

    uri: str | None = None
    prefLabel: str
    altLabels: dict[str, list[str]] = {}
    broader: list[str] = []
    narrower: list[str] = []
    source: str
    labels: dict[str, str] = {}
    description: str | None = None
    wikipediaUrl: str | None = None


class HierarchyResponse(BaseModel):
    """Hierarchy paths for a label."""

    label: str
    paths: list[str] = []
    found: bool
    source: str


class LabelsResponse(BaseModel):
    """Translations for a URI."""

    uri: str
    labels: dict[str, str] = {}
    source: str


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
