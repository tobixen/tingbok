"""FastAPI application for tingbok."""

from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import yaml
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from tingbok import __version__
from tingbok.models import HealthResponse, VocabularyConcept
from tingbok.routers import ean, skos

VOCABULARY_PATH = Path(__file__).parent / "data" / "vocabulary.yaml"

vocabulary: dict[str, Any] = {}


def _load_vocabulary() -> dict[str, Any]:
    """Load the package vocabulary from YAML."""
    with open(VOCABULARY_PATH) as f:
        data = yaml.safe_load(f)
    return data.get("concepts", {})


@asynccontextmanager
async def lifespan(app: FastAPI):  # noqa: ARG001
    """Load vocabulary on startup."""
    global vocabulary  # noqa: PLW0603
    vocabulary = _load_vocabulary()
    yield


app = FastAPI(
    title="tingbok",
    description="Product and category lookup service",
    version=__version__,
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(skos.router, prefix="/api/skos", tags=["skos"])
app.include_router(ean.router, prefix="/api/ean", tags=["ean"])


@app.get("/health", response_model=HealthResponse)
async def health():
    """Liveness check."""
    return HealthResponse(version=__version__)


@app.get("/api/vocabulary")
async def get_vocabulary() -> dict[str, VocabularyConcept]:
    """Return the full package vocabulary."""
    result = {}
    for concept_id, data in vocabulary.items():
        broader = data.get("broader", [])
        if isinstance(broader, str):
            broader = [broader]
        result[concept_id] = VocabularyConcept(
            id=concept_id,
            prefLabel=data.get("prefLabel", concept_id),
            altLabel=data.get("altLabel", {}),
            broader=broader,
            narrower=data.get("narrower", []),
            uri=data.get("uri"),
            labels=data.get("labels", {}),
            description=data.get("description"),
            wikipediaUrl=data.get("wikipediaUrl"),
        )
    return result


@app.get("/api/vocabulary/{concept_id}")
async def get_vocabulary_concept(concept_id: str) -> VocabularyConcept:
    """Return a single concept from the package vocabulary."""
    from fastapi import HTTPException

    data = vocabulary.get(concept_id)
    if data is None:
        raise HTTPException(status_code=404, detail=f"Concept '{concept_id}' not found")
    broader = data.get("broader", [])
    if isinstance(broader, str):
        broader = [broader]
    return VocabularyConcept(
        id=concept_id,
        prefLabel=data.get("prefLabel", concept_id),
        altLabel=data.get("altLabel", {}),
        broader=broader,
        narrower=data.get("narrower", []),
        uri=data.get("uri"),
        labels=data.get("labels", {}),
        description=data.get("description"),
        wikipediaUrl=data.get("wikipediaUrl"),
    )
