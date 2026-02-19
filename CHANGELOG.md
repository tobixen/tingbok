# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project should adhere to [Semantic Versioning](https://semver.org/spec/v2.0.0.html) - except, for pre-releases PEP440 takes precedence.

## [Unreleased]

### Changed
- **`ConceptResponse.broader`** now returns `list[{uri, label}]` objects instead
  of plain URI strings, so callers can traverse the hierarchy without an extra
  label-fetch round-trip

### Added
- **SKOS API — feature-complete** — all four SKOS endpoints are now operational:
  - `GET /api/skos/lookup` — concept lookup (cache → upstream fallback)
  - `GET /api/skos/labels` — multilingual label fetch (cache → upstream fallback)
  - `GET /api/skos/hierarchy` — recursive path-to-root hierarchy building with
    root mapping (e.g. AGROVOC "plant products" → "food") and cycle detection
  - `POST /api/skos/labels/batch` — batch label fetch for multiple URIs
  - `GET /api/skos/cache` — cache statistics (concept / labels / not-found counts)
- Cache format is compatible with inventory-md's SKOS cache; the cache dir can
  be pre-seeded from an existing `~/.cache/inventory-md/skos/` directory
  - Cache directory controlled by `TINGBOK_CACHE_DIR` env var (default:
    `~/.cache/tingbok/`; SKOS sub-dir: `skos/`)
  - Cache TTL: 60 days (matches inventory-md)
- Upstream fallback REST APIs: AGROVOC Skosmos, DBpedia Lookup + Data,
  Wikidata Action API (`wbsearchentities` / `wbgetentities`)
  - `skos:broader` / P279 traversal for DBpedia and Wikidata hierarchy building
  - Graceful handling of empty or malformed upstream responses (no crash on
    empty JSON body from AGROVOC data endpoint)
- `make run` target starts the dev server with `TINGBOK_CACHE_DIR` pointing at
  the local inventory-md SKOS cache (`~/.cache/inventory-md`)
- `httpx` added as a main dependency (used for upstream REST calls)

## [v0.0.4] - 2026-02-18

### Changed

Support for python 3.10

## [v0.0.2] - 2026-02-18

### Added
- Project skeleton with FastAPI application
- Health check endpoint
- Package vocabulary endpoint (serves ~258 concepts from vocabulary.yaml)
- Stub endpoints for SKOS lookup, hierarchy, and labels (501 Not Implemented)
- Stub endpoint for EAN/barcode lookup (501 Not Implemented)
- CORS middleware (open)
- CI workflow (GitHub Actions, Python 3.11-3.14)
- PyPI publish workflow (trusted publishing on tags)
