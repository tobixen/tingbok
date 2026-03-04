# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project should adhere to [Semantic Versioning](https://semver.org/spec/v2.0.0.html) - except, for pre-releases PEP440 takes precedence.

## [v0.7.0] - 2026-03-04

By a mistake the v0.7.0-tag, dedicated the inventory-md package, got applied in the wrong directory.  Oh well.  I may as well keep the version numbers in sync and release them together - because as it is now, it's sort of two parts of the same system.

## [v0.2.0] - 2026-03-04

### Added
- **`source_uris` and `excluded_sources` fields** on `VocabularyConcept` model and
  `/api/vocabulary` / `/api/vocabulary/{concept_id}` endpoints.
  - `source_uris` is a list of URIs for all external sources the concept is
    present in (including the canonical tingbok self-URI for every concept).
  - `excluded_sources` lists source names that have been checked and found
    inapplicable for this concept.
  - Both fields are populated from `vocabulary.yaml`; the tingbok self-URI
    (`https://tingbok.plann.no/api/vocabulary/{id}`) is always prepended
    automatically even if absent from YAML.

## [v0.1.2] - 2026-03-03

### Fixed
- Replaced `httpx` with `niquests` as the HTTP client.  Wikimedia's Varnish
  cache returns 403 to `httpx` due to TLS fingerprint filtering, while
  `niquests` (a `requests`-compatible fork with modern TLS handling) is
  accepted.  `httpx` is no longer a runtime dependency.
- `httpx` is retained as a dev dependency for the ASGI test client
  (`httpx.ASGITransport` + `AsyncClient`), which has no equivalent in
  `niquests`.

## [v0.1.1] - 2026-03-03

### Fixed
- Transient upstream errors (e.g. HTTP 403 from Wikidata) now return **502 Bad
  Gateway** to the client instead of 404, and are no longer written to the
  not-found cache.  Previously a 403 was indistinguishable from a genuine
  not-found result, causing the concept to be permanently cached as missing.

## [v0.1.0] - 2026-02-24

### Changed
- **`ConceptResponse.broader`** now returns `list[{uri, label}]` objects instead
  of plain URI strings, so callers can traverse the hierarchy without an extra
  label-fetch round-trip

### Added
- **SKOS API — feature-complete** — all five SKOS endpoints are now operational:
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
