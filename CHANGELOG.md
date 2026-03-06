# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project should adhere to [Semantic Versioning](https://semver.org/spec/v2.0.0.html) - except, for pre-releases PEP440 takes precedence.


## [Unreleased]

### Added
- **`uri_to_source(uri)`** in `services/skos.py` — maps any `source_uris` entry
  (DBpedia, AGROVOC, Wikidata, `off:`, `gpt:`) to its source name string.
- **`get_description(uri, source, lang, cache_dir)`** in `services/skos.py` — fetches
  a human-readable description for DBpedia and Wikidata URIs; results are stored in the
  existing labels cache alongside translations.
- **`get_labels(uri, languages)`** in `services/off.py` — returns translations for an
  `off:{node_id}` URI directly from the in-memory OFF taxonomy.
- **Background label + description fetching** (`_fetch_labels_background()` in `app.py`)
  — on startup, fetches translations from all `source_uris` for every vocabulary concept
  and stores them in `_fetched_labels` / `_fetched_descriptions`.  Uses the SKOS cache
  (60-day TTL) so live API calls are rare after initial population.
- **Vocabulary API now merges source translations** — `GET /api/vocabulary` and
  `GET /api/vocabulary/{id}` return labels merged from external sources alongside any
  static labels in `vocabulary.yaml` (static labels override source translations).
  Descriptions from sources are used as fallback when `vocabulary.yaml` has none.
- **`prune-vocabulary` CLI subcommand** — compares `labels:` blocks in `vocabulary.yaml`
  against translations fetched from `source_uris`; removes labels that match a source
  (case-insensitive) and reports deviations for manual review.  Use `--dry-run` to
  preview without writing.

## [v0.8.0] - 2026-03-04

### Added
- **Google Product Taxonomy (GPT) source** (`services/gpt.py`) — parses locally cached
  GPT taxonomy files and provides concept lookup by label.  URI scheme: `gpt:{id}`
  (e.g. `gpt:632` for "Electronics"), mirroring OFF's `off://` synthetic URIs.
  `lookup_concept` supports label-based lookup with broader-parent resolution.
  `get_labels` fetches translations from all cached language files.
- **`download-taxonomy` CLI subcommand** — downloads taxonomy data files into the local
  cache directory:
  - `--gpt [LOCALE ...]` downloads Google Product Taxonomy files from
    `https://www.google.com/basepages/producttype/taxonomy-with-ids.{locale}.txt`.
    Without a locale argument defaults to `en-GB`.  Known locales are listed in the
    help text (`nb-NO`, `sv-SE`, `de-DE`, `fr-FR`, etc.).  Files are stored as
    `{cache_dir}/gpt/taxonomy-with-ids.{locale}.txt`.
  - `--agrovoc` downloads the latest AGROVOC LOD N-Triples zip from FAO
    (`https://agrovoc.fao.org/latestAgrovoc/agrovoc_lod.nt.zip`), extracts
    `agrovoc.nt` into `{cache_dir}/skos/`, and removes the zip.
  - Default cache root: `/var/cache/tingbok` (override with `--cache-dir`).
- **`source_uris` populated in `vocabulary.yaml`** for all 15 concepts that have a known
  external URI.  Each concept's `source_uris` list now includes the corresponding DBpedia
  URI (e.g. `food` → `http://dbpedia.org/resource/Food`).
- **`excluded_sources: [agrovoc]`** added to 9 concepts known to cause AGROVOC mismatches:
  `bedding`, `disc`, `gps`, `peanuts`, `seal`, `snacks`, `tool`, `tools`, `tubing`, `washer`.
  The vocabulary builder will skip AGROVOC lookups for these concepts.
- **`vocabulary.yaml` format documentation** — added comments to the file header explaining
  the semantics of `uri:` (preferred/canonical URI), `source_uris:` (all external source URIs),
  and `excluded_sources:` (sources checked and found inapplicable).
- **Background URI auto-discovery** (`_discover_source_uris_background()` in `app.py`) —
  on startup, tingbok now queries DBpedia and Wikidata for concepts that have no external
  `source_uris` in `vocabulary.yaml`.  Discovered URIs are merged into API responses
  at serving time.  Results are in-memory and rebuilt from the SKOS cache on next restart.
  AGROVOC is also queried when the local Oxigraph store (`agrovoc.nt`) is present in the
  SKOS cache directory — skipped otherwise to avoid REST false positives.
- **AGROVOC Oxigraph lookup** added to `services/skos.py` (`get_agrovoc_store()`,
  `_lookup_agrovoc_oxigraph()`) — when `agrovoc.nt` is present in the SKOS cache dir,
  `lookup_concept(..., source="agrovoc")` uses the local store instead of the REST API.
  Mirrors inventory-md's SKOS-XL SPARQL queries and singular/plural label variations.
  Requires `tingbok[skos]` (pyoxigraph).  Gracefully degrades to REST when unavailable.


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
