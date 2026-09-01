# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project should adhere to [Semantic Versioning](https://semver.org/spec/v2.0.0.html) - except, for pre-releases PEP440 takes precedence.


## [Unreleased]

### Added
- **`GET /api/sources`** — the registry of category sources tingbok knows about: short name, human-readable label, the URI prefixes that identify a concept as coming from that source, and a homepage. Both halves of this were duplicated in inventory-md — a `_SOURCE_LABELS` dict and a `_uri_to_source()` if-chain — so adding a source here did not show up in a client's category browser until the client was released again, which is precisely backwards: tingbok is the authority on which sources exist. New `tingbok/sources.py` holds the one table; `skos.uri_to_source()` is now a thin re-export of it, `cli.py`'s two inline copies of the same prefix chain are gone, and the endpoint is generated from the table rather than being a third hand-kept list. An http(s) source is identified by **host** rather than by a literal URI prefix — upstream data carries `de.dbpedia.org` and `fr.dbpedia.org` as well as `dbpedia.org`, `wikidata.org` with and without `www.`, and both http and https spellings of all of them, and a prefix test silently dropped every one of the variants (four concepts in one real inventory). Matching the host, with a dot boundary so `notdbpedia.org` does not qualify, covers them all and makes the http/https spelling stop mattering. tingbok's own concept URIs are in the registry (clients group and label tingbok-native concepts too) but carry `is_self: true` and still resolve to `None` through `uri_to_source()`, since there is nothing upstream to fetch for a concept tingbok defines itself.
- **`tingbok.embedded` — the same answers, in-process, with no server running** — a client with the tingbok package installed can now get the vocabulary, one concept, a concept's ancestors, the source registry and a batch resolve by importing it, rather than being stuck when `tingbok.plann.no` does not respond. It exists because the alternative was a client reaching into `tingbok.app`'s private globals, which would break on any refactor here; this is the supported surface. Read-only by design: an EAN observation belongs in the service's own data file, and a client writing to a local copy would create a divergence nobody asked for. The vocabulary loads on first use, and what you get is the vocabulary *as declared* — the background label enrichment needs the network and a running service, so `labels` and `description` can be thinner than the same concept served over HTTP, while ids, hierarchy and source URIs are complete either way.
- **`POST /api/vocabulary/resolve` takes `offline: true`** — resolve against the vocabulary only, never the upstream SKOS sources; a label the vocabulary does not have comes back as an unresolved stub instead of triggering a DBpedia/Wikidata/AGROVOC lookup. Needed by the in-process path above, where by definition there is no network and the default behaviour would burn a timeout per unknown label, but useful to any client that wants a fast answer from what tingbok already knows.
- **`GET /api/ancestors/{concept_id}`** — every transitive ancestor of a concept, nearest first. `GET /api/vocabulary` serves a flat list that each client then re-structures, so "is soybeans food?" was answered by a hand-written tree walk in every client, each subtly different and each silently wrong if tingbok ever changed how hierarchy works. Breadth-first over `broader`, so a concept with several parents reports all of them, and already-seen concepts are skipped — which also breaks the broader/narrower cycles that turn up in upstream SKOS data (`lentil` has itself as both). Where a concept declares no `broader` at all, the concept named by its own path prefix is used if the vocabulary has it, so `epoxy/filler` is a child of `epoxy` rather than a second root beside it; a path prefix that is *not* itself a concept yields no parent, because inventing one is a hierarchy decision and unresolved labels are deliberately left at the root for now. It gets its own path namespace rather than `/api/vocabulary/{id}/ancestors`, because concept ids are themselves paths: that shape collides with a concept genuinely called `x/ancestors`, and whichever way the collision is resolved one of the two becomes unreachable. A trailing slash is tolerated, since no concept id ends in one.
- **Shop-prefixed local article numbers** — in-store/GS1 "restricted distribution" codes
  (e.g. Lidl's `20004132`) are not globally unique, so they are now stored under a
  `<shop>-<code>` key (`lidl-20004132`, `mercadona-00501163`). `ean_service.resolve_local_alias()`
  forwards a bare lookup to the prefixed record when exactly one shop matches, logging a
  warning on cross-shop collisions instead of guessing. Both `GET` and `PUT /api/ean/{ean}`
  honour the forward (so importers emitting bare scanned codes update the canonical record),
  and neither queries upstream sources for hyphenated local keys. Existing records in
  `ean-db.json` were migrated (66 Lidl/Mercadona codes); manufacturer part numbers and
  ambiguous unattributed codes were left untouched.
- **`mushroom-foraging` (soppsanking) vocabulary concept** — neither DBpedia "mushroom
  hunting" nor Wikidata `Q2391676` carries the canonical Norwegian term, so it is pinned
  in `vocabulary.yaml` with nb altLabels (soppsanking, sopplukking, …) under `outdoor`.
- **Observations from the server no longer go missing after a failed sync** — the
  product database is appended to both by the running service and by hand, and the
  two copies used to have to be reconciled by hand on every collision. Git now
  resolves them automatically using the same rules the service applies when it
  stores an observation, so routine simultaneous updates are no longer a conflict.
  See `DEPLOYMENT.md` for the one-time setup a fresh clone needs.

### Changed
- **What the code called the "package vocabulary" is now just the vocabulary** — nothing has been bundled as a package for some time, and the name misled on every read. Docstrings only.

- **155 products and 115 price observations recovered from the server** — the deployed
  instance had been auto-committing to its own branch for seven weeks without pulling,
  so its observations never reached the shared database. They are now merged in: 92
  prices belong to the newly arrived products and 23 were added to products already
  known. Where the server and the database disagreed on a product's name or categories
  (38 names, 16 category lists), the database's wording was kept and the server's was
  discarded rather than combined, so a few more specific labels the server had are
  worth re-checking by hand.
- **A repeated receipt name now widens its recorded date range in both directions** —
  seeing a name again advanced `last_seen` but never moved `first_seen` earlier, so an
  earlier sighting of an already-known name was silently dropped. This affects every
  stored observation from now on, not only the recovery above.
- **Universal English final fallback for SKOS label lookup** — `_LANGUAGE_FALLBACKS`
  only covered the Scandinavian cluster, so a language whose index missed the
  English-derived lookup label (e.g. `it`) dropped sources like Wikidata and returned
  sparse, language-dependent results. A new `_fallback_langs(lang)` helper appends `en`
  to every chain (empty for `en` itself), making `/api/lookup` results language-invariant.
  See `docs/language-fallback-findings.md`.

### Fixed
- **Bogus yogurt price removed** — Pilos yogurt 3.6% 1kg (`4056489941200`) carried two
  prices for 2026-07-31 at the same shop, 1.53 and 0.66 EUR; the 0.66 was dropped.
- **A concept can no longer become its own ancestor** — `/api/vocabulary/resolve`
  registers every input label's source URIs, so resolving one label (e.g. `rope`)
  could bridge a shared upstream URI back to a sibling label that is only a case or
  plural variant of itself (`Rope`, `ropes`), and the synonym chains `red lentils →
  lentil → lentils` formed the same way. The self-reference filter in
  `_build_broader_from_paths` was case-sensitive and only covered the URI-bridged
  segments, so these variants slipped into `broader`. Downstream, after a client
  normalises case/plurals, the links collapsed into cycles (`lentil` ⇄ `lentil`,
  `rope` ⇄ `rope/cord`) that crashed inventory-md's `search.html` with a category-tree
  stack overflow. The filter is now normalisation-aware (case-, separator- and
  plural-insensitive via `_normalise_self_id`) and applied to both the path-parent and
  URI-bridged branches, so the emitted `broader` graph stays acyclic.
- **Films, books and other titles excluded from category lookups** — a query like
  `sopping` fuzzy-matched a Wikidata film (`Q7563193`), leaking a creative-work title
  into the category vocabulary. The Wikidata non-concept filter now rejects common
  creative-work `P31` types (film, book, album, song, video game, …) and their
  creative-work ancestors. Blocking these instance-of values only rejects individual
  titles; the general class concepts are unaffected.

## [v0.14.0] - 2026-06-20

I've been hammering on this project for several months now, forgetting to make releases "on the go".  The following CHANGELOG seems overwhelming, it's AI-generated, probably full of junk, but I believe I'm the only user of this project so I just let it slide through.

### Added

- **Reverse receipt-name lookup** — `GET /api/ean/search?receipt_name=...` returns
  ranked candidate EANs whose stored `receipt_names` observations match the query
  (case- and whitespace-insensitive exact match scores 1.0, otherwise a similarity
  ratio). Optional `shop`, `limit`, and `min_score` filters. Lets a shopping importer
  propose EAN matches for receipts that print only a localised product name.
- **`POST /api/vocabulary/resolve` batch endpoint** — resolves a list of category
  labels to a tailored vocabulary in a single round-trip: each label's matching
  concept plus all ancestor concepts needed to render a complete category tree.
  Labels not in `vocabulary.yaml` are looked up across AGROVOC, DBpedia, and Wikidata;
  SKOS ancestors missing from the vocabulary are bridged in via their path segments,
  and labels that resolve nowhere are returned as minimal `source="inventory"` stubs.
  Synonym and number variants of an input label are folded into the matched concept's
  altLabels (one canonical ID per concept) rather than emitted as separate sibling
  concepts.  Preferred over `GET /api/vocabulary` for clients that need only a subset.
- **Scandinavian language fallback for SKOS label lookup** — when a concept lookup
  fails for the primary language (e.g. `nb`), related Scandinavian variants
  (`no`, `da`, `nn`, `sv`) are tried before giving up.  Fallback chains are symmetric
  and the fallback language is also used for `build_hierarchy_paths` so paths are
  consistently derived.
- **SIGUSR1 toggles DEBUG logging at runtime** — send `kill -USR1 <pid>` to switch
  the tingbok logger between INFO and DEBUG without restarting the service.  The
  tingbok logger now also has its own `StreamHandler` so INFO+ application messages
  are visible in the systemd journal (previously silenced by uvicorn's root-logger
  configuration).
- **Auto-commit writable data files to git (event-driven, debounced)** — new
  `TINGBOK_DATA_DIR` env var points to a stable, git-tracked location for
  `vocabulary.yaml` and `ean-db.json`.  When set, `PUT /api/ean/{ean}` and
  `PUT /api/vocabulary/{concept_id}` schedule a debounced git commit (default 10 s)
  after each write; on graceful shutdown any pending changes are committed immediately.
  The remote IP is included in the commit message for traceability.
- **`scripts/clean_skos_cache.py`** — maintenance script that removes junk entries
  (list articles, disambiguation pages) from the SKOS cache and reports bad
  `source_uris` in `vocabulary.yaml`.
- **`scripts/clean_vocabulary.py`** — normalises `http→https`, removes duplicates and
  junk URIs from `source_uris` in `vocabulary.yaml`.  `--check-types` also removes
  person/place/disambiguation URIs by fetching RDF type information from DBpedia and
  Wikidata.  Both maintenance scripts resolve the default cache directory from
  `TINGBOK_CACHE_DIR` or `/etc/tingbok/tingbok.conf` before falling back to
  `~/.cache/tingbok`.
- **`tingbok.services.skos.is_junk_uri(uri)`** — public pattern-only check (no network)
  for list articles and disambiguation pages.  Replaces the two private pattern helpers
  that were previously duplicated in internal callers.
- **`tingbok.services.skos.is_non_concept_uri(uri, cache_dir=None)`** — public full
  check including network fetches from DBpedia and Wikidata; returns `None` on network
  error so callers never silently drop URIs they could not verify.  When `cache_dir` is
  provided, results are stored in the SKOS cache (keyed `type_check:{uri_hash}`) so
  re-running maintenance scripts does not re-fetch types that were already resolved.
- **`PUT /api/vocabulary/{concept_id}` endpoint** — creates or updates a
  vocabulary concept and persists changes to `vocabulary.yaml`.  All body
  fields (`prefLabel`, `labels`, `altLabel`, `add_source_uris`,
  `remove_source_uris`, `add_excluded_sources`, `remove_excluded_sources`) are
  optional.  When the concept ID contains path separators (e.g.
  `food/roes/caviar`), any missing ancestor concepts are created automatically.
  Changes are reflected immediately in the in-memory vocabulary.
- **`condense-vocabulary` CLI subcommand** — strips redundant `broader` and
  `narrower` entries from `vocabulary.yaml`.  `narrower` is removed from all
  non-`_root` concepts (recomputed at load time from the inverse of `broader`).
  `broader` is removed from path-style concept IDs when its value matches the
  parent path segment (e.g. `food/dairy: broader: food` is redundant because the
  hierarchy is inferred from the concept ID at startup).  Supports `--dry-run`.
- **vocabulary.yaml is now self-healing at load time** — `_load_vocabulary` infers
  `broader` from the concept ID path for any path-style concept that lacks an
  explicit `broader` (e.g. `food/dairy` → `broader: [food]`), and recomputes all
  `narrower` lists as the inverse of the established `broader` relationships.
  Explicit `_root.narrower` (which defines top-level ordering) is preserved as-is.

### Changed

- **Singular/plural inflection consolidated into `tingbok.text.number_variations`** —
  the AGROVOC/SKOS, OFF and GPT lookups previously each carried their own copy of the
  heuristic (one of which mis-handled e-ending nouns, turning `juices` into `juic`
  instead of `juice`).  They now share one implementation owned by tingbok, so OFF and
  GPT label lookups also benefit from the corrected rules.

- **Default SKOS cache TTL raised from 60 to 90 days; refresh divisor from 100 to
  200** — the background cache refresh loop now wakes up less frequently; both values
  remain configurable via `TINGBOK_CACHE_MAX_AGE_DAYS` and
  `TINGBOK_CACHE_REFRESH_DIVISOR` environment variables.

- **`/health` now exposes uptime and vocabulary enrichment progress** — the response
  includes `uptime_seconds`, `vocabulary_concepts`, and `vocabulary_concepts_enriched`
  fields for all clients.  `cache_oldest_entry_age_days` is added for localhost clients
  alongside the existing `paths` dict, making it possible to verify that the cache
  refresh cycle is working.

### Fixed

- **Singular and plural labels now resolve to the same concept** —
  `GET /api/lookup/fruit-juice` and `/api/lookup/fruit-juices` returned different
  concepts: the singular matched the `fruit juice` altLabel of the `juice` concept,
  while the plural fell through to a network SKOS lookup.  The singular/plural step in
  `_lookup_in_vocabulary` now generates number variations of every separator form
  (so `fruit-juices` is tried as `fruit juice`) and — like the exact-match step —
  also consults the runtime-enriched label caches (`_fetched_labels` /
  `_fetched_alt_labels`), where altLabels such as `fruit juice` live.
- **Cache refresh loop now handles legacy entries written before `_cache_key` was added** —
  ~7 800 SKOS cache files (up to 55 days old) were silently skipped by
  `_find_oldest_cache_entry` because they lacked the `_cache_key` field, causing the
  loop to only see 13-day-old entries and sleep ~9 hours between each check.  A new
  `_infer_cache_key()` helper reconstructs the key from the SHA-256 hash embedded in
  the filename: for `labels`/`alt_labels`/`description` entries the key is rebuilt from
  `uri`+`source` stored in the data; for `concept` entries the first three underscores
  in the filename stem are replaced with colons.  Non-inferable entries (OFF/GPT caches
  with `off:`/`gpt:` URIs) continue to be skipped to prevent spin-loops.  On a
  successful refresh, the key is written back so future scans use the normal path.
- **DBpedia disambiguation pages are now filtered at lookup time** —
  `http://dbpedia.org/ontology/DisambiguationPage` is added to `_DBPEDIA_BLOCKED_TYPES`
  so new lookups reject them immediately.  Stale cached disambiguation entries are also
  evicted on the next cache hit.
- **Three bug fixes**:
  - `--version` flag added to the CLI argparser.
  - Norwegian label lookup cache: cached SKOS labels fetched for one language variant
    (e.g. `no`) are now returned when the same concept is queried under a related
    variant (e.g. `nb`), avoiding unnecessary upstream round-trips.
  - OFF missing categories: `_parse_off` now falls back to non-English category tags
    (e.g. `de:`) when no `en:`-prefixed tags are present; fixes German Lidl products
    that returned empty category lists.
- **EAN category strings are now normalised against the vocabulary** — after an
  EAN/barcode lookup the raw category strings returned by upstream sources
  (Open Food Facts, UPCitemdb, Open Library, nb.no) are matched
  case-insensitively against vocabulary concept IDs, prefLabels, altLabels, and
  path-segment aliases.  Matched categories are replaced with the canonical
  concept ID (e.g. ``"spreads"`` → ``"spread"``); unmatched categories are kept
  as-is.  The lookup index is built lazily on first use.
- **DBpedia results typed as persons or geographical places are now filtered out** —
  results whose RDF types include `dbo:Person`, `dbo:PopulatedPlace`,
  `dbo:NaturalPlace`, or the equivalent `schema.org` and `foaf` types are
  silently rejected.  This prevents cities, politicians, and mountains from
  appearing as source URIs in vocabulary concepts.
- **Wikidata results that are persons or geographic places are now filtered out** —
  after finding a candidate entity the lookup now fetches its P31 (instance of)
  and P625 (coordinate location) claims in a single `wbgetentities` request.
  Entities whose P31 includes humans (Q5), cities/towns/villages, islands,
  mountains, rivers, lakes, geographic regions, or Wikimedia
  disambiguation/list pages are rejected; entities with a P625 coordinate
  location are also rejected.  This shares the existing `wbgetentities` call
  used for P279 (subclass-of) hierarchy fetching, so no extra round-trips are
  added.
- **Book lookups always include `"book"` in categories** — Open Library and nb.no results
  previously returned an empty category list when no subjects were available.  `"book"` is
  now appended to the categories list for all ISBN lookups.
- **`populate-uris` no longer adds http/https duplicates** — the command now normalises
  `http://` to `https://` when comparing discovered URIs against existing ones, preventing
  duplicate entries when a source switches scheme between runs.

## [v0.13.0] - 2026-03-10

Lots of changes - still trying to get the category system in inventory-md to work reasonably well.

### Added

- **GPT and OFF added to `/api/lookup`** — the unified concept lookup now queries the
  Google Product Taxonomy (GPT) and Open Food Facts taxonomy (OFF) alongside AGROVOC,
  DBpedia, and Wikidata.  GPT contributes hierarchy path derivation and multilingual labels;
  OFF contributes `off:` source URIs and food labels/altLabels.  GPT lookup also tries
  singular/plural variants when an exact label match fails.
- **`source_paths` field on `VocabularyConcept`** — carries tingbok-normalised hierarchy
  paths per source (e.g. `{"gpt": "food/beverages"}`), populated at serve time from
  GPT URI-based lookup.
- **Language-specific `path_aliases` on `VocabularyConcept`** — allows e.g. `klær/vinter`
  (Norwegian) to resolve to `clothing/thermal` when queried with `lang=nb/no/nn`, without
  accidentally matching other languages.
- **`prune-cache` CLI subcommand** — removes the oldest cache entries until the cache is
  below a target size, using the new `_last_accessed` timestamps to determine eviction order.
- **Cache access tracking** — `_save_to_cache` now records `_last_accessed` on every write
  and `_load_from_cache` updates it on every hit, enabling LRU-style `prune-cache` behaviour.
- **On-demand label fetching in `GET /api/vocabulary/{concept}`** — the single-concept
  endpoint now fetches labels, altLabels, and descriptions eagerly when the background task
  has not yet processed that concept, so it never returns incomplete data after a fresh start.
- **`/health` exposes file paths for localhost** — when the request originates from
  `127.0.0.1` / `::1`, the health response includes resolved paths for the data dir,
  cache dir, EAN observations file, etc.
- **DBpedia hierarchy dead-end blacklist** — when DBpedia traversal reaches abstract
  Wikipedia meta-categories (property, quantity, scalar, ratio, elements of music, …)
  the branch is silently discarded rather than chasing the chain to max depth.
- **`PUT /api/ean/{ean}`** — new endpoint that accepts inventory-sourced EAN observations
  (`categories`, `name`, `quantity`, `prices`, `receipt_names`).  Observations are persisted
  to `ean-db.json`, merged into future `GET` responses (inventory categories take priority),
  and the merged product view is returned immediately.
- **`GET /api/ean/{ean}`** now also merges runtime `ean-db.json` observations into responses.
- **AGROVOC labels and altLabels served from local Oxigraph store** — previously the server
  fell back to the AGROVOC REST API even when the local NT store was loaded.  SKOS-XL
  `prefLabel` and `altLabel` are now queried directly via Oxigraph SPARQL, eliminating
  unnecessary online lookups during parse runs.
- **AGROVOC Oxigraph store loaded in background thread at startup** — the server now
  responds immediately after startup using the AGROVOC REST API as fallback while the
  store is loading.  Once loading completes, all label/alt-label/lookup calls switch to
  the local store automatically.
- **INFO log when AGROVOC REST API is used** — operators can now see the reason
  ("still loading" or "store unavailable") in logs when the server goes online for AGROVOC.
- **`/api/lookup` source-conflict warnings** — when SKOS sources return hierarchy
  paths under different top-level roots (e.g. AGROVOC: ``livestock/bedding``,
  DBpedia: ``household/bedding``), a warning entry is appended to
  ``{TINGBOK_CACHE_DIR}/lookup-warnings.json``.  Operators can inspect this file to
  add ``excluded_sources:`` entries for ambiguous concepts in ``vocabulary.yaml``.

### Changed

- **Cache: background refresh loop replaces TTL-based expiry** — `_load_from_cache` no
  longer evicts stale entries; a background task (`cache_refresh_loop`) proactively
  refreshes the oldest entry, so stale data is always served immediately while a fresh
  fetch happens in the background.  The `_cache_key` is stored alongside cached data so
  the refresh loop knows which upstream function to call.
- **EAN data unified into `ean-db.json`** — `ean-db.yaml` is no longer loaded at startup;
  all EAN data (including previously manual entries) flows through `PUT /api/ean/{ean}` and
  is persisted in `ean-db.json`.  `ean-db.yaml` remains in the repo as an archived reference.
- **`ean-db.json` moved from cache dir to data dir** — inventory observations are persistent
  data, not a cache artefact; the file now lives alongside `vocabulary.yaml` so it survives
  cache clears and is visible to git.
- **Source URIs normalised to `https://`** — all `http://` DBpedia, Wikidata, and AGROVOC
  URIs in `vocabulary.yaml` are converted to `https://`; the `_normalise_uri()` helper also
  normalises URIs discovered at runtime.
- **`GET /api/vocabulary` returns 503 Retry-After while enrichment is incomplete** — avoids
  silently serving partial data (missing translations/descriptions) while the background
  label-fetch task is still running.  The single-concept endpoint is unaffected (fetches on-demand).
- **`prune-vocabulary` also removes redundant `altLabel` entries** — fetches alt-labels from
  source URIs at runtime and removes any `altLabel` entries in `vocabulary.yaml` that are
  already provided by those sources, keeping the YAML minimal.
- **Transient upstream failures now cached with a short TTL** — when DBpedia, AGROVOC, or
  Wikidata returns a timeout or connection error, `lookup_concept` adds a transient entry
  to the not-found cache (4-hour TTL, marked `"transient": true`) so the same query does
  not time out on every parse run.

### Fixed

- **Wikidata API updated from v0 to v1** — the v0 endpoint now returns 404; descriptions,
  labels, and aliases all updated to the v1 URL.
- **`GET /api/vocabulary/{concept_id:path}` now handles slash-containing IDs** — concepts
  like `food/spices` previously returned 404 because the path parameter stopped at the first
  slash.
- **`/api/lookup` matches vocabulary concepts via runtime-fetched labels** — the vocabulary
  pre-check now also searches `_fetched_alt_labels` and `_fetched_labels` (synonyms and
  translations fetched from external sources), so e.g. `spices` correctly resolves to
  `food/spices` rather than falling through to an unrelated AGROVOC concept.
- **`lookup_concept` prefers vocabulary-anchored paths for canonical concept ID** — when
  AGROVOC returns multiple paths for the same concept, the path whose deepest ancestor
  segment exists in `vocabulary.yaml` is selected as canonical.
- **`PriceObservation.date` made optional** — inventory entries may not carry an explicit
  observation date; `None` means date unknown.
- **Fix misplaced `@asynccontextmanager`** — decorator slipped onto `_cache_refresh_config`
  instead of `lifespan`, causing startup to fail with `TypeError`.
- **`ReceiptNameObservation.shop`** is now optional (`str | None = None`); previously
  required, causing 500 errors for EAN entries that had a receipt name but no shop.
- **`GET /api/ean/{ean}` injects `ean` into manual-only results** — `source: manual` entries
  don't contain the EAN (it is the YAML key), which caused 500 validation errors.
- **`_lookup_dbpedia` HTML stripping** — DBpedia Lookup API sometimes returns
  HTML-highlighted labels; tags are now stripped before storing.
- **`_lookup_dbpedia` and `_lookup_wikidata` similarity threshold** — the first result is
  now accepted only if its label has ≥ 60 % token-set-ratio similarity to the query,
  preventing nonsensical mappings like `mounting tool` → `list_of_naruto_episodes`.
- **DBpedia 'List of …' articles filtered from hierarchy** — ``List_of_*`` and
  ``Lists_of_*`` URIs are no longer followed when building broader-concept chains.
- **GPT locale corrected from `nb-NO` to `no-NO`** — the Norwegian taxonomy file uses
  `no-NO` locale in its filename.
- **EAN save `PermissionError` now logged at ERROR level** and re-raised so callers get a
  500 rather than a silent success.

### Data

- **`hardware/nut` concept added** — covers hex/wing/lock nuts (fasteners); `nut`/`nuts`
  altLabels added to `fasteners`; food/nuts hierarchy corrected (peanuts, cashews, coconut
  moved under `food/nuts`; coconut added).
- Various vocabulary improvements: spices hierarchy, salt, potatoes, roes/caviar, clothing
  labels, altLabel cleanup, long-johns placement, source URI corrections.

## [v0.12.0] - 2026-03-07

### Added
- **`data/ean-db.yaml`** — git-tracked store for manually curated and locally observed
  EAN/ISBN product data.  Two kinds of entries: `source: manual` for products not found
  in any upstream source (tools, hardware, etc.), and supplementary-only entries that add
  locally-observed data on top of what upstream sources provide.
- **`PriceObservation`** model — `{shop, date, price, currency, unit}` — records a
  single observed price at a shop on a date.
- **`ReceiptNameObservation`** model — `{shop, name, first_seen, last_seen}` — records
  how a product name appears on a shop receipt, with an observation period.  Receipt names
  can differ by shop and locale (e.g. Lidl Bulgaria vs. Lidl Germany).
- **`ProductResponse.prices`**, **`.receipt_names`**, **`.note`** fields — new optional
  fields on `ProductResponse` for locally observed data.
- **`services/ean.load_manual_ean(path)`** — loads `ean-db.yaml` at startup.
- **`services/ean.merge_manual_data(upstream, manual)`** — merges upstream product data
  with a manual entry: supplementary fields are added to upstream results; `source: manual`
  entries are returned as-is when no upstream data exists.
- **`scripts/migrate-ean-cache.py`** — one-time migration script that converts an
  existing `ean_cache.json` (flat dict format) to tingbok formats: per-file upstream cache
  entries, `ean-db.yaml` for manual/supplementary data, and `_not_found.json` for nulls.

### Changed
- **`GET /api/ean/{ean}`** now merges `ean-db.yaml` data into every response.
- **`_save_to_cache` and `_add_to_not_found_cache`** in `services/skos.py` — `mkdir()`
  moved inside the `OSError` handler so a permission failure is logged and silently skipped
  rather than propagating as an unhandled exception (this was preventing the EAN cache
  directory from being created on the server).

## [v0.11.0] - 2026-03-07

### Added
- **`GET /api/lookup/{label}`** — unified concept lookup endpoint that returns
  `VocabularyConcept` format regardless of whether the concept is in `vocabulary.yaml`.
  Checks vocabulary by ID and by `prefLabel`/`altLabel` (case-insensitive) first; if not
  found, queries AGROVOC, DBpedia, and Wikidata **in parallel** and merges the results:
  labels (first-per-language), `altLabel` (union), description (longest wins), and
  `source_uris` (union).  Returns 404 only if all sources miss.
  The canonical concept ID is derived from the hierarchy path (e.g. `food/spices/cumin`).
- **EAN/ISBN product lookup service** (`services/ean.py`) — multi-source lookup with 60-day
  caching.  ISBNs (978/979 prefix) are routed to Open Library, with a fallback to nb.no for
  Norwegian titles.  Other EAN/UPC codes are routed to Open Food Facts, with a fallback to
  UPCitemdb.
- **`ProductResponse.author`** (optional `str`) and **`ProductResponse.type`** (`"product"`
  or `"book"`) fields added to `models.py`.

### Changed
- **`GET /api/ean/{ean}`** now uses the new multi-source service; book lookups return
  `type="book"` and `author` when available.

### Refactored
- **`_vocabulary_concept_from_data(concept_id, data)`** helper extracted in `app.py` to
  remove ~20 lines of duplication between `get_vocabulary()` and `get_vocabulary_concept()`.

## [v0.10.0] - 2026-03-06

### Added
- **`get_alt_labels(uri, languages, source, cache_dir)`** in `services/skos.py` — fetches
  alternative labels (synonyms) for DBpedia (via `skos:altLabel`), Wikidata (aliases
  endpoint), and AGROVOC (graph `altLabel`); results are cached separately from
  preferred-label translations.
- **`get_alt_labels(uri, languages)`** in `services/off.py` — returns synonym lists from
  the in-memory OFF taxonomy (using `node.synonyms`).
- **`_fetched_alt_labels`** module dict in `app.py` — populated by
  `_fetch_labels_background()` alongside `_fetched_labels`; maps concept_id to
  `{lang: [altLabel, ...]}`.
- **`_build_alt_labels()`** in `app.py` — merges static `altLabel` entries from
  `vocabulary.yaml` with source-fetched synonyms; deduplicates and excludes the
  `prefLabel` value.

### Changed
- **`VocabularyConcept.uri`** is now always the canonical tingbok URL
  (`https://tingbok.plann.no/api/vocabulary/{id}`).  External source URIs continue to
  appear in `source_uris`.
- **`GET /api/vocabulary` and `GET /api/vocabulary/{id}`** now return merged `altLabel`
  including synonyms fetched from DBpedia (`skos:altLabel`), Wikidata (aliases endpoint),
  AGROVOC (graph `altLabel`), and OFF (taxonomy synonyms).

## [v0.9.0] - 2026-03-06

### Added
- **MCP (Model Context Protocol) support** — `fastapi-mcp` is now a main dependency;
  an MCP server is mounted at `/mcp`, automatically exposing vocabulary, SKOS lookup,
  and EAN endpoints as MCP tools.  The health and cache-stats endpoints are excluded.
- **Root endpoint `GET /`** — returns HTML (with links to GitHub and `/docs`) or JSON
  service info (version, description, links) depending on the `Accept` header.
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
  `prefLabel` is always used as the canonical English label.
  Descriptions from sources are used as fallback when `vocabulary.yaml` has none.
- **`prune-vocabulary` CLI subcommand improvements**:
  - Source name is now included in deviation output (e.g. `dbpedia says "Footwear"`)
    so it is clear which source disagrees with the vocabulary.
  - Inter-source conflict detection: deviations where two sources disagree with each other
    (but not with the vocabulary) are reported separately.
  - Near-match suppression (rapidfuzz, threshold 85): plural/singular and minor spelling
    variants are silently accepted rather than reported as deviations.
  - `altLabel` suppression: if the source label matches an `altLabel` entry for that
    language, the deviation is suppressed (valid synonym, not an error).

### Changed
- **`rapidfuzz`** added as a main dependency (used by `prune-vocabulary` for near-match
  suppression).
- **`fastapi-mcp`** added as a main dependency.

### Fixed
- **`_build_labels()` in `app.py`** — `prefLabel` is now always used as the canonical
  `en` label, overriding any conflicting value fetched from external sources.
- **URI auto-discovery** — concepts that already have some `source_uris` entries but are
  still missing a URI for a particular source are no longer skipped; discovery now runs
  for each source independently.

### Data
- **`vocabulary.yaml` URI cleanup** — removed ~10 wrong source URIs (e.g. `dbpedia:Beetle`
  on food, `dbpedia:Goaltender` on babyutstyr, `dbpedia:British_Columbia` on recreation,
  `dbpedia:Bungalow` on hobby, `dbpedia:Equipment` on outdoor, etc.) and corrected
  `off:en:tripe` on pen (animal offal, not a writing instrument).
- **`vocabulary.yaml` GPT URIs** — added ~18 `gpt:` URIs across food, household,
  hardware, medical, outdoor, office, and other categories.
- **`vocabulary.yaml` altLabels** — added plural/singular and cross-language synonym
  entries for food, tools, clothing, and office categories (nb/de/fr/es/it/nl/pl/ru/uk)
  to suppress spurious `prune-vocabulary` deviations.

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
