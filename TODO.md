Tingbok owns the category *data* — the vocabulary, the sources, the hierarchy,
the translations.  How an inventory consumes it is `~/inventory-md/docs/TODO.md`.
The items below marked *(from inventory-md)* were migrated on 2026-08-13 from
that project's `docs/TODO-CATEGORIES.md`, which has been retired.

## Consistency

* I think that a while ago, logic was added to make it possible to simplify vocabulary.yaml, lists of paths would obsolete the need of having lists of broader and narrower for every concept.  I.e., by listing `food/staples/potatoes` and `food/vegetables/potatoes` it should not be needed to list vegetables and staples as broader for potatoes, etc.  Look into this again and use it consistently in vocuabularay.yaml to keep it DRY.
* Sometimes canonical IDs are given like a path with a slash, and sometimes they are given as a single word.  I'd like some consistency here too.  Perhaps it's good to keep slashes in canonical IDs in case the same word gets added for a different concept at some point in the future, but it's probably not needed with many parts in the canonical ID.  It could be `staples/potatoes` or `food//potatoes`, but not `food/staple/potatoes` maybe.  We'd also need some heuristics to relatively deterministicly find a canonical ID of a concept that isn't defined in vocabulary.yaml but decided from the sources.
* Dashes vs underscores vs spaces in the canonical ID.  It seems to be a bit arbitrary now.  I think we should stick to dashes.
* Plural vs singular.  We should have some consistency there, too.
* *(from inventory-md)* **A canonical name rule.**  A category has many names even
  within one language and disregarding aliases: potato / potatoes /
  `food/vegetable/potatoes` / `food/staples/potatoes`; seal as `food/meat/seal`,
  `toy/stuffed/seal` and `hardware/plumbing/seal`.  Proposed rule: take as many
  legs of the path as needed to disambiguate, counting **from the end** — so
  `soybeans` and `potatoes`, but `meat/seal`, `toy/stuffed/seal`,
  `plumbing/seal`.  The inventory should not have to write full paths, should not
  see random-looking IDs, and the name should be localisable.
  The Norwegian inventory motivates the same question from the other side: is
  `jul/belysning` a different category from `belysning`, or the same one under a
  seasonal path?  (`jul/julebelysning` avoids the question but reads redundant.)
  Note the specimen is gone — `~/furusetalle9-inventory` now uses
  `elektronikk/belysning`, bare `belysning` and `jul/elektronikk`, no
  `jul/belysning` — so this needs a fresh example before it can be tested.
* *(from inventory-md)* **IDs for categories not in the vocabulary.**  GPT has
  numeric IDs, AGROVOC and Wikidata have UID schemes.  Concepts outside the
  vocabulary cannot have a persistent ID, but could carry a temporary one, kept
  as long as some inventory still uses the category.

## The deployed vocabulary.yaml was never migrated

A deployment with `TINGBOK_DATA_DIR` set keeps its own `vocabulary.yaml`, seeded
from the packaged copy once and never re-seeded (`bootstrap_data_dir` only
copies when the file is absent). Two fixes landed in the packaged file on
2026-09-02 that its copy therefore still lacks:

* `food/spices` declaring `food/conditment` — a missing 's', so it advertises a
  parent that 404s and a client building a tree from `broader` gets an orphan.
* `clothing/underwear` with a scalar `source_uris`, served as 37 one-character
  URIs. The loader and both writers now coerce it, so nothing corrupts further,
  but the bad shape is still on disk.

Neither is fixed by deploying. Either edit tingbok.plann.no's copy directly, or
add a migration step that reconciles a data-dir vocabulary against the packaged
one on startup.

## Data that should be filtered

Recently (86e885b40f01149fe8cd263a841eb98933eb674f/b679cc6df936d3d8e0b9926afbbef4bb847f656f/1aaed1ed028dfa5ef7e93e2abe2e315712aab7f6) logic was added to filter away non-thingy concepts

Either the category source "is it a thing?" filtering does not work, or the filters should be sharpened.  https://tingbok.plann.no/api/lookup/teddy gives https://www.wikidata.org/wiki/Q18010041 which is a "given name" (instance of https://www.wikidata.org/wiki/Q12308941 - male name - which is a subclass of https://www.wikidata.org/wiki/Q202444 - given name - subclass of https://www.wikidata.org/wiki/Q10856962 - antrophonym.  None of those should be used as sources. The correct node is https://www.wikidata.org/wiki/Q213477

It also gives https://dbpedia.org/page/Teddy_Stadium - again, https://dbpedia.org/page/Teddy_bear is the correct.  Teddy Stadium has attributes like dbp:tenants `georss:point` `geo:geometry` `geo:lat` `geo:long`, `dbo:buildingStartDate`, I think any dbpedia article with any of those attributes should be disqualified as it's most likely not a thing one would have in a domestic inventory.

## EAN pattern matching

Sometimes the EAN itself cannot be looked up, but the first digits can still tell a lot of information.  Add some logic here.

The barcode lookup script in ~/inventory-md/scripts should be updated to use tingbok

## Canonical tingbok URLs *(from inventory-md, important)*

Concepts should be identified by a canonical tingbok URL, and clients should
fetch categories *by* that URL starting from the virtual `_root` category, rather
than pulling the whole of `/api/vocabulary`.  There is no such thing as a
canonical tingbok URL today.  See `docs/canonical-urls.md` for the proposed
scheme and the redesigned batch-resolve API.

Partly landed already: inventory-md calls `POST /api/vocabulary/resolve` with the
labels it actually uses (`resolve_vocabulary_from_tingbok()`), which is the
batch-resolve half.  The canonical-URL half is untouched.

Some concepts in `vocabulary.yaml` carry a `uri` field; that value may safely be
overwritten by the canonical tingbok URL.

Also consider a canonical — though not necessarily persistent — tingbok URL for
*cached* concepts, i.e. ones resolved from sources rather than declared in
`vocabulary.yaml`.

## Serve hierarchy answers instead of making clients compute them *(from inventory-md)*

From `~/inventory-md/docs/code-review-2026-05-08.md`, still open at the
2026-06-11 review, which called it "the biggest architectural ROI".

`GET /api/vocabulary` returns a flat concept list that every client then has to
re-structure, so hierarchy knowledge leaks outward and gets reimplemented — and
reimplemented subtly differently each time. inventory-md's `vocabulary.py`
carries a `build_category_tree()` with its own rules for inferred hierarchy and
stub nodes; if tingbok changes how hierarchy works, that breaks silently.

* ~~**`GET /api/concept/{id}/ancestors`**~~ — **done 2026-09-01**, as
  `GET /api/ancestors/{concept_id}`. Breadth-first over `broader`, all parents
  reported, cycles broken, and a concept with no `broader` falls back to its
  path prefix when that prefix is itself a concept. It went through
  `/api/vocabulary/{id}/ancestors` first and that was wrong: concept ids are
  paths, so it collides with a concept called `x/ancestors`, and resolving the
  collision either way leaves one of the two with no URL at all.
* **Serve a pre-built tree**, not just a flat list, so `build_category_tree()`
  becomes glue rather than a second implementation. This is the one that would
  actually let the client delete code — but note what the ancestors endpoint did
  *not* buy: inventory-md generates a `vocabulary.json` that a static web UI
  reads with no server in the loop, so `build_category_tree()` has to keep
  working with tingbok unreachable no matter what this API grows. Serving a tree
  makes tingbok authoritative; it does not delete the client's copy.
* ~~**Own the source names.**~~ **Done 2026-09-01**: `tingbok/sources.py` holds
  the one registry (name, label, hosts or URI prefixes, homepage) and
  `GET /api/sources` serves it. `skos.uri_to_source()` re-exports it and
  `cli.py`'s two inline prefix chains are gone, so tingbok itself no longer had
  three copies either. Note this partly settles **"Use https for source URIs,
  always"** under *Source handling* below: matching on host means the spelling
  no longer changes how a URI is classified. Normalising what is *stored* is
  still open.
* **Own the language fallback chains.** ~~inventory-md ... still hardcodes a
  separate copy~~ — re-checked 2026-09-01 and there is no duplication left to
  remove: inventory-md deleted its language-fallback subsystem entirely (v0.15.0
  "Removed"), and tingbok's `_LANGUAGE_FALLBACKS` in `app.py` is now the only
  copy anywhere. Serving it over the API would be building an endpoint with no
  caller. Leave it until something asks.

The client-side deletions this enables are tracked in
`~/inventory-md/docs/TODO.md`.

## 132 tingbok-sourced concepts have no parent *(from inventory-md)*

Measured in `~/solveig-inventory/vocabulary.json` on 2026-08-13 (generated
2026-08-06 against a tingbok returning 200): 342 of 1736 concepts sit at the root
of the tree, 132 of them tingbok-sourced.  So a fifth of the hierarchy is not a
hierarchy, and the inventory's category browser shows `comma_splice`, `brunost`
and `dolmas` as top-level entries.

This replaces an older, vaguer claim ("quite some regressions after the latest
rounds of work") that was carried for months without a number attached.  The
remaining 89 unparented concepts are inventory-sourced — labels tingbok resolved
to nothing — and are tracked in inventory-md.

Re-measured 2026-09-01 against the same file: 353 roots of 1736, of which 134
tingbok-sourced, 121 inferred and 98 inventory-sourced.  Drifting upward, i.e.
nothing has been eroding it.

## Split/combine source concepts *(from inventory-md)*

Some sources lump things together (spices + herbs; underwear and socks), others
keep them apart, and sometimes the lumping is only a tree node — GPT's "Underwear
and socks" has socks and underwear as children.

Sometimes a tingbok concept should combine several source URIs *from the same
source*: specifically "long johns" (Q2472769) and "longs" (Q56303142) should be
one category.  The other cases can probably be handled in the vocabulary as it
stands — if two tingbok concepts reference the same source URI, that suggests a
parent concept referencing the source.  An "underwear and socks" node can be
defined in the vocabulary with other sources excluded, with socks and underwear
as children.

## Source handling *(from inventory-md)*

* **Treat sources equally.**  The old model had a prioritised source list; the
  opinion recorded against it is that sources should be treated more or less
  equally instead.  Wikidata in particular is not special — it is one source
  among several.  Check whether any prioritisation survives in tingbok; the
  source-specific logic is all gone from inventory-md, so tingbok is the only
  place it can still live.
* **Use https for source URIs**, always — some are still http.
* `/api/skos/lookup?label=clothing` defaults to AGROVOC only; it should probably
  look in all sources.  Possibly moot — check whether the lookup endpoint is
  needed at all once batch-resolve is in place.
* **Rate limiting** should be built in.  niquests ships something usable —
  see `Retry` in `niquests.packages.urllib3.util`.

## Caching and warnings *(from inventory-md)*

* `/api/lookup` results are cached by the underlying SKOS service, per
  concept/label under `~/.cache/tingbok/skos/`.  Splitting lookup results into
  their own cache directory for easier inspection was requested and not done —
  confirmed still not done on 2026-08-13 (`~/.cache/tingbok/` holds only `ean/`
  and `skos/`).
* **Translation warnings** — e.g. "bedding" as animal litter vs. household
  bedding — should be generated at lookup time and written to a separate
  YAML/JSON file on the server.

## Google Product Taxonomy: compare against the vocabulary *(from inventory-md)*

GPT is fully implemented — `gpt.py`, the `gpt:{id}` URI scheme,
`download-taxonomy --gpt`, wired into `populate-uris` and startup label fetching,
54 `gpt:` URIs in `vocabulary.yaml`.

Remaining: how does GPT compare to the tingbok vocabulary?  Are there tingbok
concepts with no `gpt:` URI that should have one?  Some categories may want
remapping.  This needs manual review against the GPT hierarchy.

## Categories missing translations *(from inventory-md, unverified)*

Categories that exist in the vocabulary but match nothing in any source — the
root node "Health & Safety" was the example — need translations supplied locally,
since no source will provide them.

Carried over as-is: this was already marked "needs manual verification, possibly
no longer relevant given current multi-source tracking", and a 2026-08-13 attempt
to verify it against a generated `vocabulary.json` was inconclusive, because that
file is rendered for a single language and does not show which translations exist
upstream.  Verify against `vocabulary.yaml` and the source data instead.

## Data correction *(from inventory-md)*

"Plant-based foods and beverages" is not the same concept as "food" — it is a
subcategory of it.

## Follow-ups from the fastmcp migration (`5755391`)

* **Five MCP tool names changed** and were not pinned: every route with a path
  parameter lost its FastAPI-style suffix (`lookup_ean_api_ean__ean__get` →
  `lookup_ean_api_ean`, likewise `observe_ean`, `lookup_concept`,
  `get_vocabulary_concept`, `get_concept_ancestors`). A client that re-lists
  tools each session will not notice; a hardcoded allowlist breaks.
  `mcp_names={...}` on the `FastMCP.from_fastapi()` call in `app.py` restores
  the old names in one line, if that is ever wanted.
* **Dependency count went from 75 to 101** on a clean install — `fastmcp`'s
  `[client,server]` extras pull in `authlib`, `cyclopts`, `py-key-value-aio`,
  `opentelemetry-api` and `httpx2`. Large surface for one `/mcp` mount; not
  investigated whether a narrower install (e.g. `fastmcp-slim` with a smaller
  extras set) would do.
* **`README.md` still does not document the `/mcp` endpoint** — pre-existing,
  it was never documented under `fastapi-mcp` either.

