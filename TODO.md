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

* **`GET /api/concept/{id}/ancestors`** — the cheap, immediate win, and worth
  doing well before canonical URLs land. It gives "is soybeans food?" a single
  authoritative answer, so clients can stop walking the tree themselves.
* **Serve a pre-built tree**, not just a flat list, so `build_category_tree()`
  becomes glue rather than a second implementation.
* **Own the source names.** `_SOURCE_LABELS` (`"off"` → `"OpenFoodFacts"`) and
  `_uri_to_source()` (URI scheme → source name) live in inventory-md, so adding
  a source to tingbok silently requires a client release. tingbok is the
  authority on which sources exist; it should say so over the API.
* **Own the language fallback chains.** inventory-md collapsed its own three
  copies into one on 2026-06-12, but tingbok still hardcodes a separate copy in
  its services. Language knowledge is tingbok's; return the chain as part of the
  vocabulary response or from a dedicated endpoint.

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

