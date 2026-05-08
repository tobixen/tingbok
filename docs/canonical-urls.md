# Canonical Tingbok URLs and the Vocabulary API

## Background

Tingbok is not a fixed, authoritative taxonomy — it is a *service* that resolves
categories drawn from many external sources (AGROVOC, DBpedia, Wikidata, OFF, GPT) and
augmented by local overrides from each inventory.  Any given inventory may use categories
that no other inventory uses; the vocabulary grows organically as inventories grow.  There
will never be a single downloadable file that covers all possible inventories.

This document covers:

1. How canonical tingbok URLs are constructed and what they mean
2. Why the current `GET /api/vocabulary` download model is a poor fit
3. A proposed "batch resolve" API that better matches how inventories actually work

---

## 1. Canonical URL scheme

### Structure

Every concept that tingbok knows about — whether it originates in `vocabulary.yaml` or is
resolved on demand from external sources — is already identifiable by a URL of the form:

```
https://{server}/api/vocabulary/{concept-id}
```

For example:

```
https://tingbok.plann.no/api/vocabulary/food/nuts/peanuts
https://tingbok.plann.no/api/vocabulary/hardware/nut
https://tingbok.plann.no/api/vocabulary/food
```

Notes:

- Unlike typical linked-data systems (DBpedia uses `/resource/`, Wikidata uses
  `/entity/`), tingbok uses the API path directly as the canonical URI.  This is a
  pragmatic choice: the endpoint is already implemented (`GET /api/vocabulary/{id:path}`)
  and dereferences immediately to full concept data — no redirect needed.
- The `uri` field in every `VocabularyConcept` response is already set to this URL by
  `_vocabulary_concept_from_data()` in `app.py`.
- The concept ID is its hierarchical path, e.g. `food/nuts/peanuts`.  This path is
  meaningful and human-readable, unlike a numeric or UUID-based scheme.
- The URL dereferences: `GET /api/vocabulary/food/nuts/peanuts` returns full concept data
  (labels, broader/narrower, source URIs, etc.) in JSON.

### Why path-based IDs are good enough

Path IDs like `food/nuts/peanuts` are already the primary key in `vocabulary.yaml` and in
all API responses.  Converting them to URLs is purely additive.  There is no need for
opaque numeric IDs: the path *is* the identity.

The one risk is that renames break existing identifiers.  This is acceptable at the
current v0.x stage.  When/if a rename happens, the old URL should redirect to the new one
(`301 Moved Permanently`) so cached references are not silently broken.

### Dynamic/cached concepts

Concepts that are not in `vocabulary.yaml` but are resolved on demand (e.g. a label
looked up via AGROVOC that produces a path like `food/spices/cumin`) should also receive
a canonical URL once they have been resolved.  These are "semi-persistent": retained as
long as an inventory references them (see the cache TTL rules).  Their URL follows the
same scheme — `https://tingbok.plann.no/concept/food/spices/cumin` — even though the
concept does not appear in `vocabulary.yaml`.

### The existing `uri` field

`vocabulary.yaml` already has an optional `uri` field on some root concepts (e.g.
`uri: "https://dbpedia.org/resource/Food"` on the `food` concept).  This is an *external*
source URI, not a tingbok identity.  Once canonical tingbok URLs are implemented, the
`uri` field should be retired and its value moved into `source_uris`.  The canonical
tingbok URL takes over as the primary identity.

---

## 2. Why the current `GET /api/vocabulary` download model is problematic

Today inventory-md does this on every parse run:

```
GET /api/vocabulary  →  receives all ~264 vocabulary.yaml concepts as a flat dict
                         (plus merged labels from DBpedia/Wikidata/AGROVOC)
```

Then locally:
- Tries to match each inventory category label against those 264 concepts
- Calls `GET /api/lookup/{label}` for every label that does not match
- Builds the hierarchy tree itself (`build_category_tree` in `vocabulary.py`)
- Reconstructs broader/narrower links
- Synthesises virtual nodes (`category_by_source`, stubs for intermediate path segments)

Problems:

1. **The download is both too much and too little.**  Most inventories use fewer than 50
   distinct categories; downloading 264 wastes bandwidth and forces the client to index
   a vocabulary it barely uses.  At the same time, categories invented by the user
   (`klær/vinter`, `jul/belysning`) are not in the vocabulary, so the client has to do
   supplementary lookups anyway.

2. **Resolution logic is split across two systems.**  Hierarchy inference, stub creation,
   and altLabel matching are all re-implemented in `vocabulary.py` even though tingbok
   already does this work for `GET /api/lookup/{label}`.

3. **Canonical URLs cannot be used for equality checks.**  Without a URL for each
   concept, the client has no choice but to compare string paths — which breaks when a
   label like `soy-beans` and a path like `food/legumes/soy-beans` refer to the same
   thing but do not match as strings.

4. **The vocabulary is tied to `vocabulary.yaml`.**  The download only reflects concepts
   that are already known to tingbok.  Inventories with locally-invented categories do
   not benefit from it.

---

## 3. Proposed: batch-resolve endpoint

Instead of a flat vocabulary download, inventory-md (or any client) should send the
categories it actually uses and receive back a vocabulary tailored to exactly those
categories.

### Request

```
POST /api/vocabulary/resolve
Content-Type: application/json

{
  "labels": [
    "potato",
    "food/nuts",
    "soy-beans",
    "klær/vinter",
    "jul/belysning",
    "hardware/nut"
  ],
  "lang": "nb"
}
```

`labels` is the complete set of category labels/paths that appear in the inventory.
`lang` is the primary language of the inventory (affects which labels are returned and
how path aliases are resolved).

### Response

```json
{
  "concepts": {
    "food/vegetables/potato": {
      "uri": "https://tingbok.plann.no/concept/food/vegetables/potato",
      "prefLabel": "Potato",
      "labels": {"nb": "Potet", "en": "Potato"},
      "altLabel": {"nb": ["poteter"], "en": ["potatoes"]},
      "broader": ["food/vegetables"],
      "narrower": [],
      "source_uris": ["https://www.wikidata.org/entity/Q16587531"],
      "input_label": "potato"
    },
    "food/nuts": { ... },
    "food/legumes/soy-beans": {
      "uri": "https://tingbok.plann.no/concept/food/legumes/soy-beans",
      ...
      "input_label": "soy-beans"
    },
    "klær/vinter": {
      "uri": "https://tingbok.plann.no/concept/klær/vinter",
      "prefLabel": "klær/vinter",
      "labels": {},
      "broader": ["klær"],
      "narrower": [],
      "source_uris": [],
      "source": "inventory",
      "input_label": "klær/vinter"
    },
    ...
    "food": { ... },
    "food/vegetables": { ... },
    "food/legumes": { ... },
    "klær": { ... }
  },
  "unresolved": []
}
```

Key points:

- **Ancestors are included automatically.**  If the inventory uses `soy-beans`, the
  response includes `food/legumes`, `food`, and eventually `_root` — everything needed
  to render a complete category tree.  The client does not need to do any tree-building.
- **`input_label` maps back.**  Each concept that was directly requested carries the
  original label that resolved to it, so the client knows that `"soy-beans"` and
  `"food/legumes/soy-beans"` are the same thing.
- **Inventory-invented categories get stubs.**  `klær/vinter` is not in any external
  source.  Tingbok creates a minimal stub with `source: "inventory"` and no external
  URIs, but still a canonical URL.  This allows the client to store and compare by URL
  rather than by string.
- **All concepts carry canonical URLs.**  The client can now do hierarchy checks purely
  by URL comparison or by walking the `broader` chain — no local string-matching
  heuristics needed.
- **`unresolved`** lists any input labels that could not be resolved even to a stub (e.g.
  a typo).

### Caching

The response for a given `(labels, lang)` pair can be cached by the client with a
moderate TTL (e.g. 24 hours for production, longer for development).  Since the request
encodes the full inventory vocabulary, a change in the inventory (adding a new category)
simply adds that label to the next request.

On the server side, individual concept lookups are already cached; assembling the response
is just a fan-out over the existing per-concept cache.

### `GET /api/vocabulary` backward compatibility

The existing endpoint can remain as a shorthand for "resolve the full `vocabulary.yaml`
contents" — i.e. equivalent to calling the batch endpoint with all known concept IDs.
This avoids breaking existing clients while the new endpoint matures.

---

## 4. Migration path — status

1. **Done**: `GET /api/vocabulary/{id:path}` dereferences concepts; every
   `VocabularyConcept` response carries `uri` set to the canonical URL.

2. **Done**: `POST /api/vocabulary/resolve` implemented.  Resolves each input label via
   vocabulary (vocabulary-matched: full concept + ancestors; unmatched: stub with
   `source_uris=[]`).  SKOS resolution for unmatched labels is still done client-side
   via `GET /api/lookup/{label}`.

3. **Done**: inventory-md uses batch resolve when parsing.  `resolve_vocabulary_from_tingbok()`
   in `vocabulary.py` calls `POST /api/vocabulary/resolve`; `parse --auto` uses it with
   fallback to `GET /api/vocabulary` for older servers.  EAN-derived labels still use
   `enrich_categories_via_lookup`.

4. **Done**: equality checks in `find_expiring_items.py` and the JS category browser now
   use vocabulary DAG traversal (broader-chain walk) rather than string prefix matching.
   `shopping_list.py` was already correct.

5. **Remaining**: retire the `uri` field in `vocabulary.yaml` (move values to
   `source_uris`) once all clients treat the `/api/vocabulary/{id}` URL as the primary
   identifier.  Also: extend `POST /api/vocabulary/resolve` to do SKOS resolution
   server-side for unmatched labels (so clients need zero fallback calls).
