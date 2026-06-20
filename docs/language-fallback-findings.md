# Research: language-dependent `/api/lookup` results

## Symptom

`GET /api/lookup/{label}?lang=…` returns **different amounts of data depending on
the requested language**, even though the concept is the same. Reported example:

    /api/lookup/mushroom_hunting            → sparse (no altLabels)
    /api/lookup/mushroom_hunting?lang=nb    → rich (full altLabels incl. Norwegian)

The requester expected the language to affect only *which* prefLabel is surfaced,
not whether altLabels exist at all.

## Empirical data (live tingbok.plann.no, 2026-06-20)

Same label `mushroom_hunting`, varying `lang`:

| lang | id               | source_uris            | altLabels |
|------|------------------|------------------------|-----------|
| en   | mushroom_hunting | DBpedia + Wikidata     | rich      |
| nb   | mushroom_hunting | DBpedia + Wikidata     | rich      |
| nn   | mushroom_hunting | DBpedia + Wikidata     | rich      |
| bg   | mushroom_hunting | DBpedia + Wikidata     | rich      |
| uk   | mushroom_hunting | DBpedia + Wikidata     | rich      |
| it   | mushroom_hunting | **DBpedia only**       | **empty** |

The richness tracks **whether the Wikidata source contributed**, not the language.
The Norwegian altLabels ("plukke sopp", "sopplukking", …) and the Russian/Finnish
aliases all come from Wikidata entity `Q2391676`. When only DBpedia matches, you get
DBpedia's 6 native inline labels (de/en/es/fr/pl/ru) and no altLabels.

(The originally-reported sparse `en` result was a stale warm-cache entry that has
since been refreshed; `en` is rich now. The sparse/rich split is therefore not
"nb is richer than en" — it is "this language's lookup did/did not pull in Wikidata".)

## Root cause

`app.lookup_concept` queries each SKOS source (`agrovoc`, `dbpedia`, `wikidata`) in
the **requested language** via `_fetch_one_skos_source(label, source, lang)`. For
Wikidata this means `wbsearchentities(search="mushroom hunting", language=lang)`,
which searches that language's labels/aliases for the query string.

The query string is English-derived (it is the concept *id* `mushroom_hunting` with
separators normalised). So:

* `language=en` (and languages whose Wikidata index still surfaces the English
  string, e.g. nb/nn/bg/uk) → Wikidata finds `Q2391676` → its altLabels merge in.
* `language=it` → Italian aliases are "raccolta dei funghi" etc.; the English string
  "mushroom hunting" does not match → Wikidata returns nothing.

When the primary-language lookup misses, `_fetch_one_skos_source` consults
`_LANGUAGE_FALLBACKS` (app.py). But:

```python
_LANGUAGE_FALLBACKS = {
    "nb": ["no", "da", "nn", "sv"],
    "no": ["nb", "da", "nn", "sv"],
    "nn": ["nb", "no", "da", "sv"],
    "da": ["nb", "no", "nn", "sv"],
    "sv": ["da", "nb", "no", "nn"],
}
```

* It only covers the Scandinavian cluster — `it`, `de`, `fr`, … have **no** fallback.
* **No language falls back to `en`**, even though the lookup labels are English-derived
  identifiers. There is no universal final fallback.

So a language whose own index does not contain the English query string gets no
second chance, the source is dropped, and the merged result is poorer.

Note: `inventory-md`'s config already anticipates a universal final fallback —
`config.DEFAULTS["language_fallbacks"]` includes `"_final_fallback": "en"` — but
tingbok's `app._LANGUAGE_FALLBACKS` does not implement one. The two projects'
fallback definitions have drifted.

## Recommended fix (not yet applied — semantics change)

Add a universal final fallback to `en` for every language in
`app._LANGUAGE_FALLBACKS`, appended after the language-specific chain, e.g. resolve
fallbacks as `_LANGUAGE_FALLBACKS.get(lang, []) + ["en"]` (de-duplicated, skipping
`lang` itself). Rationale: lookup labels are English-derived ids, so English is the
natural backstop for any language whose native index misses.

Trade-offs to weigh before applying:

* **Pro:** language-invariant results — every language gets the full DBpedia+Wikidata
  merge and the same altLabels; fixes the reported inconsistency.
* **Con:** one extra upstream lookup per source on every primary-language miss (more
  network/latency, though cached after first hit).
* **Con:** could occasionally surface an English homograph for a word that legitimately
  means something else in the requested language; the existing similarity threshold and
  non-concept filters mitigate this.

A lower-risk alternative is to make the **label/altLabel enrichment** language-invariant:
it is already fetched per-URI for a fixed `_DEFAULT_FETCH_LANGUAGES` set, so once *any*
source resolves the concept URI, the same altLabels could be attached regardless of
which language matched — decoupling "did this language match the source" from "do we
return altLabels".
