# TODO: category taxonomy cleanup

## Category ids in `ean-db.json` follow no single style

The 330 distinct category ids in `src/tingbok/data/ean-db.json` (as of
2026-10-05) mix several conventions:

| style | count | examples |
|---|---|---|
| hyphenated leaf | 84 | `fuel-additive`, `food/cured-meat`, `clothing/t-shirt` |
| underscored leaf | 28 | `olive_oil`, `meat/whale_meat`, `food/fishery_products/dried_fish` |
| single word | 218 | `beans`, `food/dairy`, `Salami` |

Other inconsistencies:

- **Mixed case.** `Salami` and `Rope` sit next to lower-case ids.
- **Mixed language.** There are Norwegian paths such as
  `helse/hudpleie/serum` and `sko/pleie/skokrem` next to English ones, so the
  same concept can be filed under two different trees.
- **Same concept at different depths.** For example `food/cured-meat`,
  `food/meat/cured-meat` and `fermented_sausage`.

Underscored ids look like they come from AGROVOC/Wikidata labels
(path segments like `processed_animal_products`), and hyphenated ones from `vocabulary.yaml`
or hand entry.

To do:

1. Settle on one style. Lower-case and English, with hyphens: hyphens are the
   more common multi-word style in `vocabulary.yaml` too, but only by 62 ids
   to 49 underscored ones, so that file needs the same cleanup.
2. Normalise categories as they are written, the way price units are
   normalised in `services/ean.py`.
3. Migrate the existing ids, mapping each to a `vocabulary.yaml` concept where
   one exists.

See also `TODO.md`: the canonical-id consistency item, and the one asking to
stick to dashes.
