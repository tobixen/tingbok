See also ~/inventory-md/docs/TODO-CATEGORIES.md

## Consistency

* I think that a while ago, logic was added to make it possible to simplify vocabulary.yaml, lists of paths would obsolete the need of having lists of broader and narrower for every concept.  I.e., by listing `food/staples/potatoes` and `food/vegetables/potatoes` it should not be needed to list vegetables and staples as broader for potatoes, etc.  Look into this again and use it consistently in vocuabularay.yaml to keep it DRY.
* Sometimes canonical IDs are given like a path with a slash, and sometimes they are given as a single word.  I'd like some consistency here too.  Perhaps it's good to keep slashes in canonical IDs in case the same word gets added for a different concept at some point in the future, but it's probably not needed with many parts in the canonical ID.  It could be `staples/potatoes` or `food//potatoes`, but not `food/staple/potatoes` maybe.  We'd also need some heuristics to relatively deterministicly find a canonical ID of a concept that isn't defined in vocabulary.yaml but decided from the sources.
* Dashes vs underscores vs spaces in the canonical ID.  It seems to be a bit arbitrary now.  I think we should stick to dashes.
* Plural vs singular.  We should have some consistency there, too.

## Data that should be filtered

Recently (86e885b40f01149fe8cd263a841eb98933eb674f/b679cc6df936d3d8e0b9926afbbef4bb847f656f/1aaed1ed028dfa5ef7e93e2abe2e315712aab7f6) logic was added to filter away non-thingy concepts

Either the category source "is it a thing?" filtering does not work, or the filters should be sharpened.  https://tingbok.plann.no/api/lookup/teddy gives https://www.wikidata.org/wiki/Q18010041 which is a "given name" (instance of https://www.wikidata.org/wiki/Q12308941 - male name - which is a subclass of https://www.wikidata.org/wiki/Q202444 - given name - subclass of https://www.wikidata.org/wiki/Q10856962 - antrophonym.  None of those should be used as sources. The correct node is https://www.wikidata.org/wiki/Q213477

It also gives https://dbpedia.org/page/Teddy_Stadium - again, https://dbpedia.org/page/Teddy_bear is the correct.  Teddy Stadium has attributes like dbp:tenants `georss:point` `geo:geometry` `geo:lat` `geo:long`, `dbo:buildingStartDate`, I think any dbpedia article with any of those attributes should be disqualified as it's most likely not a thing one would have in a domestic inventory.

## EAN pattern matching

Sometimes the EAN itself cannot be looked up, but the first digits can still tell a lot of information.  Add some logic here.

The barcode lookup script in ~/inventory-md/scripts should be updated to use tingbok

