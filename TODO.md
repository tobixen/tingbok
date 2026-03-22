See also ~/inventory-md/docs/TODO-CATEGORIES.md

## Cache refresher thread

~~I've tried to turn on debug logging, and I've tried monitoring the files under /var/cache/tingbok/skos and I think I may conclude: the cache refreshing system does not work.~~

Fixed in c466310: legacy cache entries written before `_cache_key` was added (~7800 files,
up to 55 days old) were silently skipped by `_find_oldest_cache_entry`, so the loop only
saw 13-day-old entries and slept ~9 h between each check.  `_infer_cache_key()` now
reconstructs the key from filename + content so all SKOS entries are eligible for refresh.

## Data that should be filtered

Recently (86e885b40f01149fe8cd263a841eb98933eb674f/b679cc6df936d3d8e0b9926afbbef4bb847f656f/1aaed1ed028dfa5ef7e93e2abe2e315712aab7f6) logic was added to filter away non-thingy concepts

Either the category source "is it a thing?" filtering does not work, or the filters should be sharpened.  https://tingbok.plann.no/api/lookup/teddy gives https://www.wikidata.org/wiki/Q18010041 which is a "given name" (instance of https://www.wikidata.org/wiki/Q12308941 - male name - which is a subclass of https://www.wikidata.org/wiki/Q202444 - given name - subclass of https://www.wikidata.org/wiki/Q10856962 - antrophonym.  None of those should be used as sources. The correct node is https://www.wikidata.org/wiki/Q213477

It also gives https://dbpedia.org/page/Teddy_Stadium - again, https://dbpedia.org/page/Teddy_bear is the correct.  Teddy Stadium has attributes like dbp:tenants `georss:point` `geo:geometry` `geo:lat` `geo:long`, `dbo:buildingStartDate`, I think any dbpedia article with any of those attributes should be disqualified as it's most likely not a thing one would have in a domestic inventory.

