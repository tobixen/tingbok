See also ~/inventory-md/docs/TODO-CATEGORIES.md

## Cache refresher thread

I've tried to turn on debug logging, and I've tried monitoring the files under /var/cache/tingbok/skos and I think I may conclude: the cache refreshing system does not work.

## Data that should be filtered

Either the category source "is it a thing?" filtering does not work, or the filters should be sharpened.  https://tingbok.plann.no/api/lookup/teddy gives https://www.wikidata.org/wiki/Q18010041 which is a "given name" (instance of https://www.wikidata.org/wiki/Q12308941 - male name - which is a subclass of https://www.wikidata.org/wiki/Q202444 - given name - subclass of https://www.wikidata.org/wiki/Q10856962 - antrophonym.  None of those should be used as sources. The correct node is https://www.wikidata.org/wiki/Q213477

It also gives https://dbpedia.org/page/Teddy_Stadium - again, https://dbpedia.org/page/Teddy_bear is the correct.  Teddy Stadium has attributes like dbp:tenants `georss:point` `geo:geometry` `geo:lat` `geo:long`, `dbo:buildingStartDate`, I think any dbpedia article with any of those attributes should be disqualified as it's most likely not a thing one would have in a domestic inventory.

