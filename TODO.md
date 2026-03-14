See also ~/inventory-md/docs/TODO-CATEGORIES.md

* When searching for sources, we should filter away names and geographical places (at least) from dbpedia and wikidata.  Make some statistics on "instance of" for the various wikidata sources, probably there are more things that should be excluded.  Things having a coordinate location can most likely be ignored.  Probably there are quite much filtering that can be done in dbpedia too, like things having a "dbo:PopulatedPlace*", "dbo:birthPlace" and many more. A dbpedia page with a "dbo:wikiPageDisambiguates" property probably needs manual attention.  We also need a tool that can find and filter out wrong existing source URIs
* Categories found through EAN should be looked up, and if it's present in the vocabulary it should be normalized
* The vocabulary.yaml is too complex to edit.  I'd like a condensed version of it:
  * No "broader" or "narrower" lists - the mere existence of a label like "food/spreads/caviar" will ensure that spreads is added to the broader-list of caviar, etc
  * Missing source URIs should be auto-updated
* We may need a PUT for concepts as well, for maintaining the vocabulary.  It should be possible to do operations like:
  * Adding linkages to the vocabulary - i.e. food/roes/caviar will add or update those three concepts in the vocabulary and make sure they are linked together.
  * Adding sources to the vocabulary
  * Evicting bad sources from the vocabulary
  * Adding labels

