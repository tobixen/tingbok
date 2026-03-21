See also ~/inventory-md/docs/TODO-CATEGORIES.md

* `tingbok --version` does not work
* A query for https://tingbok.plann.no/api/lookup/skrivemaskin?lang=no or https://tingbok.plann.no/api/lookup/skrivemaskin?lang=nb returned nothing, *fast*, while  https://tingbok.plann.no/api/lookup/typewriter?lang=en returns a blob after thinking for quite long - and the blob does contain the information that it's "skrivemaskin" in nb - so this is a bug.
* The code was updated recently to exclude lots of non-thingy-things (names, places, etc) from dbpedia and wikidata, but we probably still have many wrong concepts in the vocabulary.  It's needed to find and filter out wrong existing source URIs (dbo:wikiPageDisambiguates pages, etc.), probably making a tool for it in case more things will be added to the exclusion criterias.
* `curl -s https://tingbok.plann.no/api/ean/4056489693901` gives data from openfoodfacts, but there is no category information.  Are there more data from openfoodfacts that is discarded?
