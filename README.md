# tingbok

Product and category lookup service for domestic inventory systems - this service provides a centralized API for:

- **Global tingbok category vocabulary** — a curated ~258-concept taxonomy for household inventory categorization
- **SKOS category lookups** — hierarchy paths from AGROVOC, DBpedia, Wikidata
- **EAN/barcode product lookups** — product data from Open Food Facts, Open Library, shopping receipts and various other lookup services

In the future, I'm also considering to organize other infomation into the vocabulary, information currently stored in free-text "tags" to the vocabulary, including weather a product is "broken", "worn" or "brand new", weather a product is meant for ladies, gents or children, etc.

## Background

I'm working on a domestic inventory service [inventory-md](https://github.com/tobixen/inventory-md), and I already have two database instances - one for my boat and one for my home.

What I (or you) actually have in stock is *local* information.  Information on the things one *may* have in the inventory belongs to a global database.  As there is overlapping between the two inventories, I already want those two to share information.  Since the services out there that exists and can be queried for free often have restricted capacity and may be rate-limited it's important with caching - so the caching system was created early on - but how to share the caches?  Not only between the instances, but I also have the data duplicated on my laptop and on a server.

We have sort of an hierarchy here, at the very bottom are the "root categories".  Clothes and food are usually two very different things and fits well as root categories in a domestic inventory system.  (Of course, personal opinions as well as local needs may vary.  It should be possible to override those things).  Intermediate categories exists, like "food/fruits" - and very specific categories like "food/diary/milk/fresh full fat milk".  This is important when generating shopping lists - I do want to always have fresh full fat milk in the fridge as well as some fruits and nuts.

Near the bottom there may be very specific information about brand/producer, package size, etc, this is often linked with a European/International Article Number (EAN).  Perhaps it had a price tag when purchasing it as well.

All this information belongs to a global database.

(At the very bottom, there may also be information about a specific item.  A teddy bear may have an EAN, but your daughters teddy bear should be considered unique and may also have a name.  This does not belong to a global database).

### SKOS

I wanted to slap some standard hierarchical category system on the inventories.  According to Wikipedia, "Simple Knowledge Organization System (SKOS) is a W3C recommendation designed for representation of (...) classification schemes", so it seemed a perfect fit.  Unfortunately, this standard is just describing the schema of a classification scheme.  I found three public databases, AGROVOC, DBpedia and WikiData.  All three of them have very slow query APIs, so a local cache was paramount - but even that seemed insufficient as the API calls was timing out frequently - when managing to get hold of data from the source it seemed important to keep it and share it with all instances.  I've found better ways of accessing the information, but still the public databases are slow, so it's nice to have a public cache available.  It's also possible to download the complete database from upstream and serve it, but even the smallest (Agrovoc) is big and takes long time to load, so better to do this from some separate service than to do it every time the inventory is changed.

### EANs, ISBNs, price information etc

To make it easier to populate the database, it's an important feature to look up EANs and find both product information and category information.

There exists a public database of food-related EANs, the OpenFoodFacts database.  It does contain some category system (not SKOS-based), and is the one that seems to be closest to the hierarhcical categorization system we'd like to use in the inventory database.  However, as it's name suggests, it's mostly about food products.  The database seems to work well, but it's still nice to have a caching service, it makes things more robust (the inventory-md will by default try the tingbok service first and then go directly towards the official sources if tingbok doesn't work).  There also exists various open and free services for looking up book ISBNs.  Unfortunately, most non-food EAN databases are commercial.  Fetching things from commercial databases, caching them and exposting them for the public may lead to nasty juridical side effects, so I may need to look into that.

Another source (which in some cases seems essential as some shop chains may have their local article numbers and bar code systems) is to simply compare bar codes with shopping receipts.  This way one gets price information as well.  It may involve a lot of work, but now with AI-tools it's possible to get it done relatively quickly.

### The global tingbok category vocabulary

As none of the sources have a category hierachy suitable for easy navigation, it was sadly necessary to build yet another vocabulary.  The tingbok category vocabulary is mostly meant to link up the concepts from the other category sources into a neat category tree.  It may also be the "official source" of what's true when different databases shows different things - like "bedding" are things optimized for absorbing animal pee in AGROVOC, while in most domestic inventory lists this category are for things optimized for human having a quality sleep.

## Name

*Tingbok* is Norwegian, and it was an official registry from 1633-03-05 to 1931.  The word "ting" was in this context meant to refer to a court or other group of people making decisions.  In the beginning it contained court decisions, but gradually contained mostly ownership information on properties and special clausules on property.  Admittedly it's not much related to a category and EAN database.

The word "ting" has multiple meanings, and by today the strongest connection is to "thing".  An inventory listing is basically a list of things.  Both as a "book containing my things" and as a "registry of property", the word "Tingbok" seems to fit my inventory-md service quite well.  I'm considering to rename it - but until further notice, "Tingbok" is the name of the "official" product and category lookup service.

## Quick start

You are supposed to use tingbok.plann.no - you are not supposed to set up your own server.  As for now.  I'd happily accept pull requests if you want to contribute on making this a *federated service*.

TODO: fix a Makefile
TODO: fix "claude skills" to always fix a Makefile and never suggest "pip install" in any documentation

Disclaimer: All documentation below is AI-generated.

```bash
pip install tingbok
uvicorn tingbok.app:app --host 127.0.0.1 --port 5100
```

## API endpoints

| Method | Path | Description |
|--------|------|-------------|
| `GET` | `/health` | Liveness check |
| `GET` | `/api/skos/lookup` | Single concept lookup |
| `GET` | `/api/skos/hierarchy` | Full hierarchy paths |
| `GET` | `/api/skos/labels` | Translations for a URI |
| `GET` | `/api/ean/{ean}` | EAN/barcode product lookup |
| `GET` | `/api/vocabulary` | Full package vocabulary |
| `GET` | `/api/vocabulary/{concept_id}` | Single concept |

## Development

```bash
pip install -e ".[dev]"
pytest tests/ -v
ruff check src/
```

## License

MIT
