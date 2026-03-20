# tingbok

Product and category lookup service for domestic inventory systems - this service provides a centralized (pull requests to make it federated will be accepted) API for:

- **Global tingbok category vocabulary** — a curated ~264-concept taxonomy for household inventory categorization
- **SKOS category lookups** — hierarchy paths from AGROVOC, DBpedia, Wikidata, Open Food Facts and Google Product Taxonomy
- **EAN/barcode product lookups** — product data from Open Food Facts, Open Library, shopping receipts and various other lookup services

In the future, I may also consider adding other "information dimensions", including weather a product is "broken", "worn" or "brand new", weather a product is meant for ladies, gents or children, etc.

## Quick start

The public instance is at **https://tingbok.plann.no** with API-docs at https://tingbok.plann.no/docs/ — you are not supposed to set up your own server unless you want to contribute or run a private instance.  Pull requests to make this a *federated service* are welcome.

To run locally:

```bash
git clone https://github.com/tobixen/tingbok
cd tingbok
uv sync --extra off --extra skos
uv run uvicorn tingbok.app:app --host 127.0.0.1 --port 5100
```

## Background

I'm working on a domestic inventory service [inventory-md](https://github.com/tobixen/inventory-md), and I already have two instances - one for my boat and one for my home (plus the demo instance).

What I (or you) actually have in stock is *local* information.  Information on the things one *may* have in the inventory belongs to a global database.  As there is overlapping between the two inventories, I already want those two to share information.  Since the services out there that exists and can be queried for free often have restricted capacity and may be rate-limited it's important with caching - so the caching system was created early on - but how to share the caches?  Not only between the instances, but I also have the data duplicated on my laptop and on a server - so I decided to split it out as an independent, centralized service (I do not like the word "centralized" - if you have ideas on hwo to federate it, please contribute).

## Overview

We have sort of an hierarchy here, at the very top are the "root categories".  Clothes and food are usually two very different things and fits well as root categories in a domestic inventory system.  (Of course, personal opinions as well as local needs may vary.  It should be possible to override those things).  Intermediate categories exists, like "food/fruits" - and very specific categories like "food/diary/milk/fresh full fat milk".

This is important when generating shopping lists - I do want to always have fresh full fat milk in the fridge as well as some fruits and nuts.  Hence the fruits, nuts and "fresh full fat milk" categories are listed on the shopping list generator "wanted items".

Near the bottom there may be very specific information about brand/producer, package size, etc, this is often linked with a European/International Article Number (EAN).  Perhaps it had a price tag when purchasing it as well.

All this information, from the root category "food" and down to the specific EAN for 1l of milk from your favorite milk brand, belongs to a global database.

At the very bottom, there may be local information about a specific item.  A teddy bear may have an EAN, but your daughters teddy bear may also have a name and should be considered to be unique.

### SKOS

I wanted to slap some standard hierarchical category system on the inventories.  According to Wikipedia, "Simple Knowledge Organization System (SKOS) is a W3C recommendation designed for representation of (...) classification schemes", so it seemed a perfect fit.  Unfortunately, this standard is just describing the schema of a classification scheme.  I found three public databases, AGROVOC, DBpedia and WikiData.  All three of them have very slow query APIs, so caching is very important,  Getting hold of the data was so difficult that it seems important to keep the data cached for long and share it with all instances. It's also possible to download the complete database from upstream and serve it, but even the smallest (Agrovoc) is big and takes long time to load, so better to do this from some separate service than to do it every time the inventory is changed.

**Sources:** Agrovoc/DBpedia/Wikidata

**Data flow:** Caching or full database download (Agrovoc).

### Other category information

The OpenFoodFacts database, which contains lots of EAN codes, also has an hierarchical category database.  Then there is the Google Product Taxonomy, a relatively small flat file with all kind of products put in place in an hierarchical tree.

**Sources:** Google, OpenFoodFacts

**Data flow:** Caching (OpenFoodFacts), full database download (Google Product Taxonomy)

### EANs, ISBNs, price information etc

To make it easier to populate the database, it's an important feature to look up EANs and find both product information and category information.

There exists a public database of food-related EANs, the OpenFoodFacts database.  It does contain some category system (not SKOS-based), and is the one that seems to be closest to the hierarhcical categorization system we'd like to use in the inventory database.  However, as it's name suggests, it's mostly about food products.  The database seems to work well, but it's still nice to have a caching service, it makes things more robust (the inventory-md will by default try the tingbok service first and then go directly towards the official sources if tingbok doesn't work).  There also exists various open and free services for looking up book ISBNs.  Unfortunately, most non-food EAN databases are commercial.  Fetching things from commercial databases, caching them and exposting them for the public may lead to nasty juridical side effects, so I may need to look into that.

Another source (which in some cases seems essential as some shop chains may have their local article numbers and bar code systems) is to simply compare bar codes with shopping receipts.  This way one gets price information as well.  It may involve a lot of work, but now with AI-tools it's possible to get it done relatively quickly.

**Sources:** OpenFoodFacts and various other sources, including user contributions through the API (in the beginning we'll try without any kind of authentication, perhaps we should require signed data in the future).

**Data flow:** This is more than just caching, since user contributions are allowed, we're actually building up a database here that neesd to be backed up as well.  The database should be considered free and it should be possible not only to look up things but also download the full database.

### The "tingbok" vocabulary

As none of the sources have a category hierachy suitable for easy navigation, it was sadly necessary to build yet another vocabulary.  The tingbok category vocabulary is mostly meant to link up the concepts from the other category sources into a neat category tree.  It may also be the "official source" of what's true when different databases shows different things - like "bedding" are things optimized for absorbing animal pee in AGROVOC, while in most domestic inventory lists this category are for things optimized for human having a quality sleep.

**Sources:** Curated database with user contributions, contributions may be passed through the API or by a pull request in GitHub.

**Data flow:** The service will serve data from a local static database.

## Name

*Tingbok* is Norwegian, and it was an official registry from 1633-03-05 to 1931.  The word "ting" was in this context meant to refer to a court or other group of people making decisions.  In the beginning it contained court decisions, but gradually contained mostly ownership information on properties and special clausules on property.  Admittedly it's not much related to a category and EAN database.

The word "ting" has multiple meanings, and by today the strongest connection is to "thing".  An inventory listing is basically a list of things.  Both as a "book containing my things" and as a "registry of property", the word "Tingbok" seems to fit my inventory-md service quite well.  As for now, "Tingbok" is the name of the "official" product and category lookup service, but I'm consiering to rename my inventory-md as well.

To me, a Norwegian born 40 years after the last tingbok entry was written, "Tingbok" does not sound like a very official thing, it has a bit of a funny sound to it.  Claude suggested it, and I decided to stick with it.

## API endpoints

Please use the interactive API docs for updated overview of the API endpoints: https://tingbok.plann.no/docs

## Development

```bash
git clone https://github.com/tobixen/tingbok
cd tingbok
make dev
```

## License

AGPL 3
