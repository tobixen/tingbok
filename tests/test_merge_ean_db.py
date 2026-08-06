"""Tests for scripts/merge_ean_db.py.

The merge script reconciles ``ean-db.json`` across two diverged branches.  Two
real-world hazards drive these tests:

* ``6b9c2d5`` renamed local article-number keys to shop-prefixed form
  (``00501163`` -> ``mercadona-00501163``).  A branch that predates the rename
  still carries the bare key, and a naive key-level merge resurrects it
  alongside the prefixed record.
* A server whose data file was reset (see ``f2ae169``, which dropped 318
  entries to 15) rebuilds entries from scratch.  Those rebuilt entries are
  *poorer* than the ones on main, so a key-level "theirs vs ours" choice throws
  away brand/categories/quantity and most of the price history.

The merge must therefore fold renames and union observations field-by-field
rather than picking a winning entry.
"""

import importlib.util
from pathlib import Path
from typing import Any

import pytest

_SCRIPT = Path(__file__).parents[1] / "scripts" / "merge_ean_db.py"
_spec = importlib.util.spec_from_file_location("merge_ean_db", _SCRIPT)
assert _spec is not None
assert _spec.loader is not None
merge_ean_db = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(merge_ean_db)


def price(date: str | None, value: float, shop: str | None = "Lidl Varna", unit: str = "pcs") -> dict[str, Any]:
    return {"currency": "EUR", "date": date, "price": value, "shop": shop, "unit": unit}


@pytest.fixture
def rich_entry() -> dict[str, Any]:
    """An entry as curated on main: brand, categories, quantity, long price history."""
    return {
        "brand": "Pilos",
        "categories": ["yogurt"],
        "name": "Yogurt 3.6% 1kg",
        "prices": [price("2026-01-24", 0.78), price("2026-03-04", 1.53)],
        "quantity": "1000g",
        "receipt_names": [
            {"first_seen": "2026-01-24", "last_seen": "2026-05-31", "name": "КИСЕЛО МЛЯКО 3,6%", "shop": "Lidl Varna"}
        ],
        "source": "manual",
    }


def test_ours_only_entry_is_added(rich_entry: dict[str, Any]) -> None:
    base = {"111": rich_entry}
    ours = {"111": rich_entry, "222": {"name": "New thing", "categories": ["food"]}}
    theirs = {"111": rich_entry}

    result, _, _ = merge_ean_db.merge(base, ours, theirs)

    assert result["222"]["name"] == "New thing"


def test_shop_prefix_rename_is_folded_not_resurrected() -> None:
    """A bare key renamed on theirs must not reappear when ours modified it."""
    entry = {"categories": ["food/spices"], "name": "Hacendado Paella spice"}
    base = {"00501163": entry}
    ours = {"00501163": {**entry, "prices": [price("2026-07-21", 1.19, shop="Mercadona")]}}
    theirs = {"mercadona-00501163": entry}

    result, _, _ = merge_ean_db.merge(base, ours, theirs)

    assert "00501163" not in result, "bare key resurrected alongside the shop-prefixed record"
    assert "mercadona-00501163" in result
    assert result["mercadona-00501163"]["prices"] == [price("2026-07-21", 1.19, shop="Mercadona")]


def test_bare_code_invented_by_ours_folds_onto_theirs_prefixed_key() -> None:
    """No ancestor links the two keys, so only the service's alias rule can pair them."""
    base: dict[str, Any] = {}
    ours = {"20815400": {"name": "Суров микс кашу", "prices": [price("2026-07-21", 2.49)]}}
    theirs = {"lidl-20815400": {"categories": ["food/nuts"], "name": "Alesto cashew & cranberry mix"}}

    result, _, _ = merge_ean_db.merge(base, ours, theirs)

    assert "20815400" not in result, "bare code kept as a second entry for the same product"
    assert result["lidl-20815400"]["name"] == "Alesto cashew & cranberry mix"
    assert price("2026-07-21", 2.49) in result["lidl-20815400"]["prices"]


def test_ambiguous_bare_code_is_not_folded() -> None:
    """Two shops using the same local number must not be silently merged."""
    base: dict[str, Any] = {}
    ours = {"20815400": {"name": "something", "prices": [price("2026-07-21", 2.49)]}}
    theirs = {"lidl-20815400": {"name": "Lidl item"}, "billa-20815400": {"name": "Billa item"}}

    result, _, _ = merge_ean_db.merge(base, ours, theirs)

    assert "20815400" in result, "ambiguous code must stay put for a human to resolve"


def test_ambiguity_on_theirs_is_not_rescued_by_an_ours_internal_twin() -> None:
    """An ambiguous code must not fall through to a guess just because ours has a twin.

    The bare record was scanned at Billa; folding it onto ours' own lidl-* twin
    would file a Billa price under the Lidl product and leave billa-* empty.
    """
    base: dict[str, Any] = {}
    theirs = {"lidl-20815400": {"name": "Lidl item"}, "billa-20815400": {"name": "Billa item"}}
    ours = {
        "20815400": {"name": "scanned at Billa", "prices": [price("2026-07-21", 2.49, shop="Billa")]},
        "lidl-20815400": {"name": "Lidl item"},
    }

    result, _, _ = merge_ean_db.merge(base, ours, theirs)

    assert "20815400" in result, "ambiguous code was folded onto a guessed shop"
    assert not result["lidl-20815400"].get("prices"), "Billa price landed on the Lidl record"


def test_deleted_entry_is_not_renamed_onto_an_unrelated_product() -> None:
    """A suffix match is not evidence two entries are the same product."""
    base = {"222": {"name": "bogus", "prices": [price("2026-01-01", 9.99)]}}
    theirs = {"lidl-222": {"name": "a completely different Lidl product"}}
    ours = {"222": {"name": "bogus", "prices": [price("2026-01-01", 9.99)]}}

    result, log, _ = merge_ean_db.merge(base, ours, theirs)

    assert not result["lidl-222"].get("prices"), "deleted entry's price merged into another product"
    assert any("stays deleted" in line for line in log)


def test_uncorroborated_rename_candidate_is_reported_not_guessed() -> None:
    """Same number, no shared name/price: treat as a deletion, and say so."""
    base = {"333": {"name": "some old thing"}}
    theirs = {"lidl-333": {"name": "an unrelated Lidl product"}}
    ours = {"333": {"name": "some old thing", "prices": [price("2026-01-01", 4.0)]}}

    result, log, _ = merge_ean_db.merge(base, ours, theirs)

    assert not result["lidl-333"].get("prices")
    assert any("stays deleted" in line and "333" in line for line in log), log


def test_hyphenated_part_number_is_not_treated_as_a_bare_code() -> None:
    """Only all-digit codes with a digit-free shop prefix may be folded."""
    base = {"ab-12": {"name": "manufacturer part"}}
    theirs = {"lidl-ab-12": {"name": "unrelated Lidl item"}}
    ours = {"ab-12": {"name": "manufacturer part", "prices": [price("2026-01-01", 5.0)]}}

    result, _, _ = merge_ean_db.merge(base, ours, theirs)

    assert not result["lidl-ab-12"].get("prices")


def test_transposed_refs_are_refused_rather_than_split(rich_entry: dict[str, Any]) -> None:
    """Running the merge backwards would split one product across two keys."""
    base = {"20004132": {"name": "chopped tomatoes"}}
    theirs = {"20004132": {"name": "chopped tomatoes"}}
    ours = {"lidl-20004132": {"name": "Baresa chopped tomatoes", "prices": [price("2026-08-01", 0.89)]}}

    with pytest.raises(SystemExit, match="split product history"):
        merge_ean_db.merge(base, ours, theirs)


def test_merge_is_idempotent(rich_entry: dict[str, Any]) -> None:
    """Re-merging an already-merged result must be a no-op."""
    base = {"111": rich_entry}
    theirs = {"111": rich_entry}
    ours = {
        "111": {**rich_entry, "prices": [price("2026-08-01", 1.59)]},
        "222": {"name": "new product", "prices": [price("2026-08-02", 3.0)]},
    }

    once, _, _ = merge_ean_db.merge(base, ours, theirs)
    twice, _, _ = merge_ean_db.merge(base, ours, once)

    assert twice == once


def test_bare_and_prefixed_key_on_the_same_side_collapse() -> None:
    """Ours carrying both keys must fold into one entry, not lose either's prices."""
    # base and theirs share the name, as in the real 6b9c2d5 migration, so the
    # rename is corroborated and the two keys are known to be one product.
    base = {"20004132": {"name": "Baresa chopped tomatoes"}}
    theirs = {"lidl-20004132": {"name": "Baresa chopped tomatoes"}}
    ours = {
        "20004132": {"name": "Baresa chopped tomatoes", "prices": [price("2026-07-01", 0.86)]},
        "lidl-20004132": {"name": "Baresa chopped tomatoes", "prices": [price("2026-08-01", 0.89)]},
    }

    result, _, _ = merge_ean_db.merge(base, ours, theirs)

    assert "20004132" not in result
    merged_prices = result["lidl-20004132"]["prices"]
    assert price("2026-07-01", 0.86) in merged_prices
    assert price("2026-08-01", 0.89) in merged_prices


def test_ours_internal_bare_prefixed_pair_folds_receipt_record_first() -> None:
    """broxbox06 held both forms for 21 products; the receipt-derived name must win."""
    base: dict[str, Any] = {}
    theirs: dict[str, Any] = {}
    ours = {
        "20003326": {
            "name": "Ванилова захар 10бр. / vanilla sugar, 10 sachets",
            "categories": ["vanilla_sugar"],
            "prices": [price("2026-07-21", 0.55)],
        },
        "lidl-20003326": {"name": "Vaníliás cukor", "prices": [price("2026-08-01", 0.57)]},
    }

    result, _, _ = merge_ean_db.merge(base, ours, theirs)

    assert "20003326" not in result
    entry = result["lidl-20003326"]
    assert entry["name"] == "Ванилова захар 10бр. / vanilla sugar, 10 sachets"
    assert entry["categories"] == ["vanilla_sugar"]
    assert price("2026-07-21", 0.55) in entry["prices"]
    assert price("2026-08-01", 0.57) in entry["prices"]


def test_richer_theirs_fields_survive_a_truncated_ours(rich_entry: dict[str, Any]) -> None:
    """Ours rebuilt the entry from scratch; its missing fields must not delete theirs."""
    base = {"4056489941200": rich_entry}
    theirs = {"4056489941200": rich_entry}
    ours = {
        "4056489941200": {
            "name": "Кисело мляко 3.6%",
            "prices": [price("2026-07-31", 1.53, unit="stk")],
            "receipt_names": [
                {
                    "first_seen": "2026-06-21",
                    "last_seen": "2026-06-21",
                    "name": "КИСЕЛО МЛЯКО 3,6%",
                    "shop": "Lidl Varna бул. Вл. Варненчик 257",
                }
            ],
        }
    }

    result, _, _ = merge_ean_db.merge(base, ours, theirs)
    merged = result["4056489941200"]

    assert merged["brand"] == "Pilos"
    assert merged["categories"] == ["yogurt"]
    assert merged["quantity"] == "1000g"
    assert merged["source"] == "manual"
    assert merged["name"] == "Yogurt 3.6% 1kg", "curated name must win over the receipt-derived one"
    assert price("2026-01-24", 0.78) in merged["prices"], "theirs price history dropped"
    assert price("2026-07-31", 1.53, unit="stk") in merged["prices"], "ours new price lost"


def test_prices_dedup_by_date_currency_price(rich_entry: dict[str, Any]) -> None:
    base = {"111": rich_entry}
    theirs = {"111": rich_entry}
    ours = {
        "111": {
            **rich_entry,
            "prices": [
                # Same (date, currency, price) as theirs but a differently-spelled shop.
                price("2026-03-04", 1.53, shop="Lidl Варна"),
                price("2026-08-01", 1.59),
            ],
        }
    }

    result, _, _ = merge_ean_db.merge(base, ours, theirs)
    prices = result["111"]["prices"]

    matching = [p for p in prices if p["date"] == "2026-03-04"]
    assert len(matching) == 1, f"duplicate price observation: {matching}"
    # Pin the total too, so discarding ours' prices wholesale cannot pass this test.
    assert len(prices) == 3, prices
    assert price("2026-08-01", 1.59) in prices


def test_receipt_name_last_seen_advances(rich_entry: dict[str, Any]) -> None:
    base = {"111": rich_entry}
    theirs = {"111": rich_entry}
    ours = {
        "111": {
            **rich_entry,
            "receipt_names": [
                {
                    "first_seen": "2026-01-24",
                    "last_seen": "2026-08-04",
                    "name": "КИСЕЛО МЛЯКО 3,6%",
                    "shop": "Lidl Varna",
                }
            ],
        }
    }

    result, _, _ = merge_ean_db.merge(base, ours, theirs)
    names = result["111"]["receipt_names"]

    assert len(names) == 1, "same (name, shop) must merge rather than duplicate"
    assert names[0]["last_seen"] == "2026-08-04"
    assert names[0]["first_seen"] == "2026-01-24"


def test_receipt_name_first_seen_retreats(rich_entry: dict[str, Any]) -> None:
    """Ours may hold an earlier sighting than theirs; the window must widen both ways."""
    base = {"111": rich_entry}
    theirs = {"111": rich_entry}
    ours = {
        "111": {
            **rich_entry,
            "receipt_names": [
                {
                    "first_seen": "2025-11-02",
                    "last_seen": "2026-05-31",
                    "name": "КИСЕЛО МЛЯКО 3,6%",
                    "shop": "Lidl Varna",
                }
            ],
        }
    }

    result, _, _ = merge_ean_db.merge(base, ours, theirs)
    names = result["111"]["receipt_names"]

    assert len(names) == 1
    assert names[0]["first_seen"] == "2025-11-02"
    assert names[0]["last_seen"] == "2026-05-31"


def test_entry_deleted_on_theirs_stays_deleted(rich_entry: dict[str, Any]) -> None:
    """A deliberate deletion on theirs must not be undone by an untouched ours."""
    base = {"111": rich_entry, "222": {"name": "bogus"}}
    ours = {"111": rich_entry, "222": {"name": "bogus"}}
    theirs = {"111": rich_entry}

    result, _, _ = merge_ean_db.merge(base, ours, theirs)

    assert "222" not in result


def test_result_is_key_sorted(rich_entry: dict[str, Any]) -> None:
    base: dict[str, Any] = {}
    ours = {"999": rich_entry, "111": rich_entry}
    theirs = {"555": rich_entry}

    result, _, _ = merge_ean_db.merge(base, ours, theirs)

    assert list(result) == sorted(result)
