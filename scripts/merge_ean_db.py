#!/usr/bin/env python3
"""Merge src/tingbok/data/ean-db.json across two git branches.

Usage:
    python3 scripts/merge_ean_db.py [--ours <ref>] [--theirs <ref>]

Defaults to merging HEAD (current branch) into main, using the common
merge-base as the ancestor.  The strategy is:

- Fold key renames.  ``6b9c2d5`` moved local article numbers to shop-prefixed
  keys (``00501163`` -> ``mercadona-00501163``); a branch that predates it still
  carries the bare key.  Keys that vanished from "theirs" and reappeared there
  with a ``<shop>-`` prefix are rewritten in the ancestor and in "ours" before
  anything is compared, so the bare key is not resurrected as a duplicate.
- Start from the "theirs" snapshot (main by default) and merge "ours" entries
  into it *field by field* rather than picking a winning entry.  A server whose
  data file was reset rebuilds its entries from receipts alone, so its copy is
  routinely poorer than main's: curated ``brand``/``categories``/``quantity``/
  ``source`` and most of the price history would be lost by an entry-level
  choice.
- ``prices`` and ``receipt_names`` — and only these two — are unioned, using the
  same rules the service applies when storing an observation
  (``tingbok.services.ean``), so for the observation lists the merged file
  matches one built by replaying the observations.
- Every other field, ``categories`` included, is fill-only: taken from "ours"
  just where "theirs" has no value.  Note what this means for ``categories``:
  the two sides' lists are **not** combined, so a category only "ours" recorded
  is dropped rather than appended.  That is deliberate — the two sides label
  products in different registers (``food/nuts`` vs ``roasted_peanut``), and
  unioning them would mix hierarchy paths with bare slugs.  Every such drop is
  reported as ``[differs]`` so it can be curated by hand afterwards.
- Entries deleted on "theirs" stay deleted.

The result is written back to src/tingbok/data/ean-db.json in the working
tree (not committed automatically).
"""

import argparse
import copy
import json
import subprocess
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).parents[1] / "src"))

from tingbok.services.ean import (  # noqa: E402
    merge_price_observations,
    merge_receipt_name_observations,
)

EAN_DB_PATH = Path("src/tingbok/data/ean-db.json")

#: Observation lists, unioned with the service's own de-duplication rules.
LIST_FIELDS = ("prices", "receipt_names")

#: Fields curated on main that a receipt-derived entry must never overwrite.
CURATED_FIELDS = ("name", "brand", "categories", "quantity", "source")


def git_blob_json(ref: str, path: str) -> dict:
    raw = subprocess.check_output(["git", "cat-file", "-p", f"{ref}:{path}"])
    return json.loads(raw)


def resolve_sha(ref: str) -> str:
    return subprocess.check_output(["git", "rev-parse", ref], text=True).strip()


def merge_base(ref_a: str, ref_b: str) -> str:
    return subprocess.check_output(["git", "merge-base", ref_a, ref_b], text=True).strip()


def _prefixed_variants(observations: dict, code: str) -> list[str]:
    """Keys of *observations* of the form ``<shop>-<code>`` for a bare local *code*.

    A shop prefix never contains digits, and a bare local code is all digits.
    Without those two conditions the suffix match is pure string coincidence:
    a deliberately deleted key ``222`` would "rename" itself onto an unrelated
    ``lidl-222``, and a hyphenated manufacturer part number ``ab-12`` would
    match ``lidl-ab-12``.
    """
    if not code.isdigit():
        return []
    matches = []
    for key in observations:
        prefix, _, suffix = key.rpartition("-")
        if suffix == code and prefix and not any(c.isdigit() for c in prefix):
            matches.append(key)
    return matches


def _shares_evidence(a: dict[str, Any], b: dict[str, Any]) -> bool:
    """True when two entries carry positive evidence of being the same product."""
    if a.get("name") and a["name"] == b.get("name"):
        return True

    def pkeys(e: dict[str, Any]) -> set:
        return {(p.get("date"), p.get("currency"), p.get("price")) for p in e.get("prices") or []}

    def rkeys(e: dict[str, Any]) -> set:
        return {(r.get("name"), r.get("shop")) for r in e.get("receipt_names") or []}

    return bool(pkeys(a) & pkeys(b)) or bool(rkeys(a) & rkeys(b))


def detect_renames(base: dict, theirs: dict) -> dict[str, str]:
    """Map ancestor keys to the shop-prefixed keys they became on *theirs*.

    A rename needs the bare key to have disappeared from *theirs*, exactly one
    key added there to be a ``<shop>-`` variant of it, *and* the two entries to
    share a name, a price observation or a receipt-name sighting.  Without that
    last condition a suffix match is only a string coincidence: a bare key that
    *was* deliberately deleted would be silently revived into whatever unrelated
    product happens to sit under the same number with a shop prefix.  All 66
    records migrated by ``6b9c2d5`` corroborate, since that migration moved keys
    without rewriting values.
    """
    added = {k: theirs[k] for k in set(theirs) - set(base)}
    renames: dict[str, str] = {}
    for old in set(base) - set(theirs):
        candidates = _prefixed_variants(added, old)
        if len(candidates) == 1 and _shares_evidence(base[old], added[candidates[0]]):
            renames[old] = candidates[0]
    return renames


def detect_alias_folds(base: dict, ours: dict, theirs: dict) -> dict[str, str]:
    """Map bare local codes *ours* invented to the shop-prefixed key that owns them.

    A server predating ``6b9c2d5`` files a freshly scanned Lidl code under the
    bare number.  When the same product was independently recorded on the other
    side as ``<shop>-<code>``, no ancestor entry links the two, so
    :func:`detect_renames` cannot see it.  Without this the merge yields both
    keys for one product.

    Two shops using the same local number is genuinely ambiguous and is never
    folded — not even onto an ours-internal twin, since guessing which shop the
    bare record came from is exactly the mistake ``6b9c2d5`` set out to prevent.

    Unlike :func:`detect_renames` this needs no corroborating name or price: a
    bare local code and the single ``<shop>-`` variant of it *are* the same
    product by construction of the numbering scheme, and the two records
    routinely disagree on the name precisely because one came from a receipt and
    the other from upstream.
    """
    folds: dict[str, str] = {}
    for key in set(ours) - set(base) - set(theirs):
        candidates = _prefixed_variants(theirs, key)
        if len(candidates) > 1:
            continue  # ambiguous on theirs — leave it for a human
        if not candidates:
            # Fall back to a twin *ours* holds itself: a receipt-derived bare
            # record beside an upstream-derived prefixed one.
            candidates = _prefixed_variants({k: v for k, v in ours.items() if k != key}, key)
        if len(candidates) == 1:
            folds[key] = candidates[0]
    return folds


def apply_renames(data: dict, renames: dict[str, str], log: list[str] | None = None) -> dict:
    """Rewrite keys of *data* per *renames*, merging entries that collide.

    Bare keys are visited before their ``<shop>-`` twins, so when the two fold
    together the bare entry seeds the merge and the prefixed one only fills its
    gaps.  That is the wanted direction: the bare record is the receipt-derived
    one, carrying the local-language name and the categories, while the prefixed
    twin often holds only an upstream name in a third language.  The ordering is
    stated explicitly rather than left to rely on digits sorting before letters,
    which a numeric shop prefix would invert.
    """
    result: dict[str, Any] = {}
    for key in sorted(data, key=lambda k: ("-" in k, k)):
        target = renames.get(key, key)
        if target in result:
            result[target] = merge_entry(result[target], data[key], target, log if log is not None else [])
        else:
            result[target] = data[key]
    return result


def merge_entry(theirs_entry: dict[str, Any], ours_entry: dict[str, Any], key: str, log: list[str]) -> dict[str, Any]:
    """Merge one *ours* entry into the corresponding *theirs* entry."""
    merged = copy.deepcopy(theirs_entry)

    for field in LIST_FIELDS:
        ours_list = ours_entry.get(field) or []
        if not ours_list:
            continue
        existing = merged.get(field) or []
        if field == "prices":
            merged[field] = merge_price_observations(existing, ours_list)
        else:
            merged[field] = merge_receipt_name_observations(existing, ours_list)
        # Count added and pruned separately: netting them hides the case where
        # ours contributes a dated price that supersedes a null-date row on
        # theirs, which changes the data while leaving the length equal.
        added = [o for o in merged[field] if o not in existing]
        pruned = [o for o in existing if o not in merged[field]]
        if added or pruned:
            note = f"{len(added)} new" if added else ""
            if pruned:
                note = f"{note}, {len(pruned)} superseded" if note else f"{len(pruned)} superseded"
            log.append(f"[+{field}] {key}: {note}")

    for field, value in ours_entry.items():
        if field in LIST_FIELDS:
            continue
        if not merged.get(field):
            # Present-but-empty on theirs counts as absent and is refilled.
            merged[field] = value
            log.append(f"[fill] {key}.{field} = {value!r}")
        elif merged[field] != value:
            log.append(f"[differs] {key}.{field}: kept {merged[field]!r}, ours had {value!r}")

    return merged


def merge(base: dict, ours: dict, theirs: dict) -> tuple[dict, list[str], set[str]]:
    """Merge *ours* into *theirs*; returns the result, a log, and the keys added."""
    log: list[str] = []

    renames = detect_renames(base, theirs)
    for old, new in sorted(renames.items()):
        log.append(f"[rename] {old} -> {new}")
    # The ancestor is only consulted for key-set membership, so collapsing its
    # collisions must not emit merge-decision lines into the user-facing report.
    base = apply_renames(base, renames)
    ours = apply_renames(ours, renames, log)

    folds = detect_alias_folds(base, ours, theirs)
    for old, new in sorted(folds.items()):
        log.append(f"[fold alias] {old} -> {new}")
    ours = apply_renames(ours, folds, log)

    result = copy.deepcopy(theirs)

    new_keys = set(ours) - set(base) - set(theirs)
    for key in sorted(new_keys):
        result[key] = ours[key]
        log.append(f"[add ours-only] {key}: {ours[key].get('name', '')}")

    for key in sorted(set(ours) & set(theirs)):
        if ours[key] == theirs[key]:
            continue
        result[key] = merge_entry(theirs[key], ours[key], key, log)

    for key in sorted((set(ours) & set(base)) - set(theirs)):
        log.append(f"[stays deleted] {key}: removed on theirs, present in ours")

    result = dict(sorted(result.items()))
    _assert_no_twin_pairs(result)
    return result, log, new_keys


def _assert_no_twin_pairs(result: dict) -> None:
    """Fail loudly if a bare local code survives next to its shop-prefixed twin.

    Splitting one product's history across two keys is the corruption this
    script exists to prevent, so it is checked rather than trusted.  Rename
    detection only looks for keys that vanished from *theirs*, which means
    running with ``--ours`` and ``--theirs`` transposed would otherwise produce
    exactly that split.
    """
    twins = []
    for bare in sorted(result):
        variants = _prefixed_variants(result, bare)
        # Several variants is the documented ambiguous case: the bare code is
        # deliberately left in place for a human, and is not a split history.
        if len(variants) == 1:
            twins.append(f"{bare} + {variants[0]}")
    if twins:
        raise SystemExit(
            "refusing to write a split product history — bare code kept beside its "
            f"prefixed twin: {', '.join(twins)}\n"
            "If --ours and --theirs were transposed, swap them and re-run."
        )


def report_unprefixed_local_codes(result: dict, new_keys: set[str], log: list[str]) -> None:
    """Flag bare local article numbers among the entries *ours* contributed.

    A server predating ``6b9c2d5`` still files GS1 restricted-distribution codes
    (short, or 13 digits starting with 2) under a bare key, but they are not
    globally unique and belong under ``<shop>-``.  Only newly arrived keys are
    reported: the bare codes already on main were left that way deliberately
    (manufacturer part numbers, unattributed codes).  Assigning the shop needs
    human knowledge of where the item was scanned, so this only reports.
    """
    for key in sorted(new_keys):
        if "-" in key:
            continue
        if len(key) < 13 or key.startswith("2"):
            log.append(f"[review] {key}: bare local code? ({result[key].get('name', '')!r})")


def run_merge_driver(ancestor_path: str, current_path: str, other_path: str) -> int:
    """Resolve a git merge of this file in-place; git's ``merge.<driver>.driver`` entry point.

    Git invokes this with ``%O %A %B``: the common-ancestor version, the version
    on the branch being merged *into* (which the driver must overwrite with the
    result), and the incoming version.  Exit 0 means resolved, non-zero means
    "record a conflict".

    ``%A`` is mapped to "ours" and ``%B`` to "theirs", so the *incoming* side
    wins on curated scalar fields.  That is right for the deployment this exists
    for — the server merges ``origin/main`` into its own branch, making ``%B``
    the curated shared history — and it is why the server must merge rather than
    rebase: a rebase swaps the two sides over, and the server's receipt-derived
    names would start overwriting the curated ones.
    """

    def load(path: str) -> dict:
        text = Path(path).read_text(encoding="utf-8").strip()
        return json.loads(text) if text else {}

    try:
        base, ours, theirs = load(ancestor_path), load(current_path), load(other_path)
    except (OSError, json.JSONDecodeError) as exc:
        print(f"merge_ean_db: cannot read inputs, leaving conflict for a human: {exc}", file=sys.stderr)
        return 1

    try:
        result, log, new_keys = merge(base, ours, theirs)
    except SystemExit as exc:  # the twin-pair invariant refused the result
        print(f"merge_ean_db: {exc}", file=sys.stderr)
        return 1

    report_unprefixed_local_codes(result, new_keys, log)
    Path(current_path).write_text(json.dumps(result, indent=2, ensure_ascii=False, sort_keys=True) + "\n")
    # Goes to the journal on an unattended run; it is the only record of what the
    # automatic resolution decided.
    print(f"merge_ean_db: resolved {EAN_DB_PATH.name} -> {len(result)} entries", file=sys.stderr)
    for line in log:
        print(f"  {line}", file=sys.stderr)
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ours", default="HEAD", help="'our' git ref (default: HEAD)")
    parser.add_argument("--theirs", default="main", help="'their' git ref (default: main)")
    parser.add_argument("--dry-run", action="store_true", help="report only, do not write the file")
    parser.add_argument(
        "--merge-driver",
        nargs=3,
        metavar=("ANCESTOR", "CURRENT", "OTHER"),
        help="run as a git merge driver over three files (%%O %%A %%B); result is written to CURRENT",
    )
    args = parser.parse_args()

    if args.merge_driver:
        raise SystemExit(run_merge_driver(*args.merge_driver))

    ours_sha = resolve_sha(args.ours)
    theirs_sha = resolve_sha(args.theirs)
    base_sha = merge_base(ours_sha, theirs_sha)

    path = str(EAN_DB_PATH)
    base = git_blob_json(base_sha, path)
    ours = git_blob_json(ours_sha, path)
    theirs = git_blob_json(theirs_sha, path)

    result, log, new_keys = merge(base, ours, theirs)
    report_unprefixed_local_codes(result, new_keys, log)

    for line in log:
        print(line)
    print(f"\nFinal: {len(result)} entries (base={len(base)}, ours={len(ours)}, theirs={len(theirs)})")

    if args.dry_run:
        print("Dry run: nothing written")
        return
    EAN_DB_PATH.write_text(json.dumps(result, indent=2, ensure_ascii=False, sort_keys=True) + "\n")
    print(f"Written to {EAN_DB_PATH}")


if __name__ == "__main__":
    main()
