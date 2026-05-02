#!/usr/bin/env python3
"""Merge src/tingbok/data/ean-db.json across two git branches.

Usage:
    python3 scripts/merge_ean_db.py [--ours <ref>] [--theirs <ref>]

Defaults to merging HEAD (current branch) into main, using the common
merge-base as the ancestor.  The strategy is:

- Start from the "theirs" snapshot (main by default).
- Add entries that exist only in "ours".
- For keys modified in "ours" but unchanged in "theirs", keep "ours".
- For true conflicts (same EAN added independently on both sides), prefer
  "theirs" for all fields except "prices", which is taken from "ours" when
  "theirs" has none.

The result is written back to src/tingbok/data/ean-db.json in the working
tree (not committed automatically).
"""

import argparse
import copy
import json
import subprocess
from pathlib import Path

EAN_DB_PATH = Path("src/tingbok/data/ean-db.json")


def git_blob_json(ref: str, path: str) -> dict:
    raw = subprocess.check_output(["git", "cat-file", "-p", f"{ref}:{path}"])
    return json.loads(raw)


def resolve_sha(ref: str) -> str:
    return subprocess.check_output(["git", "rev-parse", ref], text=True).strip()


def merge_base(ref_a: str, ref_b: str) -> str:
    return subprocess.check_output(["git", "merge-base", ref_a, ref_b], text=True).strip()


def merge(base: dict, ours: dict, theirs: dict) -> tuple[dict, list[str]]:
    result = copy.deepcopy(theirs)
    log: list[str] = []

    only_ours = set(ours) - set(base) - set(theirs)
    for k in only_ours:
        result[k] = ours[k]
        log.append(f"[add ours-only] {k}: {ours[k].get('name', '')}")

    modified_ours = {k for k in base if k in ours and ours[k] != base[k]}
    modified_theirs = {k for k in base if k in theirs and theirs[k] != base[k]}
    for k in modified_ours - modified_theirs:
        result[k] = ours[k]
        log.append(f"[keep ours-mod] {k}: {ours[k].get('name', '')}")

    both_new = (set(ours) - set(base)) & (set(theirs) - set(base))
    for k in sorted(both_new):
        merged = copy.deepcopy(theirs[k])
        if "prices" in ours[k] and "prices" not in merged:
            merged["prices"] = ours[k]["prices"]
            log.append(f"[conflict+prices] {k}: theirs name, ours prices")
        else:
            log.append(f"[conflict] {k}: using theirs ({theirs[k].get('name', '')!r})")
        result[k] = merged

    return dict(sorted(result.items())), log


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ours", default="HEAD", help="'our' git ref (default: HEAD)")
    parser.add_argument("--theirs", default="main", help="'their' git ref (default: main)")
    args = parser.parse_args()

    ours_sha = resolve_sha(args.ours)
    theirs_sha = resolve_sha(args.theirs)
    base_sha = merge_base(ours_sha, theirs_sha)

    path = str(EAN_DB_PATH)
    base = git_blob_json(base_sha, path)
    ours = git_blob_json(ours_sha, path)
    theirs = git_blob_json(theirs_sha, path)

    result, log = merge(base, ours, theirs)

    for line in log:
        print(line)
    print(f"\nFinal: {len(result)} entries (base={len(base)}, ours={len(ours)}, theirs={len(theirs)})")

    EAN_DB_PATH.write_text(json.dumps(result, indent=2, ensure_ascii=False) + "\n")
    print(f"Written to {EAN_DB_PATH}")


if __name__ == "__main__":
    main()
