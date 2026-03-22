#!/usr/bin/env python3
"""Clean up stale/junk entries from the SKOS concept cache.

Scans the SKOS cache directory and removes entries that were cached before
the current filters were in place:

  - DBpedia list articles  (List_of_* / Lists_of_*)
  - DBpedia disambiguation pages  (*_(disambiguation)*)
  - Vocabulary source_uris that point to the above

Also prints a summary of what was removed and flags any remaining entries
whose prefLabel contains "(disambiguation)" for manual review.

Usage:
    python scripts/clean_skos_cache.py [--cache-dir DIR] [--dry-run] [--vocab PATH]

Options:
    --cache-dir DIR   SKOS cache directory (default: ~/.cache/tingbok/skos)
    --dry-run         Print what would be deleted without deleting anything
    --vocab PATH      Path to vocabulary.yaml to scan for bad source_uris
                      (default: src/tingbok/data/vocabulary.yaml)
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


def _is_junk_dbpedia_uri(uri: str) -> bool:
    """Return True if *uri* is a list article or disambiguation page."""
    local = uri.rsplit("/", 1)[-1]
    return local.startswith("List_of_") or local.startswith("Lists_of_") or "_(disambiguation)" in local


def scan_skos_cache(cache_dir: Path, dry_run: bool) -> tuple[int, int]:
    """Scan *cache_dir* for junk concept entries.

    Returns ``(deleted, flagged)`` counts.
    """
    deleted = 0
    flagged = 0

    if not cache_dir.exists():
        print(f"Cache dir not found: {cache_dir}", file=sys.stderr)
        return 0, 0

    for path in sorted(cache_dir.rglob("*.json")):
        if path.name.startswith("_"):
            continue
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except Exception as e:
            print(f"  SKIP (unreadable): {path.name}: {e}")
            continue

        cache_key: str = data.get("_cache_key", "")
        if not cache_key.startswith("concept:dbpedia:"):
            continue

        uri: str = data.get("uri", "")
        pref_label: str = data.get("prefLabel", "")

        if uri and _is_junk_dbpedia_uri(uri):
            label_part = cache_key.split(":", 3)[-1]
            action = "WOULD DELETE" if dry_run else "DELETE"
            print(f"  {action}: {label_part!r} → {uri}")
            if not dry_run:
                path.unlink()
            deleted += 1
        elif "(disambiguation)" in pref_label.lower():
            label_part = cache_key.split(":", 3)[-1]
            print(f"  REVIEW: {label_part!r} → {uri!r}  (prefLabel contains 'disambiguation')")
            flagged += 1

    return deleted, flagged


def scan_vocabulary(vocab_path: Path, dry_run: bool) -> int:
    """Scan *vocab_path* for source_uris pointing to junk DBpedia pages.

    Prints a report but does NOT modify vocabulary.yaml — use the PUT API
    (``remove_source_uris``) to remove bad URIs from a running instance.
    Returns the count of bad URIs found.
    """
    if not vocab_path.exists():
        return 0

    try:
        import yaml  # noqa: PLC0415

        with open(vocab_path) as f:
            doc = yaml.safe_load(f)
    except Exception as e:
        print(f"  SKIP (unreadable vocabulary): {e}")
        return 0

    found = 0
    for concept_id, data in (doc.get("concepts") or {}).items():
        if not data:
            continue
        for uri in data.get("source_uris") or []:
            if uri.startswith(("http://dbpedia.org/", "https://dbpedia.org/")) and _is_junk_dbpedia_uri(uri):
                print(f"  VOCAB BAD URI: {concept_id}: {uri}")
                found += 1
    return found


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=Path.home() / ".cache" / "tingbok" / "skos",
        help="SKOS cache directory (default: ~/.cache/tingbok/skos)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print what would be deleted without deleting anything",
    )
    parser.add_argument(
        "--vocab",
        type=Path,
        default=Path(__file__).parent.parent / "src" / "tingbok" / "data" / "vocabulary.yaml",
        help="Path to vocabulary.yaml",
    )
    args = parser.parse_args()

    if args.dry_run:
        print("DRY RUN — nothing will be deleted\n")

    print(f"Scanning SKOS cache: {args.cache_dir}")
    deleted, flagged = scan_skos_cache(args.cache_dir, args.dry_run)

    print(f"\nScanning vocabulary: {args.vocab}")
    bad_uris = scan_vocabulary(args.vocab, args.dry_run)

    print("\nSummary:")
    if args.dry_run:
        print(f"  Would delete: {deleted}")
    else:
        print(f"  Deleted:      {deleted}")
    print(f"  Flagged for review: {flagged}")
    print(f"  Bad vocab URIs:     {bad_uris}")
    if bad_uris:
        print("  → Remove bad vocab URIs via PUT /api/vocabulary/<id> with remove_source_uris.")


if __name__ == "__main__":
    main()
