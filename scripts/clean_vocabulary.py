#!/usr/bin/env python3
"""Clean up source_uris in vocabulary.yaml.

Removes junk URIs, normalises http→https, and deduplicates.
With --check-types also removes persons, places, and disambiguation pages
by fetching RDF type information from DBpedia and Wikidata.

The "is this a non-concept?" algorithm lives in tingbok.services.skos so
that re-running this script after an upgrade automatically applies any
improvements to the classification logic.

Usage:
    python scripts/clean_vocabulary.py [--vocab PATH] [--dry-run] [--check-types]

Options:
    --vocab PATH    Path to vocabulary.yaml
                    (default: src/tingbok/data/vocabulary.yaml)
    --dry-run       Print changes without writing anything
    --check-types   Also fetch RDF types from DBpedia/Wikidata to detect
                    persons, geographic places, etc.  Slow — makes one
                    network request per unique URI.
"""

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

# Allow running directly from the repo root without an editable install.
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from tingbok.services.skos import is_junk_uri, is_non_concept_uri

_TINGBOK_CONF = Path("/etc/tingbok/tingbok.conf")


def _resolve_cache_dir() -> Path:
    """Return the SKOS cache directory.

    Resolution order:
    1. ``TINGBOK_CACHE_DIR`` environment variable (set when running as the
       service user or after sourcing the conf file).
    2. ``TINGBOK_CACHE_DIR`` parsed directly from ``/etc/tingbok/tingbok.conf``.
    3. ``~/.cache/tingbok`` user-level fallback.
    """
    if "TINGBOK_CACHE_DIR" in os.environ:
        return Path(os.environ["TINGBOK_CACHE_DIR"]) / "skos"
    if _TINGBOK_CONF.exists():
        for line in _TINGBOK_CONF.read_text().splitlines():
            line = line.strip()
            if line.startswith("TINGBOK_CACHE_DIR="):
                return Path(line.split("=", 1)[1].strip()) / "skos"
    return Path.home() / ".cache" / "tingbok" / "skos"


def normalise_uri(uri: str) -> str:
    """Upgrade http:// to https:// for well-known LOD hosts."""
    if uri.startswith("http://"):
        return "https://" + uri[7:]
    return uri


def process_uris(
    uris: list[str],
    check_types: bool = False,
    cache_dir: Path | None = None,
) -> tuple[list[str], list[tuple[str, str]]]:
    """Process a source_uris list: normalise, deduplicate, remove junk.

    Returns ``(kept, removed)`` where *removed* is a list of
    ``(original_uri, reason)`` pairs.
    """
    kept: list[str] = []
    removed: list[tuple[str, str]] = []
    seen: set[str] = set()

    for raw in uris:
        normed = normalise_uri(raw)

        if normed in seen:
            removed.append((raw, "duplicate after normalisation"))
            continue
        seen.add(normed)

        if is_junk_uri(normed):
            removed.append((raw, "junk URI pattern"))
            continue

        if check_types:
            result = is_non_concept_uri(normed, cache_dir=cache_dir)
            if result is True:
                removed.append((raw, "blocked type (person/place/disambiguation)"))
                continue
            # result is False (valid) or None (network error / unsupported) — keep

        kept.append(normed)

    return kept, removed


def clean_vocabulary(
    vocab_path: Path,
    dry_run: bool = False,
    check_types: bool = False,
    cache_dir: Path | None = None,
) -> dict[str, int]:
    """Clean *vocab_path* in-place.

    Returns a summary dict with ``removed`` and ``normalised`` counts.
    """
    try:
        from ruamel.yaml import YAML  # noqa: PLC0415
    except ImportError:
        print("ERROR: ruamel.yaml is required.  pip install ruamel.yaml", file=sys.stderr)
        sys.exit(1)

    yaml = YAML()
    yaml.preserve_quotes = True

    with open(vocab_path, encoding="utf-8") as f:
        doc = yaml.load(f)

    concepts: dict = (doc or {}).get("concepts") or {}
    total_removed = 0
    total_normalised = 0

    for concept_id, data in concepts.items():
        if not data:
            continue
        raw_uris = data.get("source_uris")
        if not raw_uris:
            continue
        # Normalise scalar string → single-element list (malformed YAML entry)
        uris: list[str] = [raw_uris] if isinstance(raw_uris, str) else list(raw_uris)

        new_uris, removed = process_uris(uris, check_types=check_types, cache_dir=cache_dir)

        # Count http→https normalisations
        for orig, new in zip(uris, [normalise_uri(u) for u in uris], strict=True):
            if orig != new and new in new_uris:
                total_normalised += 1

        if removed:
            for uri, reason in removed:
                print(f"  REMOVE [{concept_id}] {uri!r}  ({reason})")
            total_removed += len(removed)

        if not dry_run and (new_uris != list(uris)):
            if new_uris:
                data["source_uris"] = new_uris
            else:
                del data["source_uris"]

    if not dry_run:
        with open(vocab_path, "w", encoding="utf-8") as f:
            yaml.dump(doc, f)

    return {"removed": total_removed, "normalised": total_normalised}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        "--vocab",
        type=Path,
        default=Path(__file__).parent.parent / "src" / "tingbok" / "data" / "vocabulary.yaml",
        help="Path to vocabulary.yaml",
    )
    parser.add_argument("--dry-run", action="store_true", help="Print changes without writing")
    parser.add_argument(
        "--check-types",
        action="store_true",
        help="Fetch RDF types from DBpedia/Wikidata (slow; cached in --cache-dir)",
    )
    parser.add_argument(
        "--cache-dir",
        type=Path,
        default=_resolve_cache_dir(),
        help="SKOS cache directory for type-check results (default: from TINGBOK_CACHE_DIR / /etc/tingbok/tingbok.conf)",
    )
    args = parser.parse_args()

    if args.dry_run:
        print("DRY RUN — nothing will be written\n")

    print(f"Vocabulary: {args.vocab}")
    if args.check_types:
        print(f"Type-checking enabled — caching results in {args.cache_dir}\n")

    cache_dir = args.cache_dir if args.check_types else None
    report = clean_vocabulary(args.vocab, dry_run=args.dry_run, check_types=args.check_types, cache_dir=cache_dir)

    print("\nSummary:")
    action = "Would remove" if args.dry_run else "Removed"
    print(f"  {action}: {report['removed']} URI(s)")
    print(f"  Normalised (http→https): {report['normalised']}")


if __name__ == "__main__":
    main()
