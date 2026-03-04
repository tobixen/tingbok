"""tingbok command-line interface.

Entry point: ``tingbok`` (see ``[project.scripts]`` in pyproject.toml).

Subcommands
-----------
populate-uris
    Discover external source URIs for vocabulary concepts that have none and
    write the results back into the vocabulary YAML file.

    For each concept in ``vocabulary.yaml`` that lacks external ``source_uris``,
    the command queries DBpedia, Wikidata, and (when ``agrovoc.nt`` is present in
    the SKOS cache directory) AGROVOC via the local Oxigraph store.  Discovered
    URIs are written back to ``vocabulary.yaml`` using ruamel.yaml so that
    existing comments and formatting are preserved.

    Run ``tingbok populate-uris --help`` for details.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# populate-uris
# ---------------------------------------------------------------------------


def _populate_uris(
    vocab_path: Path,
    cache_dir: Path,
    *,
    dry_run: bool = False,
    lang: str = "en",
) -> int:
    """Core logic for the ``populate-uris`` subcommand.

    Returns an exit code (0 = success).
    """
    try:
        from ruamel.yaml import YAML  # noqa: PLC0415
    except ImportError:
        print("ruamel.yaml is required for populate-uris.  Install it with: pip install ruamel.yaml", file=sys.stderr)
        return 1

    if not vocab_path.exists():
        print(f"Error: vocabulary file not found: {vocab_path}", file=sys.stderr)
        return 1

    from tingbok.services import skos as skos_service  # noqa: PLC0415

    # --- Load vocabulary preserving comments ---
    yaml = YAML()
    yaml.preserve_quotes = True
    with open(vocab_path) as f:
        doc = yaml.load(f)

    concepts: dict = doc.get("concepts", {})

    # --- Determine available sources ---
    store = skos_service.get_agrovoc_store(cache_dir)
    sources = []
    if store is not None:
        sources.append("agrovoc")
    sources.extend(["dbpedia", "wikidata"])

    # --- Discover URIs ---
    updates: dict[str, list[str]] = {}  # concept_id -> list of new URIs

    for concept_id, data in concepts.items():
        if data is None:
            continue
        static_uris: list[str] = list(data.get("source_uris") or [])
        excluded: set[str] = set(data.get("excluded_sources") or [])

        # Skip if already has at least one non-tingbok external URI
        has_external = any(not u.startswith("https://tingbok.plann.no/") for u in static_uris)
        if has_external:
            continue

        label: str = data.get("prefLabel") or concept_id.split("/")[-1].replace("_", " ")
        discovered: list[str] = []

        for source in sources:
            if source in excluded:
                continue
            try:
                concept = skos_service.lookup_concept(label, lang, source, cache_dir)
                if concept and concept.get("uri"):
                    uri = concept["uri"]
                    if uri not in static_uris and uri not in discovered:
                        discovered.append(uri)
            except Exception as exc:  # noqa: BLE001
                print(f"  Warning: lookup failed for '{concept_id}' via {source}: {exc}", file=sys.stderr)

        if discovered:
            updates[concept_id] = discovered

    if not updates:
        print("No new source URIs discovered.")
        return 0

    # --- Report / apply ---
    print(f"{'(dry-run) ' if dry_run else ''}Discovered URIs for {len(updates)} concept(s):")
    for concept_id, uris in sorted(updates.items()):
        for uri in uris:
            print(f"  {concept_id}: {uri}")

    if dry_run:
        return 0

    # Apply to ruamel.yaml document (preserves comments)
    for concept_id, new_uris in updates.items():
        data = concepts[concept_id]
        existing: list[str] = list(data.get("source_uris") or [])
        for uri in new_uris:
            if uri not in existing:
                existing.append(uri)
        data["source_uris"] = existing

    with open(vocab_path, "w") as f:
        yaml.dump(doc, f)

    print(f"Updated {vocab_path}")
    return 0


# ---------------------------------------------------------------------------
# Argument parsing + dispatch
# ---------------------------------------------------------------------------


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="tingbok",
        description="tingbok vocabulary management tools",
    )
    sub = parser.add_subparsers(dest="command", metavar="COMMAND")

    # populate-uris
    p = sub.add_parser(
        "populate-uris",
        help="Discover and write source URIs for vocabulary concepts",
        description=(
            "For each concept in vocabulary.yaml that lacks external source_uris, "
            "query DBpedia, Wikidata, and (when agrovoc.nt is present) AGROVOC via "
            "the local Oxigraph store.  Write discovered URIs back into the YAML file."
        ),
    )
    p.add_argument("vocabulary", metavar="VOCABULARY_YAML", help="Path to vocabulary.yaml")
    p.add_argument(
        "--cache-dir",
        metavar="DIR",
        default=None,
        help="SKOS cache directory (default: ~/.cache/tingbok/skos)",
    )
    p.add_argument("--dry-run", action="store_true", help="Print proposed changes without modifying the file")
    p.add_argument("--lang", default="en", metavar="LANG", help="Language for concept lookup (default: en)")

    return parser


def main() -> None:
    """Entry point for the ``tingbok`` CLI."""
    parser = _build_parser()
    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        sys.exit(0)

    if args.command == "populate-uris":
        from os import environ  # noqa: PLC0415

        cache_dir = (
            Path(args.cache_dir)
            if args.cache_dir
            else (Path(environ.get("TINGBOK_CACHE_DIR", Path.home() / ".cache" / "tingbok")) / "skos")
        )
        rc = _populate_uris(
            Path(args.vocabulary),
            cache_dir,
            dry_run=args.dry_run,
            lang=args.lang,
        )
        sys.exit(rc)
