"""tingbok command-line interface.

Entry point: ``tingbok`` (see ``[project.scripts]`` in pyproject.toml).

Subcommands
-----------
populate-uris
    Discover external source URIs for vocabulary concepts that have none and
    write the results back into the vocabulary YAML file.

    For each concept in ``vocabulary.yaml`` that lacks external ``source_uris``,
    the command queries AGROVOC (Oxigraph if available, else REST), DBpedia,
    Wikidata, and Google Product Taxonomy (GPT, if taxonomy files are present in
    the cache directory).  Discovered URIs are written back to ``vocabulary.yaml``
    using ruamel.yaml so that existing comments and formatting are preserved.

    Run ``tingbok populate-uris --help`` for details.

download-taxonomy
    Download external taxonomy databases into the local cache directory.

    ``--gpt [LOCALE ...]``
        Download Google Product Taxonomy (GPT) files.  When no locales are
        given, downloads the English (en-GB) file.  Locale codes are the
        BCP-47 locale tags used in the GPT filenames, e.g. ``nb-NO``,
        ``sv-SE``, ``de-DE``.  Files are stored as
        ``{cache_dir}/gpt/taxonomy-with-ids.{locale}.txt``.

    ``--agrovoc``
        Download the latest AGROVOC LOD N-Triples dump from FAO and extract
        ``agrovoc.nt`` into ``{cache_dir}/skos/``.

    ``--cache-dir DIR``
        Override the cache root (default: ``/var/cache/tingbok``).

    Run ``tingbok download-taxonomy --help`` for details.
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

    from tingbok.services import gpt as gpt_service  # noqa: PLC0415
    from tingbok.services import off as off_service  # noqa: PLC0415
    from tingbok.services import skos as skos_service  # noqa: PLC0415

    # --- Load vocabulary preserving comments ---
    yaml = YAML()
    yaml.preserve_quotes = True
    with open(vocab_path) as f:
        doc = yaml.load(f)

    concepts: dict = doc.get("concepts", {})

    # cache_dir is the root cache directory.
    # SKOS (agrovoc/dbpedia/wikidata) caches live in cache_dir/skos/.
    # GPT files live in cache_dir/gpt/.
    skos_dir = cache_dir / "skos"

    # --- Discover URIs ---
    updates: dict[str, list[str]] = {}  # concept_id -> list of new URIs

    for concept_id, data in concepts.items():
        if data is None:
            continue
        static_uris: list[str] = list(data.get("source_uris") or [])
        excluded: set[str] = set(data.get("excluded_sources") or [])

        label: str = data.get("prefLabel") or concept_id.split("/")[-1].replace("_", " ")
        discovered: list[str] = []

        # SKOS sources: agrovoc always included (Oxigraph if available, REST fallback)
        for source in ("agrovoc", "dbpedia", "wikidata"):
            if source in excluded:
                continue
            try:
                concept = skos_service.lookup_concept(label, lang, source, skos_dir)
                if concept and concept.get("uri"):
                    uri = concept["uri"]
                    if uri not in static_uris and uri not in discovered:
                        discovered.append(uri)
            except Exception as exc:  # noqa: BLE001
                print(f"  Warning: lookup failed for '{concept_id}' via {source}: {exc}", file=sys.stderr)

        # GPT source (local taxonomy files, no network required)
        if "gpt" not in excluded:
            try:
                gpt_concept = gpt_service.lookup_concept(label, lang, cache_dir)
                if gpt_concept and gpt_concept.get("uri"):
                    uri = gpt_concept["uri"]
                    if uri not in static_uris and uri not in discovered:
                        discovered.append(uri)
            except Exception as exc:  # noqa: BLE001
                print(f"  Warning: GPT lookup failed for '{concept_id}': {exc}", file=sys.stderr)

        # OFF source (openfoodfacts package, food taxonomy only)
        if "off" not in excluded:
            try:
                off_concept = off_service.lookup_concept(label, lang, skos_dir)
                if off_concept and off_concept.get("uri"):
                    uri = off_concept["uri"]
                    if uri not in static_uris and uri not in discovered:
                        discovered.append(uri)
            except Exception as exc:  # noqa: BLE001
                print(f"  Warning: OFF lookup failed for '{concept_id}': {exc}", file=sys.stderr)

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
# download-taxonomy
# ---------------------------------------------------------------------------

#: Default cache root used by download-taxonomy (system-wide).
_DEFAULT_DOWNLOAD_CACHE_DIR = "/var/cache/tingbok"

#: URL template for GPT taxonomy files.
_GPT_URL_TEMPLATE = "https://www.google.com/basepages/producttype/taxonomy-with-ids.{locale}.txt"

#: URL for the latest AGROVOC LOD N-Triples zip.
_AGROVOC_NT_ZIP_URL = "https://agrovoc.fao.org/latestAgrovoc/agrovoc_lod.nt.zip"


def _download_file(url: str, dest: Path, *, description: str = "") -> bool:
    """Download *url* to *dest*, streaming the response.

    Returns True on success, False on error.
    """
    import niquests  # noqa: PLC0415

    label = description or url
    print(f"Downloading {label} ...")
    try:
        response = niquests.get(url, stream=True, timeout=60)
        if response.status_code != 200:
            print(f"  Error: HTTP {response.status_code} for {url}", file=sys.stderr)
            return False
        dest.parent.mkdir(parents=True, exist_ok=True)
        with open(dest, "wb") as f:
            for chunk in response.iter_content(chunk_size=65536):
                if chunk:
                    f.write(chunk)
        print(f"  Saved to {dest}")
        return True
    except Exception as exc:  # noqa: BLE001
        print(f"  Error downloading {url}: {exc}", file=sys.stderr)
        return False


def _download_taxonomy(
    cache_dir: Path,
    *,
    gpt_locales: list[str] | None = None,
    agrovoc: bool = False,
) -> int:
    """Core logic for the ``download-taxonomy`` subcommand.

    Returns an exit code (0 = success, 1 = one or more downloads failed).
    """
    import zipfile  # noqa: PLC0415

    if not gpt_locales and not agrovoc:
        print("Nothing to download.  Use --gpt and/or --agrovoc.  Run with --help for details.")
        return 0

    errors = 0

    # --- Google Product Taxonomy ---
    if gpt_locales is not None:
        gpt_dir = cache_dir / "gpt"
        for locale in gpt_locales:
            url = _GPT_URL_TEMPLATE.format(locale=locale)
            dest = gpt_dir / f"taxonomy-with-ids.{locale}.txt"
            if not _download_file(url, dest, description=f"GPT taxonomy ({locale})"):
                errors += 1

    # --- AGROVOC LOD ---
    if agrovoc:
        skos_dir = cache_dir / "skos"
        zip_dest = skos_dir / "_agrovoc_lod.nt.zip"
        if _download_file(_AGROVOC_NT_ZIP_URL, zip_dest, description="AGROVOC LOD (N-Triples zip)"):
            print("  Extracting agrovoc.nt ...")
            try:
                with zipfile.ZipFile(zip_dest) as zf:
                    # Find the .nt member (name varies by release)
                    nt_members = [m for m in zf.namelist() if m.endswith(".nt")]
                    if not nt_members:
                        print("  Error: no .nt file found in zip archive", file=sys.stderr)
                        errors += 1
                    else:
                        member = nt_members[0]
                        dest_nt = skos_dir / "agrovoc.nt"
                        with zf.open(member) as src, open(dest_nt, "wb") as dst:
                            dst.write(src.read())
                        print(f"  Saved to {dest_nt}")
                zip_dest.unlink(missing_ok=True)
            except zipfile.BadZipFile as exc:
                print(f"  Error: bad zip file: {exc}", file=sys.stderr)
                errors += 1
        else:
            errors += 1

    return 0 if errors == 0 else 1


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
        help="Cache root directory (default: ~/.cache/tingbok).  SKOS caches are read from DIR/skos/, GPT files from DIR/gpt/.",
    )
    p.add_argument("--dry-run", action="store_true", help="Print proposed changes without modifying the file")
    p.add_argument("--lang", default="en", metavar="LANG", help="Language for concept lookup (default: en)")

    # download-taxonomy
    from tingbok.services.gpt import GPT_LOCALES  # noqa: PLC0415

    dt = sub.add_parser(
        "download-taxonomy",
        help="Download external taxonomy databases (GPT, AGROVOC) into the local cache",
        description=(
            "Download taxonomy data files into the cache directory.\n\n"
            "Use --gpt to download Google Product Taxonomy files and --agrovoc to\n"
            "download the latest AGROVOC LOD N-Triples dump from FAO.\n"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    dt.add_argument(
        "--cache-dir",
        metavar="DIR",
        default=_DEFAULT_DOWNLOAD_CACHE_DIR,
        help=f"Cache root directory (default: {_DEFAULT_DOWNLOAD_CACHE_DIR})",
    )
    dt.add_argument(
        "--gpt",
        nargs="*",
        metavar="LOCALE",
        help=(
            "Download Google Product Taxonomy file(s).  Without arguments downloads en-GB. "
            f"Known locales: {', '.join(GPT_LOCALES)}"
        ),
    )
    dt.add_argument(
        "--agrovoc",
        action="store_true",
        help="Download the latest AGROVOC LOD N-Triples dump from FAO",
    )

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
            else Path(environ.get("TINGBOK_CACHE_DIR", str(Path.home() / ".cache" / "tingbok")))
        )
        rc = _populate_uris(
            Path(args.vocabulary),
            cache_dir,
            dry_run=args.dry_run,
            lang=args.lang,
        )
        sys.exit(rc)

    if args.command == "download-taxonomy":
        cache_dir = Path(args.cache_dir)
        gpt_locales: list[str] | None = None
        if args.gpt is not None:
            # --gpt with no arguments → default to en-GB
            gpt_locales = args.gpt if args.gpt else ["en-GB"]
        rc = _download_taxonomy(cache_dir, gpt_locales=gpt_locales, agrovoc=args.agrovoc)
        sys.exit(rc)
