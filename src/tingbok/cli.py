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
import json
import sys
import time
from pathlib import Path

# ---------------------------------------------------------------------------
# populate-uris
# ---------------------------------------------------------------------------


def _normalise_uri(uri: str) -> str:
    """Upgrade http:// to https:// for normalised comparison."""
    if uri.startswith("http://"):
        return "https://" + uri[7:]
    return uri


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
        # Normalised set for dedup comparison (http ↔ https transparent)
        existing_normalised: set[str] = {_normalise_uri(u) for u in static_uris}

        label: str = data.get("prefLabel") or concept_id.split("/")[-1].replace("_", " ")
        discovered: list[str] = []

        # SKOS sources: agrovoc always included (Oxigraph if available, REST fallback)
        for source in ("agrovoc", "dbpedia", "wikidata"):
            if source in excluded:
                continue
            try:
                concept = skos_service.lookup_concept(label, lang, source, skos_dir)
                if concept and concept.get("uri"):
                    n = _normalise_uri(concept["uri"])
                    if n not in existing_normalised:
                        existing_normalised.add(n)
                        discovered.append(concept["uri"])
            except Exception as exc:  # noqa: BLE001
                print(f"  Warning: lookup failed for '{concept_id}' via {source}: {exc}", file=sys.stderr)

        # GPT source (local taxonomy files, no network required)
        if "gpt" not in excluded:
            try:
                gpt_concept = gpt_service.lookup_concept(label, lang, cache_dir)
                if gpt_concept and gpt_concept.get("uri"):
                    n = _normalise_uri(gpt_concept["uri"])
                    if n not in existing_normalised:
                        existing_normalised.add(n)
                        discovered.append(gpt_concept["uri"])
            except Exception as exc:  # noqa: BLE001
                print(f"  Warning: GPT lookup failed for '{concept_id}': {exc}", file=sys.stderr)

        # OFF source (openfoodfacts package, food taxonomy only)
        if "off" not in excluded:
            try:
                off_concept = off_service.lookup_concept(label, lang, skos_dir)
                if off_concept and off_concept.get("uri"):
                    n = _normalise_uri(off_concept["uri"])
                    if n not in existing_normalised:
                        existing_normalised.add(n)
                        discovered.append(off_concept["uri"])
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
# prune-vocabulary
# ---------------------------------------------------------------------------


def _normalize_for_match(label: str) -> str:
    """Normalise a label string for loose matching.

    Lowercases, strips leading/trailing whitespace, and reduces internal
    whitespace to a single space.
    """
    return " ".join(label.lower().split())


#: Similarity ratio threshold (0–100) above which two labels are considered
#: near-matches (e.g. plural/singular variants) and are removed without a
#: deviation warning.  Uses rapidfuzz when available.
_NEAR_MATCH_THRESHOLD = 85


def _labels_similarity(label1: str, label2: str) -> float:
    """Return a 0–100 similarity score between two normalised labels.

    Uses rapidfuzz.fuzz.ratio when available; falls back to 100 for exact
    matches and 0 otherwise.
    """
    n1 = _normalize_for_match(label1)
    n2 = _normalize_for_match(label2)
    if n1 == n2:
        return 100.0
    try:
        from rapidfuzz import fuzz  # noqa: PLC0415

        return fuzz.ratio(n1, n2)
    except ImportError:
        return 0.0


def _labels_match(vocab_label: str, source_label: str) -> bool:
    """Return True if two labels are an exact (normalised) match."""
    return _normalize_for_match(vocab_label) == _normalize_for_match(source_label)


def _labels_near_match(vocab_label: str, source_label: str) -> bool:
    """Return True if labels are similar enough to be treated as equivalent.

    Catches common plural/singular differences across languages without
    requiring language-specific stemming.
    """
    return _labels_similarity(vocab_label, source_label) >= _NEAR_MATCH_THRESHOLD


def _prune_vocabulary(
    vocab_path: Path,
    cache_dir: Path,
    *,
    dry_run: bool = False,
    lang: str = "en",
) -> int:
    """Core logic for the ``prune-vocabulary`` subcommand.

    For each concept in ``vocabulary.yaml`` that has ``source_uris``, fetch
    translations from those sources and compare against the ``labels:`` block
    in the vocabulary file.  Labels that match a source translation are removed
    (they are redundant — the translation will come from the source at runtime).
    Labels that deviate from source translations are reported so the user can
    review them manually.

    Returns an exit code (0 = success).
    """
    try:
        from ruamel.yaml import YAML  # noqa: PLC0415
    except ImportError:
        print(
            "ruamel.yaml is required for prune-vocabulary.  Install it with: pip install ruamel.yaml", file=sys.stderr
        )
        return 1

    if not vocab_path.exists():
        print(f"Error: vocabulary file not found: {vocab_path}", file=sys.stderr)
        return 1

    from tingbok.services import skos as skos_service  # noqa: PLC0415

    yaml = YAML()
    yaml.preserve_quotes = True
    with open(vocab_path) as f:
        doc = yaml.load(f)

    concepts: dict = doc.get("concepts", {})
    skos_dir = cache_dir / "skos"

    # Languages to check (all languages used in vocabulary.yaml labels blocks)
    all_langs: set[str] = set()
    for data in concepts.values():
        if data and data.get("labels"):
            all_langs.update(data["labels"].keys())
    languages = list(all_langs) if all_langs else [lang]

    removals: dict[str, dict[str, set[str]]] = {}  # concept_id -> lang -> set of values to remove
    alt_removals: dict[str, dict[str, set[str]]] = {}  # concept_id -> lang -> set of altLabel values to remove
    deviations: list[str] = []

    print(f"Checking {len(concepts)} concepts against sources...")

    for concept_id, data in concepts.items():
        if data is None:
            continue
        static_labels: dict = data.get("labels") or {}
        alt_labels: dict = data.get("altLabel") or {}
        if not static_labels and not alt_labels:
            continue

        source_uris: list[str] = list(data.get("source_uris") or [])
        if not source_uris:
            continue

        # Fetch translations from all source URIs, tracking per-source labels
        per_source: dict[str, dict[str, str]] = {}  # {source_name: {lang: label}}
        for uri in source_uris:
            if not uri or uri.startswith("https://tingbok.plann.no/"):
                continue
            # Determine source from URI prefix
            if uri.startswith(("http://aims.fao.org/", "https://aims.fao.org/")):
                source = "agrovoc"
            elif uri.startswith(("http://dbpedia.org/", "https://dbpedia.org/")):
                source = "dbpedia"
            elif uri.startswith(("http://www.wikidata.org/", "https://www.wikidata.org/")):
                source = "wikidata"
            else:
                continue  # off/gpt don't support get_labels via SKOS service

            try:
                fetched = skos_service.get_labels(uri, languages, source, skos_dir)
                if fetched:
                    existing = per_source.setdefault(source, {})
                    for fetch_lang, fetch_label in fetched.items():
                        if fetch_lang not in existing:
                            existing[fetch_lang] = fetch_label
            except Exception as exc:  # noqa: BLE001
                print(f"  Warning: label fetch failed for '{concept_id}' ({uri}): {exc}", file=sys.stderr)

        # Fetch alt-labels from all source URIs and flag redundant altLabel entries
        for uri in source_uris:
            if not uri or uri.startswith("https://tingbok.plann.no/"):
                continue
            if uri.startswith(("http://aims.fao.org/", "https://aims.fao.org/")):
                source = "agrovoc"
            elif uri.startswith(("http://dbpedia.org/", "https://dbpedia.org/")):
                source = "dbpedia"
            elif uri.startswith(("http://www.wikidata.org/", "https://www.wikidata.org/")):
                source = "wikidata"
            else:
                continue
            try:
                fetched_alts = skos_service.get_alt_labels(uri, languages, source, skos_dir)
            except Exception as exc:  # noqa: BLE001
                print(f"  Warning: alt-label fetch failed for '{concept_id}' ({uri}): {exc}", file=sys.stderr)
                continue
            for fetch_lang, fetch_vals in (fetched_alts or {}).items():
                vocab_alts = alt_labels.get(fetch_lang, [])
                for fetch_val in fetch_vals:
                    for vocab_val in vocab_alts:
                        if _labels_match(vocab_val, fetch_val) or _labels_near_match(vocab_val, fetch_val):
                            alt_removals.setdefault(concept_id, {}).setdefault(fetch_lang, set()).add(vocab_val)
                            print(
                                f"  {concept_id} altLabel[{fetch_lang}]: '{vocab_val}' matches {source} → will remove"
                            )

        if not per_source:
            continue

        # Detect inter-source conflicts: different sources disagree on the same language
        all_source_names = list(per_source.keys())
        for i, src_a in enumerate(all_source_names):
            for src_b in all_source_names[i + 1 :]:
                for conflict_lang in per_source[src_a]:
                    val_a = per_source[src_a][conflict_lang]
                    val_b = per_source[src_b].get(conflict_lang)
                    if val_b and not _labels_near_match(val_a, val_b):
                        # Suppress if either value is a known altLabel
                        known = alt_labels.get(conflict_lang, [])
                        if any(
                            _normalize_for_match(al) in (_normalize_for_match(val_a), _normalize_for_match(val_b))
                            for al in known
                        ):
                            continue
                        deviations.append(
                            f"  CONFLICT {concept_id}[{conflict_lang}]: {src_a}='{val_a}' vs {src_b}='{val_b}'"
                        )

        # Merged source labels (first source wins per language), tracking provider
        source_labels: dict[str, tuple[str, str]] = {}  # {lang: (source_name, label)}
        for src_name, labels in per_source.items():
            for fetch_lang, fetch_label in labels.items():
                if fetch_lang not in source_labels:
                    source_labels[fetch_lang] = (src_name, fetch_label)

        # Compare each vocabulary label against merged source labels
        for label_lang, vocab_value in list(static_labels.items()):
            if label_lang not in source_labels:
                continue  # Source doesn't have this language — keep it

            providing_source, src_value = source_labels[label_lang]

            if _labels_match(vocab_value, src_value):
                removals.setdefault(concept_id, {}).setdefault(label_lang, set()).add(vocab_value)
                print(f"  {concept_id}[{label_lang}]: '{vocab_value}' matches {providing_source} → will remove")
            elif _labels_near_match(vocab_value, src_value):
                removals.setdefault(concept_id, {}).setdefault(label_lang, set()).add(vocab_value)
                print(
                    f"  {concept_id}[{label_lang}]: '{vocab_value}' ≈ {providing_source} '{src_value}'"
                    f" (near-match, likely plural/singular) → will remove"
                )
            else:
                # Suppress if the source label is a known altLabel for this language
                known = alt_labels.get(label_lang, [])
                if any(_normalize_for_match(al) == _normalize_for_match(src_value) for al in known):
                    continue
                deviations.append(
                    f"  {concept_id}[{label_lang}]: vocab='{vocab_value}' vs {providing_source}='{src_value}'"
                )

    if deviations:
        print("\nDeviations (kept, need manual review):")
        for line in deviations:
            print(line)

    total_labels = sum(len(langs) for langs in removals.values())
    total_alts = sum(len(vals) for langs in alt_removals.values() for vals in langs.values())

    if not removals and not alt_removals:
        print("\nNo redundant labels found.")
        return 0

    total = total_labels + total_alts
    print(f"\n{'(dry-run) ' if dry_run else ''}Would remove {total} redundant label(s).")

    if dry_run:
        return 0

    # Apply label removals
    for concept_id, lang_map in removals.items():
        data = concepts[concept_id]
        labels_block: dict = data.get("labels") or {}
        for remove_lang in lang_map:
            if remove_lang in labels_block:
                del labels_block[remove_lang]
        if not labels_block:
            del data["labels"]

    # Apply altLabel removals
    for concept_id, lang_map in alt_removals.items():
        data = concepts[concept_id]
        alt_block: dict = data.get("altLabel") or {}
        for remove_lang, remove_vals in lang_map.items():
            if remove_lang not in alt_block:
                continue
            remaining = [v for v in alt_block[remove_lang] if v not in remove_vals]
            if remaining:
                alt_block[remove_lang] = remaining
            else:
                del alt_block[remove_lang]
        if not alt_block:
            del data["altLabel"]

    with open(vocab_path, "w") as f:
        yaml.dump(doc, f)

    print(f"Updated {vocab_path}")
    return 0


# ---------------------------------------------------------------------------
# prune-cache
# ---------------------------------------------------------------------------

CACHE_MAX_AGE_DAYS = 60  # entries not accessed within this window are deleted


def _prune_cache(cache_dir: Path, max_age_days: int = CACHE_MAX_AGE_DAYS, *, dry_run: bool = False) -> int:
    """Delete SKOS cache files that have not been accessed within *max_age_days*.

    Uses ``_last_accessed`` when present, falling back to ``_cached_at``.  The
    not-found index (``_not_found.json``) is handled entry-by-entry.

    Returns the number of entries removed (or that would be removed in dry-run).
    """
    cutoff = time.time() - max_age_days * 86400
    removed = 0

    for cache_path in cache_dir.rglob("*.json"):
        if cache_path.name == "_not_found.json":
            removed += _prune_not_found_cache(cache_path, cutoff, dry_run=dry_run)
            continue
        try:
            with open(cache_path, encoding="utf-8") as f:
                data: dict = json.load(f)
        except (json.JSONDecodeError, OSError):
            continue
        last_used = data.get("_last_accessed") or data.get("_cached_at", 0)
        if last_used < cutoff:
            if not dry_run:
                cache_path.unlink(missing_ok=True)
            removed += 1

    return removed


def _prune_not_found_cache(cache_path: Path, cutoff: float, *, dry_run: bool = False) -> int:
    """Prune stale entries from a ``_not_found.json`` index file in-place."""
    try:
        with open(cache_path, encoding="utf-8") as f:
            data: dict = json.load(f)
    except (json.JSONDecodeError, OSError):
        return 0
    entries: dict = data.get("entries", {})
    stale = [k for k, v in entries.items() if v.get("cached_at", 0) < cutoff]
    if not stale:
        return 0
    if not dry_run:
        for k in stale:
            del entries[k]
        data["entries"] = entries
        try:
            with open(cache_path, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except OSError as exc:
            print(f"Warning: could not write {cache_path}: {exc}", file=sys.stderr)
    return len(stale)


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

    # prune-vocabulary
    pv = sub.add_parser(
        "prune-vocabulary",
        help="Remove redundant translations from vocabulary.yaml that already exist in sources",
        description=(
            "For each concept in vocabulary.yaml, fetch translations from its source_uris "
            "and compare against the labels: block.  Labels that match a source translation "
            "are removed (they will be served from the source at runtime).  Deviating labels "
            "are reported for manual review."
        ),
    )
    pv.add_argument("vocabulary", metavar="VOCABULARY_YAML", help="Path to vocabulary.yaml")
    pv.add_argument(
        "--cache-dir",
        metavar="DIR",
        default=None,
        help="Cache root directory (default: ~/.cache/tingbok).  SKOS caches are read from DIR/skos/.",
    )
    pv.add_argument("--dry-run", action="store_true", help="Print proposed changes without modifying the file")
    pv.add_argument("--lang", default="en", metavar="LANG", help="Fallback language for concept lookup (default: en)")

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

    # prune-cache
    pc = sub.add_parser(
        "prune-cache",
        help="Delete SKOS cache entries not accessed within the retention window",
        description=(
            "Scan the SKOS cache directory and delete files whose ``_last_accessed``\n"
            f"timestamp (falling back to ``_cached_at``) is older than {CACHE_MAX_AGE_DAYS} days.\n"
            "Run this periodically (e.g. via a systemd timer or cron job) to prevent\n"
            "unbounded cache growth while keeping recently used entries intact."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    pc.add_argument(
        "--cache-dir",
        metavar="DIR",
        default=None,
        help="Cache root directory (default: ~/.cache/tingbok).  Scans DIR/skos/ and DIR/ean/.",
    )
    pc.add_argument(
        "--max-age",
        metavar="DAYS",
        type=int,
        default=CACHE_MAX_AGE_DAYS,
        help=f"Delete entries not accessed for more than DAYS days (default: {CACHE_MAX_AGE_DAYS})",
    )
    pc.add_argument("--dry-run", action="store_true", help="Report what would be deleted without deleting anything")

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

    if args.command == "prune-vocabulary":
        from os import environ  # noqa: PLC0415

        cache_dir = (
            Path(args.cache_dir)
            if args.cache_dir
            else Path(environ.get("TINGBOK_CACHE_DIR", str(Path.home() / ".cache" / "tingbok")))
        )
        rc = _prune_vocabulary(
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

    if args.command == "prune-cache":
        from os import environ  # noqa: PLC0415

        cache_dir = (
            Path(args.cache_dir)
            if args.cache_dir
            else Path(environ.get("TINGBOK_CACHE_DIR", str(Path.home() / ".cache" / "tingbok")))
        )
        max_age = args.max_age
        dry_run = args.dry_run
        total = 0
        for subdir in ("skos", "ean"):
            d = cache_dir / subdir
            if d.exists():
                n = _prune_cache(d, max_age_days=max_age, dry_run=dry_run)
                if n:
                    action = "Would remove" if dry_run else "Removed"
                    print(f"  {action} {n} entries from {d}")
                total += n
        if not total:
            print("Nothing to prune.")
        elif dry_run:
            print(f"Dry run: {total} entries would be removed.")
        else:
            print(f"Pruned {total} entries.")
        sys.exit(0)
