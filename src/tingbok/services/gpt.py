"""Google Product Taxonomy (GPT) lookup service.

Parses locally cached GPT text files (downloaded via ``tingbok download-taxonomy
--gpt``) and provides label-based concept lookup.

URI scheme: ``gpt:{numeric_id}``  (e.g. ``gpt:632`` for "Electronics").
This mirrors how OFF uses ``off://category:{name}`` — constructing a synthetic
URI rather than a misleading https URL.

GPT file format (one category per line after the version comment)::

    # Google_Product_Taxonomy_Version: 2021-09-21
    1 - Animals & Pet Supplies
    3237 - Animals & Pet Supplies > Live Animals
    632 - Electronics
    499 - Electronics > Cameras & Optics

Language files are stored as::

    {cache_dir}/gpt/taxonomy-with-ids.{locale}.txt

The ``lang`` parameter (e.g. ``"en"``, ``"nb"``) is mapped to the closest
available locale (e.g. ``"en"`` → ``en-GB``, ``"nb"`` → ``nb-NO``).
"""

from __future__ import annotations

import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Locale mapping: BCP-47 short code → locale used in GPT filename
# ---------------------------------------------------------------------------

#: Known GPT locale filenames published by Google.
#: https://www.google.com/basepages/producttype/taxonomy-with-ids.{locale}.txt
GPT_LOCALES: list[str] = [
    "en-GB",
    "en-US",
    "de-DE",
    "fr-FR",
    "it-IT",
    "nl-NL",
    "sv-SE",
    "no-NO",
    "da-DK",
    "fi-FI",
    "pt-BR",
    "ja-JP",
    "tr-TR",
    "pl-PL",
    "ru-RU",
    "ar-AE",
    "ko-KR",
    "zh-CN",
    "zh-TW",
]

#: Default locale to try first when no language-specific file is found.
_DEFAULT_LOCALE = "en-GB"

#: Map from short BCP-47 language tag to preferred GPT locale.
_LANG_TO_LOCALE: dict[str, str] = {
    "en": "en-GB",
    "de": "de-DE",
    "fr": "fr-FR",
    "it": "it-IT",
    "nl": "nl-NL",
    "sv": "sv-SE",
    "nb": "nb-NO",
    "no": "nb-NO",
    "nn": "nb-NO",
    "da": "da-DK",
    "fi": "fi-FI",
    "pt": "pt-BR",
    "ja": "ja-JP",
    "tr": "tr-TR",
    "pl": "pl-PL",
    "ru": "ru-RU",
    "ar": "ar-AE",
    "ko": "ko-KR",
    "zh": "zh-CN",
}


def _gpt_dir(cache_dir: Path) -> Path:
    return cache_dir / "gpt"


def _locale_for_lang(lang: str) -> str:
    """Return the best-matching GPT locale for a BCP-47 language tag."""
    if "-" in lang:
        # Already a locale like "nb-NO" — use as-is if known, else strip
        if lang in GPT_LOCALES:
            return lang
        lang = lang.split("-")[0]
    return _LANG_TO_LOCALE.get(lang, _DEFAULT_LOCALE)


def _locale_candidates(lang: str) -> list[str]:
    """Return ordered list of locale candidates for *lang*, falling back to en-GB."""
    primary = _locale_for_lang(lang)
    if primary == _DEFAULT_LOCALE:
        return [_DEFAULT_LOCALE]
    return [primary, _DEFAULT_LOCALE]


# ---------------------------------------------------------------------------
# Internal: parse a GPT file into {label_lower: (id, label, path_parts)} dict
# ---------------------------------------------------------------------------

#: Module-level cache: {file_path_str: parsed_index}
_parsed_cache: dict[str, dict[str, tuple[str, str, list[str]]]] = {}


def _parse_gpt_file(path: Path) -> dict[str, tuple[str, str, list[str]]]:
    """Parse a GPT taxonomy file.

    Returns a dict mapping ``label.lower()`` to ``(id, label, path_parts)``
    where *path_parts* is the list of ancestor labels from root to leaf.
    """
    key = str(path)
    if key in _parsed_cache:
        return _parsed_cache[key]

    index: dict[str, tuple[str, str, list[str]]] = {}
    try:
        with open(path, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                # Format: "632 - Electronics > Cameras & Optics"
                dash_idx = line.index(" - ")
                gpt_id = line[:dash_idx].strip()
                full_path = line[dash_idx + 3 :].strip()
                parts = [p.strip() for p in full_path.split(">")]
                label = parts[-1]
                index[label.lower()] = (gpt_id, label, parts)
    except OSError as e:
        logger.warning("Failed to parse GPT file %s: %s", path, e)

    _parsed_cache[key] = index
    return index


def _id_index(path: Path) -> dict[str, tuple[str, list[str]]]:
    """Return a ``{gpt_id: (label, path_parts)}`` index for *path*."""
    parsed = _parse_gpt_file(path)
    return {gpt_id: (label, parts) for _, (gpt_id, label, parts) in parsed.items()}


def _find_gpt_file(lang: str, cache_dir: Path) -> Path | None:
    """Return the first existing GPT file for *lang*, or None."""
    gpt_dir = _gpt_dir(cache_dir)
    for locale in _locale_candidates(lang):
        p = gpt_dir / f"taxonomy-with-ids.{locale}.txt"
        if p.exists():
            return p
    return None


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def lookup_concept(label: str, lang: str, cache_dir: Path) -> dict | None:
    """Look up a GPT concept by label in the locally cached taxonomy file.

    Args:
        label:     Human-readable label (e.g. ``"Electronics"``).
        lang:      BCP-47 language code (e.g. ``"en"``, ``"nb"``).
        cache_dir: Cache directory that may contain ``gpt/taxonomy-with-ids.*.txt``.

    Returns:
        Concept dict with ``uri``, ``prefLabel``, ``source``, ``broader`` keys,
        or ``None`` if not found or no GPT file is available.
    """
    gpt_file = _find_gpt_file(lang, cache_dir)
    if gpt_file is None:
        return None

    index = _parse_gpt_file(gpt_file)

    # Try exact match, then simple plural/singular variants
    key = label.lower()
    label_candidates = [key]
    if key.endswith("s"):
        label_candidates.append(key[:-1])  # "pillows" → "pillow"
        if key.endswith("es") and len(key) > 3:
            label_candidates.append(key[:-2])  # "boxes" → "box"
    else:
        label_candidates.append(key + "s")  # "pillow" → "pillows"

    entry = None
    for candidate in label_candidates:
        entry = index.get(candidate)
        if entry is not None:
            break
    if entry is None:
        return None

    gpt_id, pref_label, path_parts = entry

    # Build broader list from parent path segments
    broader: list[dict] = []
    if len(path_parts) > 1:
        parent_label = path_parts[-2]
        parent_entry = index.get(parent_label.lower())
        if parent_entry:
            parent_id, _, _ = parent_entry
            broader = [{"uri": f"gpt:{parent_id}", "label": parent_label}]
        else:
            broader = [{"uri": "", "label": parent_label}]

    return {
        "uri": f"gpt:{gpt_id}",
        "prefLabel": pref_label,
        "source": "gpt",
        "broader": broader,
        "path_parts": path_parts,
    }


def get_labels(uri: str, languages: list[str], cache_dir: Path) -> dict[str, str]:
    """Fetch labels for a ``gpt:{id}`` URI in the requested languages.

    Looks up the numeric ID in each available language file.

    Args:
        uri:       GPT URI (e.g. ``"gpt:632"``).
        languages: List of BCP-47 language codes.
        cache_dir: Cache directory containing GPT files.

    Returns:
        Dict mapping language code to label string.
    """
    if not uri.startswith("gpt:"):
        return {}
    gpt_id = uri[4:]

    labels: dict[str, str] = {}
    gpt_dir = _gpt_dir(cache_dir)
    if not gpt_dir.exists():
        return {}

    # Build a mapping from locale → lang for available files
    locale_to_lang: dict[str, str] = {}
    for lang in languages:
        locale = _locale_for_lang(lang)
        locale_to_lang.setdefault(locale, lang)

    for locale, lang in locale_to_lang.items():
        if lang in labels:
            continue
        p = gpt_dir / f"taxonomy-with-ids.{locale}.txt"
        if not p.exists():
            continue
        id_idx = _id_index(p)
        entry = id_idx.get(gpt_id)
        if entry:
            label, _ = entry
            labels[lang] = label

    return labels
