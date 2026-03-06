"""Open Food Facts (OFF) category taxonomy lookup service.

Uses the ``openfoodfacts`` PyPI package to access ~14K food category nodes
with localized names, synonyms, and hierarchy navigation.  No network calls
at lookup time — the package handles download and caching internally.

URI scheme: ``off:{node_id}``  (e.g. ``off:en:potatoes``).
This mirrors the same scheme used in inventory-md.

Install the optional dependency to enable OFF support::

    pip install tingbok[off]
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

#: Module-level cached taxonomy (None = not loaded yet).
_taxonomy: object | None = None

#: Module-level cached label index: {label_lower: node_id}.
_label_index: dict[str, str] | None = None


def _get_taxonomy() -> object | None:
    """Lazily load the OFF category taxonomy.

    Returns the taxonomy object, or ``None`` if the ``openfoodfacts``
    package is not installed.
    """
    global _taxonomy  # noqa: PLW0603

    if _taxonomy is not None:
        return _taxonomy

    try:
        from openfoodfacts.taxonomy import get_taxonomy  # noqa: PLC0415

        logger.info("Loading Open Food Facts category taxonomy...")
        _taxonomy = get_taxonomy("category")
        logger.info("OFF taxonomy loaded: %d categories", len(_taxonomy))  # type: ignore[arg-type]
        return _taxonomy
    except ImportError:
        logger.debug(
            "openfoodfacts package not installed; OFF lookup unavailable.  Install with: pip install tingbok[off]"
        )
        return None
    except Exception as exc:
        logger.warning("Failed to load OFF taxonomy: %s", exc)
        return None


def _build_label_index(taxonomy: object) -> dict[str, str]:
    """Build a reverse index: ``label.lower() -> node_id``.

    Indexes prefLabels and synonyms in all languages.  Built once and
    stored in the module-level :data:`_label_index` cache.
    """
    global _label_index  # noqa: PLW0603

    if _label_index is not None:
        return _label_index

    index: dict[str, str] = {}
    for node in taxonomy.iter_nodes():  # type: ignore[union-attr]
        if node is None:
            continue
        node_id: str = node.id
        # Index names in all languages
        for name in node.names.values():
            if name:
                key = name.lower()
                if key not in index:
                    index[key] = node_id
        # Index synonyms (don't overwrite a name entry)
        for syns in node.synonyms.values():
            for syn in syns:
                key = syn.lower()
                if key not in index:
                    index[key] = node_id

    _label_index = index
    return _label_index


def _generate_variations(label: str) -> list[str]:
    """Generate singular/plural variations of *label* (lowercase).

    Mirrors ``_generate_variations`` in inventory-md's ``off.py``.
    """
    variations: list[str] = []

    # Plural → singular
    if label.endswith("ies") and len(label) > 4:
        variations.append(label[:-3] + "y")
    elif label.endswith("oes") and len(label) > 4:
        variations.append(label[:-2])
    elif label.endswith("es") and len(label) > 3:
        stem = label[:-2]
        if stem.endswith(("s", "x", "z", "ch", "sh")):
            variations.append(stem)
        else:
            variations.append(label[:-1])
    elif label.endswith("s") and not label.endswith(("ss", "us", "is")):
        variations.append(label[:-1])

    # Singular → plural
    if not label.endswith("s"):
        if label.endswith("y") and len(label) > 2 and label[-2] not in "aeiou":
            variations.append(label[:-1] + "ies")
        elif label.endswith(("s", "x", "z", "ch", "sh", "o")):
            variations.append(label + "es")
        else:
            variations.append(label + "s")

    return variations


def get_labels(uri: str, languages: list[str]) -> dict[str, str]:
    """Get labels for an OFF node URI in the requested languages.

    Args:
        uri:       OFF node URI (e.g. ``"off:en:potatoes"``).
        languages: List of BCP-47 language codes.

    Returns:
        Dict mapping language code to label string.  Empty if the taxonomy is
        unavailable or the node is not found.
    """
    if not uri.startswith("off:"):
        return {}
    node_id = uri[4:]

    taxonomy = _get_taxonomy()
    if taxonomy is None:
        return {}

    try:
        node = taxonomy[node_id]
    except (KeyError, TypeError):
        return {}

    labels: dict[str, str] = {}
    for lang in languages:
        name: str | None = node.names.get(lang)
        if not name and "-" in lang:
            name = node.names.get(lang.split("-")[0])
        if name:
            labels[lang] = name
    return labels


def get_alt_labels(uri: str, languages: list[str]) -> dict[str, list[str]]:
    """Get synonym labels for an OFF node URI in the requested languages.

    Args:
        uri:       OFF node URI (e.g. ``"off:en:potatoes"``).
        languages: List of BCP-47 language codes.

    Returns:
        Dict mapping language code to list of synonym strings.
    """
    if not uri.startswith("off:"):
        return {}
    node_id = uri[4:]

    taxonomy = _get_taxonomy()
    if taxonomy is None:
        return {}

    try:
        node = taxonomy[node_id]
    except (KeyError, TypeError):
        return {}

    alts: dict[str, list[str]] = {}
    for lang in languages:
        syns: list[str] = list(node.synonyms.get(lang, []))
        if not syns and "-" in lang:
            syns = list(node.synonyms.get(lang.split("-")[0], []))
        if syns:
            alts[lang] = syns
    return alts


def lookup_concept(label: str, lang: str = "en", cache_dir: Path | None = None) -> dict[str, Any] | None:
    """Look up a food concept by label in the OFF taxonomy.

    Tries exact match (case-insensitive), then synonyms, then
    singular/plural variations.  When *cache_dir* is provided, results are
    read from and written to the file cache (same format as the SKOS cache).

    Args:
        label:     Human-readable label (e.g. ``"potatoes"``).
        lang:      BCP-47 language code for the returned prefLabel.
        cache_dir: Optional directory for persistent JSON cache.

    Returns:
        Concept dict with ``uri``, ``prefLabel``, ``source``, ``broader``
        keys (compatible with the SKOS lookup format), or ``None`` if not
        found or the ``openfoodfacts`` package is unavailable.
    """
    from tingbok.services.skos import (  # noqa: PLC0415
        _add_to_not_found_cache,
        _get_cache_path,
        _is_in_not_found_cache,
        _load_from_cache,
        _save_to_cache,
    )

    cache_key = f"concept:off:{lang}:{label.lower()}"

    if cache_dir is not None:
        cache_path = _get_cache_path(cache_dir, cache_key)
        cached = _load_from_cache(cache_path)
        if cached is not None and cached.get("uri"):
            return cached
        if _is_in_not_found_cache(cache_dir, cache_key):
            return None

    taxonomy = _get_taxonomy()
    if taxonomy is None:
        return None

    index = _build_label_index(taxonomy)
    label_lower = label.lower().strip()

    node_id = index.get(label_lower)

    if node_id is None:
        for var in _generate_variations(label_lower):
            node_id = index.get(var)
            if node_id is not None:
                break

    if node_id is None:
        if cache_dir is not None:
            _add_to_not_found_cache(cache_dir, cache_key)
        return None

    node = taxonomy[node_id]  # type: ignore[index]
    pref_label: str = node.get_localized_name(lang)
    # get_localized_name falls back to node_id when no label is available
    if pref_label == node_id and lang != "en":
        en_name = node.names.get("en")
        if en_name:
            pref_label = en_name

    broader: list[dict[str, str]] = []
    for parent in node.parents:
        parent_label: str = parent.get_localized_name(lang)
        if parent_label == parent.id and lang != "en":
            en_name = parent.names.get("en")
            if en_name:
                parent_label = en_name
        broader.append({"uri": f"off:{parent.id}", "label": parent_label})

    result: dict[str, Any] = {
        "uri": f"off:{node_id}",
        "prefLabel": pref_label,
        "source": "off",
        "broader": broader,
    }

    if cache_dir is not None:
        _save_to_cache(cache_path, result)

    return result
