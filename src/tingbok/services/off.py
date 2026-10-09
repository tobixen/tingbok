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
import threading
from collections.abc import Callable
from pathlib import Path
from typing import Any

from tingbok.text import number_variations

logger = logging.getLogger(__name__)

#: Module-level cached taxonomy (None = not loaded yet).
_taxonomy: object | None = None

#: Module-level cached label index: {label_lower: node_id}.
_label_index: dict[str, str] | None = None

#: Serialises the one-time loads above.  Resolve looks labels up in parallel
#: threads, and without it a cold process would parse (or download) the
#: taxonomy once per label.
_load_lock = threading.RLock()


def _get_taxonomy(allow_download: bool = True) -> object | None:
    """Lazily load the OFF category taxonomy.

    ``openfoodfacts``' ``get_taxonomy()`` downloads the taxonomy file when it
    is not cached yet.  With *allow_download* false that is never triggered:
    the taxonomy is only loaded when the file is already on disk.

    Returns the taxonomy object, or ``None`` if the ``openfoodfacts``
    package is not installed (or the file is absent and may not be fetched).
    """
    global _taxonomy  # noqa: PLW0603

    if _taxonomy is not None:
        return _taxonomy

    try:
        from openfoodfacts.taxonomy import DEFAULT_CACHE_DIR, get_taxonomy  # noqa: PLC0415
    except ImportError:
        logger.debug(
            "openfoodfacts package not installed; OFF lookup unavailable.  Install with: pip install tingbok[off]"
        )
        return None

    with _load_lock:
        if _taxonomy is not None:
            return _taxonomy

        if not allow_download and not (Path(DEFAULT_CACHE_DIR) / "category.json").exists():
            logger.debug("OFF taxonomy not cached locally and downloading is not allowed")
            return None

        try:
            logger.info("Loading Open Food Facts category taxonomy...")
            _taxonomy = get_taxonomy("category")
            logger.info("OFF taxonomy loaded: %d categories", len(_taxonomy))  # type: ignore[arg-type]
            return _taxonomy
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

    with _load_lock:
        if _label_index is None:
            _label_index = _index_labels(taxonomy)
    return _label_index


def _index_labels(taxonomy: object) -> dict[str, str]:
    """Map every name and synonym (lower-cased, all languages) to its node id."""
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
    return index


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


def _localized_name(node: Any, lang: str) -> str:
    """Name of *node* in *lang*, falling back to English, then to the node id."""
    name: str = node.get_localized_name(lang)
    # get_localized_name falls back to node.id when no label is available
    if name == node.id and lang != "en":
        name = node.names.get("en") or name
    return name


def lookup_concept(
    label: str, lang: str = "en", cache_dir: Path | None = None, allow_download: bool = True
) -> dict[str, Any] | None:
    """Look up a food concept by label in the OFF taxonomy.

    Tries exact match (case-insensitive), then synonyms, then
    singular/plural variations.  When *cache_dir* is provided, results are
    read from and written to the file cache (same format as the SKOS cache).

    Args:
        label:     Human-readable label (e.g. ``"potatoes"``).
        lang:      BCP-47 language code for the returned prefLabel.
        cache_dir: Optional directory for persistent JSON cache.
        allow_download: Whether loading the taxonomy may download it.

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

    taxonomy = _get_taxonomy(allow_download=allow_download)
    if taxonomy is None:
        return None

    index = _build_label_index(taxonomy)
    label_lower = label.lower().strip()

    node_id = index.get(label_lower)

    if node_id is None:
        for var in number_variations(label_lower):
            node_id = index.get(var)
            if node_id is not None:
                break

    if node_id is None:
        if cache_dir is not None:
            _add_to_not_found_cache(cache_dir, cache_key)
        return None

    node = taxonomy[node_id]  # type: ignore[index]
    pref_label = _localized_name(node, lang)
    broader = [{"uri": f"off:{parent.id}", "label": _localized_name(parent, lang)} for parent in node.parents]

    result: dict[str, Any] = {
        "uri": f"off:{node_id}",
        "prefLabel": pref_label,
        "source": "off",
        "broader": broader,
    }

    if cache_dir is not None:
        _save_to_cache(cache_path, result)

    return result


def broader_graph(
    uri: str,
    is_known: Callable[[str], bool],
    lang: str = "en",
    allow_download: bool = True,
    max_depth: int = 12,
) -> dict[str, dict[str, Any]]:
    """Walk the OFF parents of *uri* upwards until reaching concepts the caller knows.

    OFF's taxonomy is a DAG with many parents per node; most top-level nodes
    ("plant-based foods", "canned foods") have no counterpart in a tidy
    category tree.  This keeps only the part of the ancestry that leads into
    something the caller already has: every walk stops at the first node for
    which ``is_known(node_uri)`` is true, and branches that never meet such a
    node (within *max_depth* steps) are dropped.

    Args:
        uri:            Start node URI (e.g. ``"off:en:peeled-tomatoes"``).
                        The start node itself is never tested with *is_known*.
        is_known:       Predicate on ``off:`` URIs marking the bridge nodes.
        lang:           Language for the returned labels.
        allow_download: Whether loading the taxonomy may download it.
        max_depth:      Maximum number of parent steps to follow.

    Returns:
        ``{node_uri: {"label": str, "broader": [parent_uri, ...]}}`` covering
        the start node, intermediate nodes and the known nodes reached (with an
        empty ``broader``, since they are not walked further).  Empty when the
        taxonomy is unavailable, the node is not found, or no branch reaches a
        known node.
    """
    if not uri.startswith("off:"):
        return {}
    taxonomy = _get_taxonomy(allow_download=allow_download)
    if taxonomy is None:
        return {}
    try:
        start = taxonomy[uri[4:]]  # type: ignore[index]
    except (KeyError, TypeError):
        return {}

    graph: dict[str, dict[str, Any]] = {}
    # Memo: node uri -> whether some branch from it reaches a known node.
    reaches: dict[str, bool] = {}

    def _walk(node: Any, depth: int, is_start: bool) -> bool:
        node_uri = f"off:{node.id}"
        if node_uri in reaches:
            return reaches[node_uri]
        reaches[node_uri] = False  # guards against cycles while walking
        if not is_start and is_known(node_uri):
            graph[node_uri] = {"label": _localized_name(node, lang), "broader": []}
            reaches[node_uri] = True
            return True
        parents: list[str] = []
        if depth < max_depth:
            for parent in node.parents:
                if _walk(parent, depth + 1, False):
                    parents.append(f"off:{parent.id}")
        if parents:
            graph[node_uri] = {"label": _localized_name(node, lang), "broader": parents}
            reaches[node_uri] = True
        return reaches[node_uri]

    if not _walk(start, 0, True):
        return {}
    return graph
