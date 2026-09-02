"""Shape fixes applied to a vocabulary.yaml document however it was read.

``app._load_vocabulary`` reads the file with PyYAML, while the paths that
*write* it use ruamel to preserve comments and formatting.  A normalisation
that lives in only one of those is not a normalisation: the loader would hide a
bad shape on read while the writer corrupted it on disk.
"""

from __future__ import annotations

from typing import Any


def coerce_scalar_source_uris(concepts: dict[str, Any]) -> None:
    """Turn a lone ``source_uris`` string into a one-item list, in place.

    YAML makes ``source_uris: https://…`` a string rather than a list, and a
    string is iterable — so every consumer sees one entry per character.  Read,
    that serves 37 bogus source URIs for a concept; written, it rewrites the
    file with 37 single-character list entries, or raises on append.
    """
    for entry in concepts.values():
        if entry is not None and isinstance(entry.get("source_uris"), str):
            entry["source_uris"] = [entry["source_uris"]]
