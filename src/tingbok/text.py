"""Shared text-normalisation helpers for tingbok.

Owns the single heuristic English singular/plural inflection used across the
service lookups (AGROVOC/SKOS, OFF, GPT) and the local vocabulary.  This logic
lives in tingbok rather than being copied per service.
"""

from __future__ import annotations


def number_variations(label: str) -> list[str]:
    """Return *label* plus heuristic English singular/plural variations.

    Results are lowercased; the lowercased input is always the first element.
    Order is preserved and duplicates removed.  This is a pure heuristic (no
    wordlist) used to widen label lookups against taxonomies and the vocabulary
    — generating spurious-but-harmless variants is preferable to missing a
    legitimate singular/plural match.

    Examples:
        ``juices`` -> ``juice`` (strip "s", not "es"), ``berries`` -> ``berry``,
        ``potatoes`` -> ``potato``, ``tool`` -> ``tools``, ``photo`` -> ``photos``.
        Words ending in ``ss``/``us``/``is`` (``glass``, ``bus``, ``analysis``)
        are left as-is rather than stripped to a bogus singular.
    """
    base = label.lower()
    variations = [base]

    # Plural -> singular
    if base.endswith("ies") and len(base) > 4:
        variations.append(base[:-3] + "y")  # berries -> berry
    elif base.endswith("oes") and len(base) > 4:
        variations.append(base[:-2])  # potatoes -> potato
    elif base.endswith("es") and len(base) > 3:
        stem = base[:-2]
        if stem.endswith(("s", "x", "z", "ch", "sh")):
            variations.append(stem)  # brushes -> brush, glasses -> glass
        else:
            variations.append(base[:-1])  # juices -> juice
    elif base.endswith("s") and not base.endswith(("ss", "us", "is")):
        variations.append(base[:-1])  # tools -> tool

    # Singular -> plural
    if not base.endswith("s"):
        if base.endswith("y") and len(base) > 2 and base[-2] not in "aeiou":
            variations.append(base[:-1] + "ies")  # berry -> berries
        elif base.endswith(("s", "x", "z", "ch", "sh", "o")):
            variations.append(base + "es")  # box -> boxes, potato -> potatoes
        else:
            variations.append(base + "s")  # tool -> tools
        if base.endswith("o"):
            variations.append(base + "s")  # photo -> photos (alongside -oes)

    return list(dict.fromkeys(variations))
