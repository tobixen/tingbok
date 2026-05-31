"""Tests for the shared text-normalisation helpers."""

import pytest

from tingbok.text import number_variations


def test_base_is_lowercased_and_first() -> None:
    result = number_variations("Tools")
    assert result[0] == "tools"
    assert all(v == v.lower() for v in result)


def test_result_is_deduplicated() -> None:
    result = number_variations("juice")
    assert len(result) == len(set(result))


@pytest.mark.parametrize(
    ("word", "expected"),
    [
        # Plural -> singular
        ("juices", "juice"),  # the fruit-juice bug: strip "s", not "es"
        ("fruit juices", "fruit juice"),
        ("berries", "berry"),
        ("potatoes", "potato"),
        ("brushes", "brush"),
        ("boxes", "box"),
        ("tools", "tool"),
        ("glasses", "glass"),
        # Singular -> plural
        ("juice", "juices"),
        ("tool", "tools"),
        ("berry", "berries"),
        ("box", "boxes"),
        ("potato", "potatoes"),
        ("photo", "photos"),
    ],
)
def test_number_variations_contains_expected(word: str, expected: str) -> None:
    assert expected in number_variations(word)


@pytest.mark.parametrize("word", ["glass", "bus", "analysis"])
def test_double_s_and_us_is_not_naively_singularised(word: str) -> None:
    """Words ending in 'ss'/'us'/'is' must not be stripped to a bogus singular."""
    result = number_variations(word)
    assert word[:-1] not in result  # e.g. "glass" must not yield "glas"
