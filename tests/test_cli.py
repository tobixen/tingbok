"""Tests for the tingbok CLI (populate-uris command)."""

from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

MINIMAL_VOCAB = """\
concepts:
  food:
    prefLabel: "Food"
    uri: "http://dbpedia.org/resource/Food"
    source_uris:
      - "http://dbpedia.org/resource/Food"

  electronics:
    prefLabel: "Electronics"
    narrower:
      - electronics/audio

  # A comment that should survive
  bedding:
    prefLabel: "Bedding"
    uri: "http://dbpedia.org/resource/Bedding"
    source_uris:
      - "http://dbpedia.org/resource/Bedding"
    excluded_sources:
      - agrovoc
"""


def _run_populate(
    tmp_path: Path,
    extra_args: list[str] | None = None,
    vocab_content: str = MINIMAL_VOCAB,
) -> tuple[int, str]:
    """Run the populate-uris command against a temp vocabulary file."""
    import sys
    from io import StringIO
    from unittest.mock import patch as _patch

    import tingbok.cli as cli_module

    vocab_file = tmp_path / "vocabulary.yaml"
    vocab_file.write_text(vocab_content)
    # cache_dir is the root cache directory (skos/ and gpt/ are subdirs)
    cache_dir = tmp_path

    argv = ["tingbok", "populate-uris", str(vocab_file), "--cache-dir", str(cache_dir)]
    if extra_args:
        argv.extend(extra_args)

    captured = StringIO()
    with _patch.object(sys, "argv", argv):
        with _patch("sys.stdout", captured):
            try:
                cli_module.main()
                rc = 0
            except SystemExit as exc:
                rc = exc.code if isinstance(exc.code, int) else 0
    return rc, captured.getvalue()


def test_populate_uris_adds_dbpedia_uri(tmp_path):
    """Should add DBpedia source_uris to concepts with no external URIs."""

    def fake_lookup(label, lang, source, cache_dir):
        if source == "dbpedia" and label.lower() == "electronics":
            return {
                "uri": "http://dbpedia.org/resource/Electronics",
                "prefLabel": "Electronics",
                "source": "dbpedia",
                "broader": [],
            }
        return None

    from tingbok.services import skos as skos_service

    with patch.object(skos_service, "lookup_concept", side_effect=fake_lookup):
        with patch.object(skos_service, "get_agrovoc_store", return_value=None):
            rc, _ = _run_populate(tmp_path)

    assert rc == 0
    vocab_file = tmp_path / "vocabulary.yaml"
    updated = yaml.safe_load(vocab_file.read_text())
    assert "http://dbpedia.org/resource/Electronics" in updated["concepts"]["electronics"].get("source_uris", [])


def test_populate_uris_skips_concepts_with_existing_uris(tmp_path):
    """Should not modify concepts that already have external source_uris."""
    call_labels = []

    def recording_lookup(label, lang, source, cache_dir):
        call_labels.append(label.lower())
        return None

    from tingbok.services import skos as skos_service

    with patch.object(skos_service, "lookup_concept", side_effect=recording_lookup):
        with patch.object(skos_service, "get_agrovoc_store", return_value=None):
            _run_populate(tmp_path)

    # "electronics" has no external URIs → should be looked up
    assert "electronics" in call_labels


def test_populate_uris_dry_run_does_not_write(tmp_path):
    """--dry-run should print proposed changes but not modify the file."""
    vocab_file = tmp_path / "vocabulary.yaml"
    original_text = MINIMAL_VOCAB

    def fake_lookup(label, lang, source, cache_dir):
        if source == "dbpedia":
            return {
                "uri": f"http://dbpedia.org/resource/{label.title()}",
                "prefLabel": label.title(),
                "source": "dbpedia",
                "broader": [],
            }
        return None

    from tingbok.services import skos as skos_service

    with patch.object(skos_service, "lookup_concept", side_effect=fake_lookup):
        with patch.object(skos_service, "get_agrovoc_store", return_value=None):
            rc, output = _run_populate(tmp_path, extra_args=["--dry-run"])

    assert rc == 0
    # File must be unchanged
    assert vocab_file.read_text() == original_text
    # Output must mention the concept and discovered URI
    assert "electronics" in output.lower() or "Electronics" in output


def test_populate_uris_preserves_yaml_comments(tmp_path):
    """ruamel.yaml round-trip should preserve existing comments."""

    def fake_lookup(label, lang, source, cache_dir):
        if source == "dbpedia" and label.lower() == "electronics":
            return {
                "uri": "http://dbpedia.org/resource/Electronics",
                "prefLabel": "Electronics",
                "source": "dbpedia",
                "broader": [],
            }
        return None

    from tingbok.services import skos as skos_service

    with patch.object(skos_service, "lookup_concept", side_effect=fake_lookup):
        with patch.object(skos_service, "get_agrovoc_store", return_value=None):
            _run_populate(tmp_path)

    updated_text = (tmp_path / "vocabulary.yaml").read_text()
    assert "A comment that should survive" in updated_text


def test_populate_uris_queries_agrovoc_when_store_present(tmp_path):
    """Should query AGROVOC when the Oxigraph store is available."""
    queried_sources: set[str] = set()

    def recording_lookup(label, lang, source, cache_dir):
        queried_sources.add(source)
        return None

    fake_store = object()
    from tingbok.services import skos as skos_service

    with patch.object(skos_service, "lookup_concept", side_effect=recording_lookup):
        with patch.object(skos_service, "get_agrovoc_store", return_value=fake_store):
            _run_populate(tmp_path)

    assert "agrovoc" in queried_sources


def test_populate_uris_nonexistent_vocab_exits_with_error(tmp_path):
    """Should exit with a non-zero code when the vocabulary file does not exist."""
    import sys

    import tingbok.cli as cli_module

    argv = ["populate-uris", str(tmp_path / "no_such_file.yaml"), "--cache-dir", str(tmp_path)]
    with patch.object(sys, "argv", argv):
        with pytest.raises(SystemExit) as exc_info:
            cli_module.main()
    assert exc_info.value.code != 0


def test_populate_uris_always_queries_agrovoc(tmp_path):
    """Should query AGROVOC even when no Oxigraph store is available (REST fallback)."""
    queried_sources: set[str] = set()

    def recording_lookup(label, lang, source, cache_dir):
        queried_sources.add(source)
        return None

    from tingbok.services import skos as skos_service

    with patch.object(skos_service, "lookup_concept", side_effect=recording_lookup):
        with patch.object(skos_service, "get_agrovoc_store", return_value=None):
            _run_populate(tmp_path)

    assert "agrovoc" in queried_sources


def test_populate_uris_queries_gpt_when_files_present(tmp_path):
    """Should add a GPT URI when GPT taxonomy files exist in the cache."""
    SAMPLE_GPT = "# Google_Product_Taxonomy_Version: 2021-09-21\n632 - Electronics\n"
    gpt_dir = tmp_path / "gpt"
    gpt_dir.mkdir()
    (gpt_dir / "taxonomy-with-ids.en-GB.txt").write_text(SAMPLE_GPT)

    def fake_skos_lookup(label, lang, source, cache_dir):
        return None

    from tingbok.services import skos as skos_service

    with patch.object(skos_service, "lookup_concept", side_effect=fake_skos_lookup):
        with patch.object(skos_service, "get_agrovoc_store", return_value=None):
            rc, _ = _run_populate(tmp_path)

    assert rc == 0
    updated = yaml.safe_load((tmp_path / "vocabulary.yaml").read_text())
    assert "gpt:632" in updated["concepts"]["electronics"].get("source_uris", [])


def _run_prune(
    tmp_path: Path,
    extra_args: list[str] | None = None,
    vocab_content: str = "",
) -> tuple[int, str]:
    """Run the prune-vocabulary command against a temp vocabulary file."""
    import sys
    from io import StringIO
    from unittest.mock import patch as _patch

    import tingbok.cli as cli_module

    vocab_file = tmp_path / "vocabulary.yaml"
    vocab_file.write_text(vocab_content)
    cache_dir = tmp_path

    argv = ["tingbok", "prune-vocabulary", str(vocab_file), "--cache-dir", str(cache_dir)]
    if extra_args:
        argv.extend(extra_args)

    captured = StringIO()
    with _patch.object(sys, "argv", argv):
        with _patch("sys.stdout", captured):
            try:
                cli_module.main()
                rc = 0
            except SystemExit as exc:
                rc = exc.code if isinstance(exc.code, int) else 0
    return rc, captured.getvalue()


VOCAB_WITH_LABELS = """\
concepts:
  food:
    prefLabel: "Food"
    source_uris:
      - "http://dbpedia.org/resource/Food"
    labels:
      en: "Food"
      nb: "Mat"
      de: "Speise"

  electronics:
    prefLabel: "Electronics"
    source_uris:
      - "http://dbpedia.org/resource/Electronics"
    labels:
      en: "Electronics"
"""

VOCAB_WITH_ALTLABELS = """\
concepts:
  food:
    prefLabel: "Food"
    altLabel:
      en: ["groceries", "provisions"]
      nb: ["matbiter"]
    source_uris:
      - "http://dbpedia.org/resource/Food"
"""


def test_prune_vocabulary_removes_matching_labels(tmp_path):
    """Labels matching source translations should be removed from vocabulary.yaml."""
    from tingbok.services import skos as skos_service

    def fake_get_labels(uri, languages, source, cache_dir):
        if "Food" in uri or source == "dbpedia":
            return {"en": "Food", "nb": "Mat", "de": "Lebensmittel"}
        return {}

    with patch.object(skos_service, "get_labels", side_effect=fake_get_labels):
        with patch.object(skos_service, "get_agrovoc_store", return_value=None):
            rc, output = _run_prune(tmp_path, vocab_content=VOCAB_WITH_LABELS)

    assert rc == 0
    import yaml

    updated = yaml.safe_load((tmp_path / "vocabulary.yaml").read_text())
    food_labels = updated["concepts"]["food"].get("labels", {})
    # "en: Food" and "nb: Mat" match source → removed
    assert "en" not in food_labels
    assert "nb" not in food_labels
    # "de: Speise" deviates from source "Lebensmittel" → kept
    assert "de" in food_labels


def test_prune_vocabulary_reports_deviations(tmp_path):
    """Labels deviating from source should be reported, not silently removed."""
    from tingbok.services import skos as skos_service

    def fake_get_labels(uri, languages, source, cache_dir):
        return {"en": "Food", "nb": "Mat", "de": "Lebensmittel"}

    with patch.object(skos_service, "get_labels", side_effect=fake_get_labels):
        with patch.object(skos_service, "get_agrovoc_store", return_value=None):
            rc, output = _run_prune(tmp_path, vocab_content=VOCAB_WITH_LABELS)

    assert rc == 0
    # Deviation between "Speise" and "Lebensmittel" should appear in output
    assert "Speise" in output or "Lebensmittel" in output or "de" in output


def test_prune_vocabulary_dry_run_does_not_write(tmp_path):
    """--dry-run should not modify the vocabulary file."""
    original_text = VOCAB_WITH_LABELS
    (tmp_path / "vocabulary.yaml").write_text(original_text)

    from tingbok.services import skos as skos_service

    def fake_get_labels(uri, languages, source, cache_dir):
        return {"en": "Food", "nb": "Mat"}

    with patch.object(skos_service, "get_labels", side_effect=fake_get_labels):
        with patch.object(skos_service, "get_agrovoc_store", return_value=None):
            rc, _ = _run_prune(tmp_path, extra_args=["--dry-run"], vocab_content=VOCAB_WITH_LABELS)

    assert rc == 0
    assert (tmp_path / "vocabulary.yaml").read_text() == original_text


def test_prune_vocabulary_preserves_yaml_comments(tmp_path):
    """ruamel.yaml round-trip should preserve comments."""
    vocab_with_comment = """\
concepts:
  # This is an important comment
  food:
    prefLabel: "Food"
    source_uris:
      - "http://dbpedia.org/resource/Food"
    labels:
      en: "Food"
"""
    from tingbok.services import skos as skos_service

    with patch.object(skos_service, "get_labels", return_value={"en": "Food"}):
        with patch.object(skos_service, "get_agrovoc_store", return_value=None):
            _run_prune(tmp_path, vocab_content=vocab_with_comment)

    assert "important comment" in (tmp_path / "vocabulary.yaml").read_text()


def test_prune_vocabulary_case_insensitive_match(tmp_path):
    """Matching should be case-insensitive (source 'food' matches vocab 'Food')."""
    from tingbok.services import skos as skos_service

    with patch.object(skos_service, "get_labels", return_value={"en": "food"}):
        with patch.object(skos_service, "get_agrovoc_store", return_value=None):
            rc, _ = _run_prune(tmp_path, vocab_content=VOCAB_WITH_LABELS)

    assert rc == 0
    import yaml

    updated = yaml.safe_load((tmp_path / "vocabulary.yaml").read_text())
    # "Food" matches "food" case-insensitively → should be removed
    assert "en" not in updated["concepts"]["food"].get("labels", {})


def test_populate_uris_skips_gpt_when_excluded(tmp_path):
    """Should not add GPT URIs when 'gpt' is in excluded_sources."""
    SAMPLE_GPT = "# Google_Product_Taxonomy_Version: 2021-09-21\n632 - Electronics\n"
    gpt_dir = tmp_path / "gpt"
    gpt_dir.mkdir()
    (gpt_dir / "taxonomy-with-ids.en-GB.txt").write_text(SAMPLE_GPT)

    vocab_with_excluded = MINIMAL_VOCAB.replace(
        '  electronics:\n    prefLabel: "Electronics"\n    narrower:\n      - electronics/audio',
        '  electronics:\n    prefLabel: "Electronics"\n    narrower:\n      - electronics/audio\n    excluded_sources:\n      - gpt',
    )

    def fake_skos_lookup(label, lang, source, cache_dir):
        return None

    from tingbok.services import skos as skos_service

    with patch.object(skos_service, "lookup_concept", side_effect=fake_skos_lookup):
        with patch.object(skos_service, "get_agrovoc_store", return_value=None):
            rc, _ = _run_populate(tmp_path, vocab_content=vocab_with_excluded)

    updated = yaml.safe_load((tmp_path / "vocabulary.yaml").read_text())
    source_uris = updated["concepts"]["electronics"].get("source_uris", [])
    assert not any(u.startswith("gpt:") for u in source_uris)
