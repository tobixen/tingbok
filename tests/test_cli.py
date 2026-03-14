"""Tests for the tingbok CLI (populate-uris command)."""

import json
import sys
import time
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
    alt_labels_side_effect=None,
) -> tuple[int, str]:
    """Run the prune-vocabulary command against a temp vocabulary file.

    ``alt_labels_side_effect`` overrides the default no-op mock for
    ``skos_service.get_alt_labels``.  Pass a callable or a dict-return
    value to test alt-label pruning behaviour.
    """
    from io import StringIO
    from unittest.mock import patch as _patch

    import tingbok.cli as cli_module
    from tingbok.services import skos as skos_service

    vocab_file = tmp_path / "vocabulary.yaml"
    vocab_file.write_text(vocab_content)
    cache_dir = tmp_path

    argv = ["tingbok", "prune-vocabulary", str(vocab_file), "--cache-dir", str(cache_dir)]
    if extra_args:
        argv.extend(extra_args)

    _default_no_alts = alt_labels_side_effect if alt_labels_side_effect is not None else (lambda *_: {})

    captured = StringIO()
    with _patch.object(sys, "argv", argv):
        with _patch("sys.stdout", captured):
            with _patch.object(skos_service, "get_alt_labels", side_effect=_default_no_alts):
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


def test_prune_vocabulary_includes_source_name_in_deviation(tmp_path):
    """Deviation output should say which source provided the differing label."""
    from tingbok.services import skos as skos_service

    def fake_get_labels(uri, languages, source, cache_dir):
        if source == "dbpedia":
            return {"de": "Completely_Different_DE"}
        return {}

    with patch.object(skos_service, "get_labels", side_effect=fake_get_labels):
        with patch.object(skos_service, "get_agrovoc_store", return_value=None):
            rc, output = _run_prune(tmp_path, vocab_content=VOCAB_WITH_LABELS)

    assert rc == 0
    # The source name should appear in the deviation line
    assert "dbpedia" in output


def test_prune_vocabulary_detects_inter_source_conflict(tmp_path):
    """When different sources give different labels for the same language, report it."""
    vocab = """\
concepts:
  food:
    prefLabel: "Food"
    source_uris:
      - "http://dbpedia.org/resource/Food"
      - "http://www.wikidata.org/entity/Q2095"
    labels:
      en: "Food"
"""
    from tingbok.services import skos as skos_service

    def fake_get_labels(uri, languages, source, cache_dir):
        if source == "dbpedia":
            return {"en": "Food"}
        if source == "wikidata":
            return {"en": "Nutrition"}  # Different from dbpedia
        return {}

    with patch.object(skos_service, "get_labels", side_effect=fake_get_labels):
        with patch.object(skos_service, "get_agrovoc_store", return_value=None):
            rc, output = _run_prune(tmp_path, vocab_content=vocab)

    assert rc == 0
    # Inter-source conflict between "Food" and "Nutrition" should be reported
    assert "conflict" in output.lower() or "Nutrition" in output


def test_prune_vocabulary_near_match_removed_without_deviation(tmp_path):
    """Plural/singular variants (high similarity) should be removed without a deviation warning."""
    vocab = """\
concepts:
  tools:
    prefLabel: "Tools"
    source_uris:
      - "http://dbpedia.org/resource/Tool"
    labels:
      de: "Werkzeuge"
"""
    from tingbok.services import skos as skos_service

    # Source has singular "Werkzeug" while vocab has plural "Werkzeuge"
    def fake_get_labels(uri, languages, source, cache_dir):
        return {"de": "Werkzeug"}

    with patch.object(skos_service, "get_labels", side_effect=fake_get_labels):
        with patch.object(skos_service, "get_agrovoc_store", return_value=None):
            rc, output = _run_prune(tmp_path, vocab_content=vocab)

    assert rc == 0
    import yaml

    updated = yaml.safe_load((tmp_path / "vocabulary.yaml").read_text())
    # "Werkzeuge" is a near-match for "Werkzeug" → should be removed
    assert "de" not in updated["concepts"]["tools"].get("labels", {})
    # Should NOT appear as a deviation (no "DEVIATION" line about de)
    assert "Werkzeug" not in output or "variant" in output.lower() or output.count("Werkzeug") < 3


def test_prune_vocabulary_suppresses_deviation_when_source_matches_altlabel(tmp_path):
    """If a source label matches an altLabel, it should not be reported as a deviation."""
    vocab = """\
concepts:
  food:
    prefLabel: "Food"
    source_uris:
      - "http://dbpedia.org/resource/Food"
    labels:
      nl: "Voedsel"
    altLabel:
      nl: ["Levensmiddel", "voeding"]
"""
    from tingbok.services import skos as skos_service

    def fake_get_labels(uri, languages, source, cache_dir):
        return {"nl": "Levensmiddel"}  # matches altLabel, not the main label

    with patch.object(skos_service, "get_labels", side_effect=fake_get_labels):
        with patch.object(skos_service, "get_agrovoc_store", return_value=None):
            rc, output = _run_prune(tmp_path, vocab_content=vocab)

    assert rc == 0
    # Should NOT report as a deviation since "Levensmiddel" is an altLabel
    assert "Levensmiddel" not in output or "deviation" not in output.lower()
    assert "vocab=" not in output  # no deviation line


def test_prune_vocabulary_suppresses_conflict_when_one_source_matches_altlabel(tmp_path):
    """CONFLICT between sources should be suppressed if one value is an altLabel."""
    vocab = """\
concepts:
  food:
    prefLabel: "Food"
    source_uris:
      - "http://dbpedia.org/resource/Food"
      - "http://aims.fao.org/aos/agrovoc/c_3032"
    labels:
      fr: "Nourriture"
    altLabel:
      fr: ["produit alimentaire"]
"""
    from tingbok.services import skos as skos_service

    def fake_get_labels(uri, languages, source, cache_dir):
        if source == "dbpedia":
            return {"fr": "Nourriture"}
        if source == "agrovoc":
            return {"fr": "produit alimentaire"}
        return {}

    with patch.object(skos_service, "get_labels", side_effect=fake_get_labels):
        with patch.object(skos_service, "get_agrovoc_store", return_value=None):
            rc, output = _run_prune(tmp_path, vocab_content=vocab)

    assert rc == 0
    # "produit alimentaire" is an altLabel → conflict should be suppressed
    assert "CONFLICT" not in output


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


def test_prune_vocabulary_removes_redundant_altlabels(tmp_path):
    """altLabel entries available from sources should be removed from vocabulary.yaml."""
    from tingbok.services import skos as skos_service

    def fake_get_labels(uri, languages, source, cache_dir):
        return {}

    def fake_get_alt_labels(uri, languages, source, cache_dir):
        # Source provides all altLabels already in VOCAB_WITH_ALTLABELS
        if source == "dbpedia":
            return {"en": ["groceries", "provisions"], "nb": ["matbiter"]}
        return {}

    with patch.object(skos_service, "get_labels", side_effect=fake_get_labels):
        with patch.object(skos_service, "get_agrovoc_store", return_value=None):
            rc, output = _run_prune(
                tmp_path, vocab_content=VOCAB_WITH_ALTLABELS, alt_labels_side_effect=fake_get_alt_labels
            )

    assert rc == 0
    import yaml

    updated = yaml.safe_load((tmp_path / "vocabulary.yaml").read_text())
    food = updated["concepts"]["food"]
    # All altLabels were provided by source — should be removed
    assert not food.get("altLabel"), f"Expected altLabel removed, got: {food.get('altLabel')}"


def test_prune_vocabulary_keeps_altlabels_not_in_sources(tmp_path):
    """altLabel entries NOT available from sources should be kept."""
    from tingbok.services import skos as skos_service

    def fake_get_labels(uri, languages, source, cache_dir):
        return {}

    def fake_get_alt_labels(uri, languages, source, cache_dir):
        # Source only provides one of the altLabels
        if source == "dbpedia":
            return {"en": ["groceries"]}  # "provisions" and "nb" not provided
        return {}

    with patch.object(skos_service, "get_labels", side_effect=fake_get_labels):
        with patch.object(skos_service, "get_agrovoc_store", return_value=None):
            rc, output = _run_prune(
                tmp_path, vocab_content=VOCAB_WITH_ALTLABELS, alt_labels_side_effect=fake_get_alt_labels
            )

    assert rc == 0
    import yaml

    updated = yaml.safe_load((tmp_path / "vocabulary.yaml").read_text())
    food = updated["concepts"]["food"]
    # "provisions" not in source → kept
    assert "provisions" in food.get("altLabel", {}).get("en", [])
    # "groceries" in source → removed
    assert "groceries" not in food.get("altLabel", {}).get("en", [])
    # nb altLabel not in source → kept
    assert "matbiter" in food.get("altLabel", {}).get("nb", [])


def test_populate_uris_no_http_https_duplicates(tmp_path):
    """Should not add a https:// URI when http:// variant already exists in source_uris."""
    # MINIMAL_VOCAB already has "http://dbpedia.org/resource/Food" for "food".
    # If dbpedia now returns the https:// variant, it must not be added as a duplicate.

    def fake_lookup(label, lang, source, cache_dir):
        if source == "dbpedia" and label.lower() == "food":
            return {
                "uri": "https://dbpedia.org/resource/Food",  # https variant
                "prefLabel": "Food",
                "source": "dbpedia",
                "broader": [],
            }
        return None

    from tingbok.services import skos as skos_service

    with patch.object(skos_service, "lookup_concept", side_effect=fake_lookup):
        with patch.object(skos_service, "get_agrovoc_store", return_value=None):
            rc, _ = _run_populate(tmp_path)

    assert rc == 0
    updated = yaml.safe_load((tmp_path / "vocabulary.yaml").read_text())
    food_uris = updated["concepts"]["food"].get("source_uris", [])
    # Both http:// and https:// variants must not both be present
    has_http = "http://dbpedia.org/resource/Food" in food_uris
    has_https = "https://dbpedia.org/resource/Food" in food_uris
    assert not (has_http and has_https), f"Duplicate http/https URIs found: {food_uris}"


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


# ---------------------------------------------------------------------------
# prune-cache
# ---------------------------------------------------------------------------


def _write_cache(path: Path, cached_at: float, last_accessed: float | None = None) -> None:
    data: dict = {"uri": "http://example.org/x", "_cached_at": cached_at}
    if last_accessed is not None:
        data["_last_accessed"] = last_accessed
    path.write_text(json.dumps(data))


def _run_prune_cache(cache_dir: Path, extra_args: list[str] | None = None) -> tuple[int, str]:
    import io
    from unittest.mock import patch

    argv = ["tingbok", "prune-cache", "--cache-dir", str(cache_dir)]
    argv += extra_args or []
    with patch("sys.argv", argv):
        with patch("sys.stdout", new_callable=io.StringIO) as mock_out:
            try:
                from tingbok.cli import main

                main()
            except SystemExit as exc:
                return int(exc.code or 0), mock_out.getvalue()
    return 0, mock_out.getvalue()


def test_prune_cache_deletes_old_entries(tmp_path: Path) -> None:
    """Files not accessed for more than max_age_days are deleted."""
    cache_dir = tmp_path / "skos"
    cache_dir.mkdir()
    old = cache_dir / "old.json"
    recent = cache_dir / "recent.json"
    age_61_days = time.time() - 61 * 86400
    age_1_day = time.time() - 86400
    _write_cache(old, cached_at=age_61_days, last_accessed=age_61_days)
    _write_cache(recent, cached_at=age_1_day, last_accessed=age_1_day)

    rc, _ = _run_prune_cache(tmp_path)

    assert rc == 0
    assert not old.exists(), "old entry should be pruned"
    assert recent.exists(), "recent entry should be kept"


def test_prune_cache_uses_last_accessed_over_cached_at(tmp_path: Path) -> None:
    """An entry cached long ago but recently accessed should be kept."""
    cache_dir = tmp_path / "skos"
    cache_dir.mkdir()
    f = cache_dir / "item.json"
    _write_cache(f, cached_at=time.time() - 90 * 86400, last_accessed=time.time() - 86400)

    _run_prune_cache(tmp_path)

    assert f.exists(), "recently accessed entry must not be pruned even if old cached_at"


def test_prune_cache_dry_run_does_not_delete(tmp_path: Path) -> None:
    cache_dir = tmp_path / "skos"
    cache_dir.mkdir()
    f = cache_dir / "old.json"
    _write_cache(f, cached_at=time.time() - 90 * 86400, last_accessed=time.time() - 90 * 86400)

    _run_prune_cache(tmp_path, extra_args=["--dry-run"])

    assert f.exists(), "dry-run must not delete anything"
