"""Tests for Google Product Taxonomy (GPT) lookup service and download CLI."""

from __future__ import annotations

import sys
from io import StringIO
from pathlib import Path
from unittest.mock import MagicMock, patch

# ---------------------------------------------------------------------------
# Sample GPT file content for tests
# ---------------------------------------------------------------------------

SAMPLE_GPT_EN = """\
# Google_Product_Taxonomy_Version: 2021-09-21
1 - Animals & Pet Supplies
3237 - Animals & Pet Supplies > Live Animals
2 - Animals & Pet Supplies > Pet Supplies
5122 - Hardware > Brackets
632 - Electronics
499 - Electronics > Cameras & Optics
"""

SAMPLE_GPT_NB = """\
# Google_Product_Taxonomy_Version: 2021-09-21
1 - Dyr og kjæledyrartikler
3237 - Dyr og kjæledyrartikler > Levende dyr
2 - Dyr og kjæledyrartikler > Kjæledyrartikler
5122 - Jernvarer > Braketter
632 - Elektronikk
499 - Elektronikk > Kameraer og optikk
"""


def _write_gpt_file(cache_dir: Path, locale: str, content: str) -> Path:
    gpt_dir = cache_dir / "gpt"
    gpt_dir.mkdir(parents=True, exist_ok=True)
    p = gpt_dir / f"taxonomy-with-ids.{locale}.txt"
    p.write_text(content, encoding="utf-8")
    return p


# ---------------------------------------------------------------------------
# Tests for GPT parsing / lookup service
# ---------------------------------------------------------------------------


def test_gpt_lookup_by_label_exact(tmp_path: Path) -> None:
    """Exact label match returns the correct concept with gpt: URI."""
    _write_gpt_file(tmp_path, "en-GB", SAMPLE_GPT_EN)

    from tingbok.services import gpt as gpt_service

    result = gpt_service.lookup_concept("Electronics", "en", tmp_path)
    assert result is not None
    assert result["uri"] == "gpt:632"
    assert result["prefLabel"] == "Electronics"
    assert result["source"] == "gpt"


def test_gpt_lookup_case_insensitive(tmp_path: Path) -> None:
    """Lookup is case-insensitive."""
    _write_gpt_file(tmp_path, "en-GB", SAMPLE_GPT_EN)

    from tingbok.services import gpt as gpt_service

    result = gpt_service.lookup_concept("electronics", "en", tmp_path)
    assert result is not None
    assert result["uri"] == "gpt:632"


def test_gpt_lookup_missing_label_returns_none(tmp_path: Path) -> None:
    """Lookup for unknown label returns None."""
    _write_gpt_file(tmp_path, "en-GB", SAMPLE_GPT_EN)

    from tingbok.services import gpt as gpt_service

    result = gpt_service.lookup_concept("Potatoes", "en", tmp_path)
    assert result is None


def test_gpt_lookup_no_file_returns_none(tmp_path: Path) -> None:
    """Returns None when no GPT file is present."""
    from tingbok.services import gpt as gpt_service

    result = gpt_service.lookup_concept("Electronics", "en", tmp_path)
    assert result is None


def test_gpt_lookup_broader_hierarchy(tmp_path: Path) -> None:
    """Concept with a parent path includes a broader entry."""
    _write_gpt_file(tmp_path, "en-GB", SAMPLE_GPT_EN)

    from tingbok.services import gpt as gpt_service

    result = gpt_service.lookup_concept("Cameras & Optics", "en", tmp_path)
    assert result is not None
    assert result["uri"] == "gpt:499"
    broader = result.get("broader", [])
    assert len(broader) == 1
    assert broader[0]["label"] == "Electronics"
    assert broader[0]["uri"] == "gpt:632"


def test_gpt_lookup_norwegian_locale(tmp_path: Path) -> None:
    """Lookup falls back to nb-NO file when 'nb' language requested."""
    _write_gpt_file(tmp_path, "nb-NO", SAMPLE_GPT_NB)

    from tingbok.services import gpt as gpt_service

    result = gpt_service.lookup_concept("Elektronikk", "nb", tmp_path)
    assert result is not None
    assert result["uri"] == "gpt:632"


def test_gpt_get_labels_from_multiple_locales(tmp_path: Path) -> None:
    """get_labels returns labels from available language files."""
    _write_gpt_file(tmp_path, "en-GB", SAMPLE_GPT_EN)
    _write_gpt_file(tmp_path, "nb-NO", SAMPLE_GPT_NB)

    from tingbok.services import gpt as gpt_service

    labels = gpt_service.get_labels("gpt:632", ["en", "nb"], tmp_path)
    assert labels.get("en") == "Electronics"
    assert labels.get("nb") == "Elektronikk"


def test_gpt_get_labels_unknown_uri_returns_empty(tmp_path: Path) -> None:
    """get_labels for an unknown gpt: URI returns empty dict."""
    _write_gpt_file(tmp_path, "en-GB", SAMPLE_GPT_EN)

    from tingbok.services import gpt as gpt_service

    labels = gpt_service.get_labels("gpt:99999", ["en"], tmp_path)
    assert labels == {}


# ---------------------------------------------------------------------------
# Tests for download-taxonomy CLI subcommand
# ---------------------------------------------------------------------------


def _run_download(tmp_path: Path, extra_args: list[str]) -> tuple[int, str]:
    """Run the download-taxonomy command and capture stdout."""
    import tingbok.cli as cli_module

    cache_dir = tmp_path / "cache"
    argv = ["tingbok", "download-taxonomy", "--cache-dir", str(cache_dir)] + extra_args

    captured = StringIO()
    with patch.object(sys, "argv", argv):
        with patch("sys.stdout", captured):
            try:
                cli_module.main()
                rc = 0
            except SystemExit as exc:
                rc = exc.code if isinstance(exc.code, int) else 0
    return rc, captured.getvalue()


def test_gpt_lookup_singular_finds_plural_label(tmp_path: Path) -> None:
    """Querying a singular form finds the concept when the GPT label is plural."""
    sample = """\
# Google_Product_Taxonomy_Version: 2021-09-21
569 - Home & Garden > Bedding
4951 - Home & Garden > Bedding > Pillows
"""
    _write_gpt_file(tmp_path, "en-GB", sample)

    from tingbok.services import gpt as gpt_service

    result = gpt_service.lookup_concept("pillow", "en", tmp_path)
    assert result is not None
    assert result["uri"] == "gpt:4951"
    assert result["prefLabel"] == "Pillows"


def test_gpt_lookup_plural_finds_singular_label(tmp_path: Path) -> None:
    """Querying a plural form finds the concept when the GPT label is singular."""
    sample = """\
# Google_Product_Taxonomy_Version: 2021-09-21
1 - Electronics
99 - Electronics > Camera
"""
    _write_gpt_file(tmp_path, "en-GB", sample)

    from tingbok.services import gpt as gpt_service

    result = gpt_service.lookup_concept("cameras", "en", tmp_path)
    assert result is not None
    assert result["uri"] == "gpt:99"


def test_gpt_lookup_returns_path_parts(tmp_path: Path) -> None:
    """lookup_concept result includes path_parts for hierarchy derivation."""
    _write_gpt_file(tmp_path, "en-GB", SAMPLE_GPT_EN)

    from tingbok.services import gpt as gpt_service

    result = gpt_service.lookup_concept("Cameras & Optics", "en", tmp_path)
    assert result is not None
    assert result.get("path_parts") == ["Electronics", "Cameras & Optics"]


def test_download_taxonomy_gpt_writes_file(tmp_path: Path) -> None:
    """--gpt should download the en-GB GPT file into cache_dir/gpt/."""
    fake_response = MagicMock()
    fake_response.status_code = 200
    fake_response.iter_content = MagicMock(return_value=[SAMPLE_GPT_EN.encode()])

    with patch("niquests.get", return_value=fake_response):
        rc, output = _run_download(tmp_path, ["--gpt"])

    assert rc == 0
    gpt_file = tmp_path / "cache" / "gpt" / "taxonomy-with-ids.en-GB.txt"
    assert gpt_file.exists()


def test_download_taxonomy_gpt_with_locales(tmp_path: Path) -> None:
    """--gpt with locale list downloads the specified locales."""
    fake_response = MagicMock()
    fake_response.status_code = 200
    fake_response.iter_content = MagicMock(return_value=[SAMPLE_GPT_EN.encode()])

    with patch("niquests.get", return_value=fake_response):
        rc, output = _run_download(tmp_path, ["--gpt", "nb-NO", "sv-SE"])

    assert rc == 0
    assert (tmp_path / "cache" / "gpt" / "taxonomy-with-ids.nb-NO.txt").exists()
    assert (tmp_path / "cache" / "gpt" / "taxonomy-with-ids.sv-SE.txt").exists()


def test_download_taxonomy_agrovoc_writes_file(tmp_path: Path) -> None:
    """--agrovoc should download and extract agrovoc.nt into cache_dir/skos/."""
    import zipfile
    from io import BytesIO

    # Build a fake zip containing agrovoc_lod.nt
    buf = BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("agrovoc_lod.nt", b"<http://example.org/s> <http://example.org/p> <http://example.org/o> .\n")
    zip_bytes = buf.getvalue()

    fake_response = MagicMock()
    fake_response.status_code = 200
    fake_response.iter_content = MagicMock(return_value=[zip_bytes])

    with patch("niquests.get", return_value=fake_response):
        rc, output = _run_download(tmp_path, ["--agrovoc"])

    assert rc == 0
    nt_file = tmp_path / "cache" / "skos" / "agrovoc.nt"
    assert nt_file.exists()


def test_gpt_lookup_by_uri_returns_concept(tmp_path: Path) -> None:
    """lookup_by_uri returns concept dict for a known gpt: URI."""
    _write_gpt_file(tmp_path, "en-GB", SAMPLE_GPT_EN)

    from tingbok.services import gpt as gpt_service

    result = gpt_service.lookup_by_uri("gpt:499", "en", tmp_path)
    assert result is not None
    assert result["uri"] == "gpt:499"
    assert result["prefLabel"] == "Cameras & Optics"
    assert result["source"] == "gpt"
    assert result["path_parts"] == ["Electronics", "Cameras & Optics"]


def test_gpt_lookup_by_uri_unknown_id_returns_none(tmp_path: Path) -> None:
    """lookup_by_uri returns None for an unknown gpt ID."""
    _write_gpt_file(tmp_path, "en-GB", SAMPLE_GPT_EN)

    from tingbok.services import gpt as gpt_service

    result = gpt_service.lookup_by_uri("gpt:99999", "en", tmp_path)
    assert result is None


def test_gpt_lookup_by_uri_non_gpt_uri_returns_none(tmp_path: Path) -> None:
    """lookup_by_uri returns None for URIs that are not gpt: scheme."""
    _write_gpt_file(tmp_path, "en-GB", SAMPLE_GPT_EN)

    from tingbok.services import gpt as gpt_service

    result = gpt_service.lookup_by_uri("http://example.org/something", "en", tmp_path)
    assert result is None


def test_gpt_lookup_by_uri_root_has_no_broader(tmp_path: Path) -> None:
    """lookup_by_uri for a root category returns broader=None."""
    _write_gpt_file(tmp_path, "en-GB", SAMPLE_GPT_EN)

    from tingbok.services import gpt as gpt_service

    result = gpt_service.lookup_by_uri("gpt:632", "en", tmp_path)
    assert result is not None
    assert result["broader"] is None
    assert result["path_parts"] == ["Electronics"]


def test_download_taxonomy_no_flags_shows_help(tmp_path: Path) -> None:
    """Running download-taxonomy without --gpt or --agrovoc should print usage hint."""
    rc, output = _run_download(tmp_path, [])
    # Should exit cleanly but indicate nothing was selected
    assert rc == 0
    assert "gpt" in output.lower() or "agrovoc" in output.lower()
