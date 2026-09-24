"""/health reports whether the service can write its own data.

On 2026-08-07 a git reset run as root in the server checkout left
``ean-db.json`` owned by root.  The service writes that file in place, so
every ``PUT /api/ean`` failed with a 500 for weeks while /health kept saying
"ok".  These tests pin the check that would have shown it.
"""

import os
from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

import tingbok.app as app_module
from tingbok.app import app

pytestmark = pytest.mark.skipif(os.geteuid() == 0, reason="root ignores file modes")


@pytest.fixture
def ean_db(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Point the app at a throwaway ean-db.json."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    path = data_dir / "ean-db.json"
    path.write_text("{}", encoding="utf-8")
    vocab = data_dir / "vocabulary.yaml"
    vocab.write_text("concepts: {}\n", encoding="utf-8")
    monkeypatch.setattr(app_module, "EAN_OBSERVATIONS_PATH", path)
    monkeypatch.setattr(app_module, "VOCABULARY_PATH", vocab)
    monkeypatch.setattr(app_module, "_UPDATE_STATUS_FILE", None)
    yield path
    data_dir.chmod(0o755)
    for f in (path, vocab):
        if f.exists():
            f.chmod(0o644)


async def _health(client: tuple[str, int] = ("127.0.0.1", 12345)) -> dict:
    async with AsyncClient(transport=ASGITransport(app=app, client=client), base_url="http://test") as ac:
        response = await ac.get("/health")
    assert response.status_code == 200
    return response.json()


@pytest.mark.anyio
async def test_health_ok_when_data_writable(ean_db: Path):
    data = await _health()
    assert data["status"] == "ok"
    assert data["data_writable"] is True
    assert data["unwritable_paths"] is None


@pytest.mark.anyio
async def test_health_degraded_when_ean_db_read_only(ean_db: Path):
    """The 2026-08-07 case: the file itself is not writable, its directory is."""
    ean_db.chmod(0o444)
    data = await _health()
    assert data["status"] == "degraded"
    assert data["data_writable"] is False
    assert data["unwritable_paths"] == [str(ean_db)]


@pytest.mark.anyio
async def test_health_degraded_when_data_dir_read_only(ean_db: Path):
    """git replaces files by rename, so it needs the directory as well."""
    ean_db.parent.chmod(0o555)
    data = await _health()
    assert data["status"] == "degraded"
    assert data["unwritable_paths"] == [str(ean_db.parent)]


@pytest.mark.anyio
async def test_health_hides_unwritable_paths_from_remote(ean_db: Path):
    """The verdict is public, the paths are not, as for the paths block."""
    ean_db.chmod(0o444)
    data = await _health(client=("203.0.113.5", 40000))
    assert data["status"] == "degraded"
    assert data["data_writable"] is False
    assert data["unwritable_paths"] is None


@pytest.mark.anyio
async def test_health_ok_before_ean_db_exists(ean_db: Path):
    """A fresh deployment has no observations yet; a writable directory is enough."""
    ean_db.unlink()
    data = await _health()
    assert data["status"] == "ok"
    assert data["data_writable"] is True


@pytest.mark.anyio
async def test_health_degraded_when_vocabulary_read_only(ean_db: Path):
    """vocabulary.yaml lives in the same checkout and is written by PUT /api/vocabulary."""
    vocab = ean_db.parent / "vocabulary.yaml"
    vocab.chmod(0o444)
    data = await _health()
    assert data["status"] == "degraded"
    assert data["unwritable_paths"] == [str(vocab)]
