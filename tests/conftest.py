from pathlib import Path

import pytest
from httpx import ASGITransport, AsyncClient

import tingbok.app as app_module
from tingbok.app import app


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture(autouse=True)
def _load_vocabulary():
    """Ensure vocabulary is loaded for all tests."""
    if not app_module.vocabulary:
        app_module.vocabulary = app_module._load_vocabulary()


@pytest.fixture
def skos_cache_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Provide a temporary SKOS cache directory, wired into the app."""
    cache_dir = tmp_path / "skos"
    cache_dir.mkdir()
    monkeypatch.setattr(app_module, "SKOS_CACHE_DIR", cache_dir)
    return cache_dir


@pytest.fixture
async def client():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        yield ac
