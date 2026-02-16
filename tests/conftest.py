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
async def client():
    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as ac:
        yield ac
