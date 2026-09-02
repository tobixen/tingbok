"""The MCP endpoint mounted at ``/mcp`` has to actually answer.

``FastMCP.http_app()`` is a Starlette app carrying a lifespan of its own — it
is where the session manager is created — and ``app.mount()`` does not run a
mounted app's lifespan.  Nothing else in the suite starts the real lifespan, so
without this test the whole MCP surface can be dead in production while every
other test passes: the tool list is built at import time and answers happily
from memory whether or not the transport underneath it was ever started.
"""

import json

import pytest
from httpx import ASGITransport, AsyncClient

import tingbok.app as app_module

_INITIALIZE = {
    "jsonrpc": "2.0",
    "id": 1,
    "method": "initialize",
    "params": {
        "protocolVersion": "2025-06-18",
        "capabilities": {},
        "clientInfo": {"name": "tingbok-tests", "version": "0"},
    },
}


def _sse_payload(body: str) -> dict:
    """Pull the single JSON message out of a text/event-stream response."""
    for line in body.splitlines():
        if line.startswith("data: "):
            return json.loads(line[len("data: ") :])
    raise AssertionError(f"no SSE data frame in {body!r}")


@pytest.mark.anyio
@pytest.mark.usefixtures("_no_background_tasks")
@pytest.mark.parametrize("path", ["/mcp", "/mcp/"])
async def test_the_mounted_mcp_endpoint_completes_a_session(path: str) -> None:
    """Initialize and list tools over HTTP, through the real lifespan.

    Both spellings of the path are checked: mounting puts the endpoint at
    ``/mcp/``, and bare ``/mcp`` only answers because the scope is rewritten
    before routing.  Without that it is a 307, which an existing client
    configured against the previous mount may or may not follow.
    """
    async with (
        app_module.lifespan(app_module.app),
        AsyncClient(transport=ASGITransport(app=app_module.app), base_url="http://test") as ac,
    ):
        headers = {"Accept": "application/json, text/event-stream"}
        response = await ac.post(path, json=_INITIALIZE, headers=headers)
        assert response.status_code == 200, response.text

        session = response.headers.get("mcp-session-id")
        assert session, f"no session id in {dict(response.headers)}"
        assert _sse_payload(response.text)["result"]["serverInfo"]["name"] == "tingbok"

        headers["mcp-session-id"] = session
        await ac.post(
            path,
            json={"jsonrpc": "2.0", "method": "notifications/initialized"},
            headers=headers,
        )
        listed = await ac.post(
            path,
            json={"jsonrpc": "2.0", "id": 2, "method": "tools/list"},
            headers=headers,
        )
        assert listed.status_code == 200, listed.text
        names = {tool["name"] for tool in _sse_payload(listed.text)["result"]["tools"]}
        assert "get_sources_api_sources_get" in names
