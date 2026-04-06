"""WebSocket smoke test for the web UI chat endpoint."""

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from file_profiler.agent import web_server


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    build_graph_calls: list[dict] = []

    async def _fake_checkpointer():
        return object()

    async def _fake_close_pool() -> None:
        return None

    async def _fake_build_graph(
        mcp_url: str,
        connector_mcp_url: str | None = None,
        provider=None,
        model=None,
    ):
        build_graph_calls.append(
            {
                "mcp_url": mcp_url,
                "connector_mcp_url": connector_mcp_url,
                "provider": provider,
                "model": model,
            }
        )

        class _DummyGraph:
            async def aget_state(self, config):
                return None

        return _DummyGraph(), 3

    async def _fake_touch_session(session_id: str, label: str = ""):
        return {"session_id": session_id, "label": label, "message_count": 0}

    monkeypatch.setattr(web_server, "get_checkpointer", _fake_checkpointer)
    monkeypatch.setattr(web_server, "close_pool", _fake_close_pool)
    monkeypatch.setattr(web_server, "_build_graph", _fake_build_graph)
    monkeypatch.setenv("WEB_MCP_URL", "")
    monkeypatch.setenv("WEB_CONNECTOR_MCP_URL", "")

    from file_profiler.agent import session_manager

    monkeypatch.setattr(session_manager, "touch_session", _fake_touch_session)

    upload_dir = tmp_path / "uploads"
    upload_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(web_server, "UPLOAD_DIR", upload_dir)
    web_server.app.state._test_build_graph_calls = build_graph_calls

    with TestClient(web_server.app) as test_client:
        yield test_client


def test_ws_chat_config_handshake(client: TestClient) -> None:
    with client.websocket_connect("/ws/chat") as ws:
        ws.send_json(
            {
                "type": "config",
                "session_id": "test-csv-run",
                "mcp_url": "http://localhost:8080/sse",
            }
        )
        resp = ws.receive_json()

    assert resp.get("type") == "connected", f"Unexpected handshake response: {resp}"
    assert resp.get("session_id") == "test-csv-run"
    assert resp.get("tools") == 3

    calls = client.app.state._test_build_graph_calls
    assert len(calls) == 1
    assert calls[0]["mcp_url"] == "http://localhost:8080/sse"
    assert calls[0]["connector_mcp_url"] == "http://localhost:8081/sse"
