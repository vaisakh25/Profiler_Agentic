"""WebSocket smoke test for the web UI chat endpoint."""

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from file_profiler.agent import web_server


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    async def _fake_checkpointer():
        return object()

    async def _fake_close_pool() -> None:
        return None

    async def _fake_build_graph(mcp_url: str, provider=None, model=None):
        class _DummyGraph:
            async def aget_state(self, config):
                return None

        return _DummyGraph(), 3

    async def _fake_touch_session(session_id: str, label: str = ""):
        return {"session_id": session_id, "label": label, "message_count": 0}

    monkeypatch.setattr(web_server, "get_checkpointer", _fake_checkpointer)
    monkeypatch.setattr(web_server, "close_pool", _fake_close_pool)
    monkeypatch.setattr(web_server, "_build_graph", _fake_build_graph)

    from file_profiler.agent import session_manager

    monkeypatch.setattr(session_manager, "touch_session", _fake_touch_session)

    upload_dir = tmp_path / "uploads"
    upload_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(web_server, "UPLOAD_DIR", upload_dir)

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
