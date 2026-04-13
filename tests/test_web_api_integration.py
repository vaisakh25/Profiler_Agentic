from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from file_profiler.agent import web_server


@dataclass
class _Summary:
    connection_id: str
    scheme: str
    display_name: str
    created_at: str
    last_tested: str | None
    is_healthy: bool


@dataclass
class _ConnectionInfo:
    connection_id: str
    scheme: str
    display_name: str
    created_at: str


@dataclass
class _TestResult:
    success: bool
    message: str
    latency_ms: float


class _FakeConnectionManager:
    def __init__(self) -> None:
        self._summaries = [
            _Summary(
                connection_id="warehouse-prod",
                scheme="snowflake",
                display_name="Warehouse Production",
                created_at="2026-04-01T00:00:00Z",
                last_tested=None,
                is_healthy=True,
            )
        ]

    def list_connections(self):
        return self._summaries

    def register(self, connection_id: str, scheme: str, credentials: dict, display_name: str = ""):
        return _ConnectionInfo(
            connection_id=connection_id,
            scheme=scheme,
            display_name=display_name or connection_id,
            created_at="2026-04-01T00:00:01Z",
        )

    def remove(self, connection_id: str) -> bool:
        return connection_id == "warehouse-prod"

    def test(self, connection_id: str):
        return _TestResult(
            success=connection_id == "warehouse-prod",
            message="ok" if connection_id == "warehouse-prod" else "not found",
            latency_ms=12.3,
        )


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    async def _fake_checkpointer():
        return object()

    async def _fake_close_pool() -> None:
        return None

    monkeypatch.setattr(web_server, "get_checkpointer", _fake_checkpointer)
    monkeypatch.setattr(web_server, "close_pool", _fake_close_pool)

    data_dir = tmp_path / "mounted"
    data_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(web_server, "DATA_DIR", data_dir)

    upload_dir = tmp_path / "uploads"
    upload_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(web_server, "UPLOAD_DIR", upload_dir)

    with TestClient(web_server.app) as test_client:
        yield test_client


@pytest.mark.integration
def test_connections_list_and_create(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    from file_profiler.connectors import connection_manager

    fake_mgr = _FakeConnectionManager()
    monkeypatch.setattr(connection_manager, "get_connection_manager", lambda: fake_mgr)

    listed = client.get("/api/connections")
    assert listed.status_code == 200
    payload = listed.json()
    assert isinstance(payload, list)
    assert payload[0]["connection_id"] == "warehouse-prod"
    assert "credentials" not in payload[0]

    created = client.post(
        "/api/connections",
        json={
            "connection_id": "warehouse-dev",
            "scheme": "snowflake",
            "display_name": "Warehouse Dev",
            "credentials": {"account": "abc"},
        },
    )
    assert created.status_code == 200
    assert created.json()["connection_id"] == "warehouse-dev"


@pytest.mark.integration
def test_connection_validation_and_test_endpoint(client: TestClient, monkeypatch: pytest.MonkeyPatch) -> None:
    from file_profiler.connectors import connection_manager

    fake_mgr = _FakeConnectionManager()
    monkeypatch.setattr(connection_manager, "get_connection_manager", lambda: fake_mgr)

    invalid = client.post("/api/connections", json={"scheme": "s3"})
    assert invalid.status_code == 400

    tested = client.post("/api/connections/warehouse-prod/test")
    assert tested.status_code == 200
    assert tested.json()["success"] is True

    deleted = client.delete("/api/connections/warehouse-prod")
    assert deleted.status_code == 200
    assert deleted.json()["deleted"] is True


@pytest.mark.integration
def test_sessions_and_upload_endpoints(
    client: TestClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    from file_profiler.agent import session_manager

    async def _list_sessions(limit: int = 30):
        return [{"session_id": "s-1", "label": "Session One", "message_count": 2}]

    async def _touch_session(session_id: str, label: str = ""):
        return {"session_id": session_id, "label": label, "message_count": 0}

    async def _update_session(session_id: str, label: str = "", message_count=None):
        return {"session_id": session_id, "label": label, "message_count": message_count}

    async def _delete_session(session_id: str):
        return session_id == "s-1"

    monkeypatch.setattr(session_manager, "list_sessions", _list_sessions)
    monkeypatch.setattr(session_manager, "touch_session", _touch_session)
    monkeypatch.setattr(session_manager, "update_session", _update_session)
    monkeypatch.setattr(session_manager, "delete_session", _delete_session)

    sessions = client.get("/api/sessions")
    assert sessions.status_code == 200
    assert sessions.json()[0]["session_id"] == "s-1"

    upserted = client.post("/api/sessions", json={"session_id": "s-2", "label": "Two", "message_count": 4})
    assert upserted.status_code == 200
    assert upserted.json()["message_count"] == 4

    deleted = client.delete("/api/sessions/s-1")
    assert deleted.status_code == 200
    assert deleted.json()["deleted"] is True

    upload = client.post(
        "/api/upload",
        files={"file": ("sample.csv", b"id,name\n1,Ada\n", "text/csv")},
    )
    assert upload.status_code == 200
    body = upload.json()
    assert body["file_name"] == "sample.csv"
    assert body["size_bytes"] > 0
    assert Path(body["server_path"]).exists()

    persistent_upload = client.post(
        "/api/upload?target=persistent&batch_id=customer-drop-01",
        files=[
            ("files", ("customers.csv", b"id,name\n1,Ada\n", "text/csv")),
            ("files", ("orders.psv", b"id|customer_id\n10|1\n", "text/plain")),
        ],
    )
    assert persistent_upload.status_code == 200
    persistent_body = persistent_upload.json()
    assert persistent_body["file_count"] == 2
    assert persistent_body["storage_target"] == "persistent"
    assert Path(persistent_body["upload_dir"]).parent == tmp_path / "mounted"
    for item in persistent_body["files"]:
        assert Path(item["server_path"]).exists()

    invalid_target = client.post(
        "/api/upload?target=archive",
        files={"file": ("sample.csv", b"id,name\n1,Ada\n", "text/csv")},
    )
    assert invalid_target.status_code == 400
