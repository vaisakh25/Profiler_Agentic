"""Tests for file_resolver.py — path security and upload handling."""

from __future__ import annotations

import base64
import os
from pathlib import Path

import pytest

from file_profiler.utils.file_resolver import (
    PathSecurityError,
    resolve_path,
    save_upload,
)


# ---------------------------------------------------------------------------
# resolve_path
# ---------------------------------------------------------------------------

class TestResolvePath:

    def test_rejects_path_outside_allowed_dirs(self, tmp_path, monkeypatch):
        """Paths that resolve outside DATA_DIR / UPLOAD_DIR must be rejected."""
        monkeypatch.setenv("PROFILER_DATA_DIR", str(tmp_path / "data"))
        monkeypatch.setenv("PROFILER_UPLOAD_DIR", str(tmp_path / "uploads"))
        # Force re-import so env vars take effect
        _reload_env(monkeypatch, tmp_path)

        with pytest.raises(PathSecurityError, match="Access denied"):
            resolve_path(str(tmp_path / "outside" / "secret.csv"))

    def test_accepts_file_under_data_dir(self, tmp_path, monkeypatch):
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        f = data_dir / "test.csv"
        f.write_text("id\n1\n", encoding="utf-8")
        _reload_env(monkeypatch, tmp_path)

        result = resolve_path(str(f))
        assert result == f.resolve()

    def test_accepts_file_under_upload_dir(self, tmp_path, monkeypatch):
        upload_dir = tmp_path / "uploads"
        upload_dir.mkdir()
        f = upload_dir / "uploaded.csv"
        f.write_text("id\n1\n", encoding="utf-8")
        _reload_env(monkeypatch, tmp_path)

        result = resolve_path(str(f))
        assert result == f.resolve()

    def test_raises_file_not_found(self, tmp_path, monkeypatch):
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        _reload_env(monkeypatch, tmp_path)

        with pytest.raises(FileNotFoundError):
            resolve_path(str(data_dir / "nonexistent.csv"))

    def test_maps_windows_project_path_to_data_dir(self, tmp_path, monkeypatch):
        data_files = tmp_path / "data" / "files"
        data_files.mkdir(parents=True)
        f = data_files / "orders.csv"
        f.write_text("id\n1\n", encoding="utf-8")
        _reload_env(monkeypatch, tmp_path)

        resolved = resolve_path(r"F:\agentic_profiler\Profiler_Agentic\data\files\orders.csv")
        assert resolved == f.resolve()

    def test_windows_path_outside_allowed_dirs_returns_actionable_hint(self, tmp_path, monkeypatch):
        _reload_env(monkeypatch, tmp_path)

        with pytest.raises(PathSecurityError) as exc_info:
            resolve_path(r"C:\Users\someone\Downloads\customers.csv")

        msg = str(exc_info.value)
        assert "Access denied" in msg
        assert "Try:" in msg


# ---------------------------------------------------------------------------
# save_upload
# ---------------------------------------------------------------------------

class TestSaveUpload:

    def test_saves_and_returns_path(self, tmp_path, monkeypatch):
        upload_dir = tmp_path / "uploads"
        _reload_env(monkeypatch, tmp_path)

        content = b"id,name\n1,Alice\n"
        b64 = base64.b64encode(content).decode()
        dest = save_upload("test.csv", b64)

        assert dest.exists()
        assert dest.read_bytes() == content
        assert dest.name == "test.csv"

    def test_rejects_invalid_base64(self, tmp_path, monkeypatch):
        _reload_env(monkeypatch, tmp_path)

        with pytest.raises(ValueError, match="Invalid base64"):
            save_upload("bad.csv", "not-valid-base64!!!")

    def test_rejects_oversized_upload(self, tmp_path, monkeypatch):
        _reload_env(monkeypatch, tmp_path)
        monkeypatch.setattr(
            "file_profiler.utils.file_resolver.MAX_UPLOAD_SIZE_MB", 0.0001
        )
        content = b"x" * 1024  # ~1 KB, exceeds 0.0001 MB limit
        b64 = base64.b64encode(content).decode()

        with pytest.raises(ValueError, match="Upload too large"):
            save_upload("big.csv", b64)


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _reload_env(monkeypatch, tmp_path: Path) -> None:
    """Patch env vars AND reload the module-level constants in file_resolver."""
    data_dir = tmp_path / "data"
    upload_dir = tmp_path / "uploads"
    data_dir.mkdir(exist_ok=True)
    upload_dir.mkdir(exist_ok=True)

    monkeypatch.setattr(
        "file_profiler.utils.file_resolver.DATA_DIR", data_dir,
    )
    monkeypatch.setattr(
        "file_profiler.utils.file_resolver.UPLOAD_DIR", upload_dir,
    )
