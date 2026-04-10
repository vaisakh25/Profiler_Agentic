"""
Tests for the MCP server tool handlers.

Tests call the tool functions directly (not via MCP protocol) to verify
that they correctly wrap the pipeline and produce valid output.
"""

from __future__ import annotations

import base64
import textwrap
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from file_profiler.mcp_server import (
    _profile_cache,
    _to_dict,
    get_quality_summary,
    list_supported_files,
    profile_file,
    upload_file,
    visualize_profile,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_csv(path: Path, content: str) -> None:
    path.write_text(textwrap.dedent(content).lstrip(), encoding="utf-8")


def _patch_dirs(monkeypatch, tmp_path: Path) -> None:
    """Point DATA_DIR, UPLOAD_DIR, OUTPUT_DIR at tmp_path subdirectories."""
    data_dir = tmp_path / "data"
    upload_dir = tmp_path / "uploads"
    output_dir = tmp_path / "output"
    data_dir.mkdir(exist_ok=True)
    upload_dir.mkdir(exist_ok=True)
    output_dir.mkdir(exist_ok=True)

    # Patch file_resolver
    monkeypatch.setattr("file_profiler.utils.file_resolver.DATA_DIR", data_dir)
    monkeypatch.setattr("file_profiler.utils.file_resolver.UPLOAD_DIR", upload_dir)
    # Patch mcp_server
    monkeypatch.setattr("file_profiler.mcp_server.OUTPUT_DIR", output_dir)


def _make_ctx() -> AsyncMock:
    """Create a mock MCP Context with report_progress."""
    ctx = AsyncMock()
    ctx.report_progress = AsyncMock()
    return ctx


# ---------------------------------------------------------------------------
# profile_file
# ---------------------------------------------------------------------------

class TestProfileFileTool:

    @pytest.mark.asyncio
    async def test_returns_dict_with_columns(self, tmp_path, monkeypatch):
        _patch_dirs(monkeypatch, tmp_path)
        f = tmp_path / "data" / "orders.csv"
        _write_csv(f, """\
            id,name,amount
            1,Alice,100.50
            2,Bob,200.75
            3,Carol,300.00
        """)

        ctx = _make_ctx()
        result = await profile_file(str(f), ctx=ctx)

        assert isinstance(result, dict)
        assert "columns" in result
        assert len(result["columns"]) == 3
        assert result["table_name"] == "orders"
        assert result["row_count"] == 3

    @pytest.mark.asyncio
    async def test_caches_profile(self, tmp_path, monkeypatch):
        _patch_dirs(monkeypatch, tmp_path)
        _profile_cache.clear()

        f = tmp_path / "data" / "cached.csv"
        _write_csv(f, "id\n1\n2\n")

        ctx = _make_ctx()
        await profile_file(str(f), ctx=ctx)

        assert "cached" in _profile_cache

    @pytest.mark.asyncio
    async def test_reports_progress(self, tmp_path, monkeypatch):
        _patch_dirs(monkeypatch, tmp_path)
        f = tmp_path / "data" / "prog.csv"
        _write_csv(f, "id\n1\n")

        ctx = _make_ctx()
        await profile_file(str(f), ctx=ctx)

        # At minimum, the tool reports progress at start, serialise, and complete
        assert ctx.report_progress.call_count >= 3


# ---------------------------------------------------------------------------
# upload_file
# ---------------------------------------------------------------------------

class TestUploadFileTool:

    @pytest.mark.asyncio
    async def test_upload_returns_path_and_size(self, tmp_path, monkeypatch):
        _patch_dirs(monkeypatch, tmp_path)
        content = b"id,name\n1,Alice\n2,Bob\n"
        b64 = base64.b64encode(content).decode()

        ctx = _make_ctx()
        result = await upload_file("test.csv", b64, ctx=ctx)

        assert "server_path" in result
        assert result["size_bytes"] == len(content)
        assert Path(result["server_path"]).exists()


# ---------------------------------------------------------------------------
# list_supported_files
# ---------------------------------------------------------------------------

class TestListSupportedFiles:

    @pytest.mark.asyncio
    async def test_lists_csv_files(self, tmp_path, monkeypatch):
        _patch_dirs(monkeypatch, tmp_path)
        data_dir = tmp_path / "data"
        _write_csv(data_dir / "a.csv", "id\n1\n")
        _write_csv(data_dir / "b.csv", "id\n2\n")
        (data_dir / "readme.txt").write_text("ignore me")

        ctx = _make_ctx()
        result = await list_supported_files(str(data_dir), ctx=ctx)

        assert len(result) == 2
        assert all(r["detected_format"] == "csv" for r in result)


# ---------------------------------------------------------------------------
# get_quality_summary
# ---------------------------------------------------------------------------

class TestGetQualitySummary:

    @pytest.mark.asyncio
    async def test_returns_quality_dict(self, tmp_path, monkeypatch):
        _patch_dirs(monkeypatch, tmp_path)
        _profile_cache.clear()

        f = tmp_path / "data" / "quality.csv"
        _write_csv(f, "id,name\n1,Alice\n2,Bob\n")

        ctx = _make_ctx()
        result = await get_quality_summary(str(f), ctx=ctx)

        assert "quality_summary" in result
        assert result["source"] == "fresh"
        assert result["table_name"] == "quality"

    @pytest.mark.asyncio
    async def test_returns_cached_on_second_call(self, tmp_path, monkeypatch):
        _patch_dirs(monkeypatch, tmp_path)
        _profile_cache.clear()

        f = tmp_path / "data" / "qual2.csv"
        _write_csv(f, "id\n1\n")

        ctx = _make_ctx()
        await get_quality_summary(str(f), ctx=ctx)
        result2 = await get_quality_summary(str(f), ctx=ctx)

        assert result2["source"] == "cache"


# ---------------------------------------------------------------------------
# visualize_profile
# ---------------------------------------------------------------------------

class TestVisualizeProfileFallback:

    @pytest.mark.asyncio
    async def test_returns_unavailable_when_chart_module_missing(self):
        result = await visualize_profile()

        assert result["status"] == "unavailable"
        assert result["error"] == "visualization_unavailable"
