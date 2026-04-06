from __future__ import annotations

import importlib
from pathlib import Path

import file_profiler.config.env as env_module


def test_default_paths_match_local_repo_layout(monkeypatch):
    for name in (
        "PROFILER_DATA_DIR",
        "PROFILER_UPLOAD_DIR",
        "PROFILER_OUTPUT_DIR",
    ):
        monkeypatch.delenv(name, raising=False)

    reloaded = importlib.reload(env_module)
    project_root = Path(__file__).resolve().parent.parent

    assert reloaded.DATA_DIR == project_root / "data" / "files"
    assert reloaded.UPLOAD_DIR == project_root / "data" / "uploads"
    assert reloaded.OUTPUT_DIR == project_root / "data" / "output"


def test_relative_env_paths_are_anchored_at_project_root(monkeypatch):
    monkeypatch.setenv("PROFILER_DATA_DIR", "./custom/files")
    monkeypatch.setenv("PROFILER_UPLOAD_DIR", "./custom/uploads")
    monkeypatch.setenv("PROFILER_OUTPUT_DIR", "./custom/output")

    reloaded = importlib.reload(env_module)
    project_root = Path(__file__).resolve().parent.parent

    assert reloaded.DATA_DIR == project_root / "custom" / "files"
    assert reloaded.UPLOAD_DIR == project_root / "custom" / "uploads"
    assert reloaded.OUTPUT_DIR == project_root / "custom" / "output"
