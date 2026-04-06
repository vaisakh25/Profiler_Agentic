"""Tests for defensive numeric parsing in config.env."""

from __future__ import annotations

import logging

from file_profiler.config import env as config_env


def test_int_from_config_uses_default_on_invalid_value(monkeypatch, caplog) -> None:
    """Invalid runtime values should not crash import-time numeric parsing."""
    monkeypatch.setattr(
        config_env,
        "get_config",
        lambda name, default: "MAX_PARALLEL_WORKERS",
    )

    with caplog.at_level(logging.WARNING):
        value = config_env._int_from_config("MAX_UPLOAD_SIZE_MB", 500)

    assert value == 500
    assert "Invalid MAX_UPLOAD_SIZE_MB='MAX_PARALLEL_WORKERS'" in caplog.text


def test_int_from_config_returns_numeric_value(monkeypatch) -> None:
    monkeypatch.setattr(config_env, "get_config", lambda name, default: "42")
    assert config_env._int_from_config("MAX_PARALLEL_WORKERS", 4) == 42
