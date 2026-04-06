"""Shared runtime configuration loader.

Precedence (highest first):
1) Process environment variables
2) .env at project root
3) config.yml at project root

`config.yml` is intended for non-secret runtime defaults. Secrets and
connection credentials should remain in environment variables.
"""

from __future__ import annotations

import os
from pathlib import Path


_PROJECT_ROOT = Path(__file__).resolve().parents[2]
_ENV_PATH = _PROJECT_ROOT / ".env"
_CONFIG_PATH = _PROJECT_ROOT / "config.yml"


def _load_dotenv() -> None:
    """Load .env once, without overriding process-level environment variables."""
    try:
        from dotenv import load_dotenv
    except ImportError:
        return
    load_dotenv(_ENV_PATH, override=False)


def _strip_inline_comment(value: str) -> str:
    """Strip # comments while preserving quoted values."""
    in_single = False
    in_double = False
    for idx, char in enumerate(value):
        if char == "'" and not in_double:
            in_single = not in_single
            continue
        if char == '"' and not in_single:
            in_double = not in_double
            continue
        if char == "#" and not in_single and not in_double:
            return value[:idx].rstrip()
    return value.strip()


def _unquote(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
        return value[1:-1]
    return value


def _load_yaml_defaults() -> dict[str, str]:
    """Load top-level scalar defaults from config.yml.

    Tries PyYAML first if available. Falls back to a tiny parser that supports
    flat `KEY: value` mappings so runtime does not require extra dependencies.
    """
    if not _CONFIG_PATH.exists():
        return {}

    # Preferred parser when available.
    try:
        import yaml  # type: ignore[import-not-found]

        loaded = yaml.safe_load(_CONFIG_PATH.read_text(encoding="utf-8")) or {}
        if isinstance(loaded, dict):
            out: dict[str, str] = {}
            for key, value in loaded.items():
                if isinstance(key, str):
                    out[key] = "" if value is None else str(value)
            return out
    except Exception:
        pass

    # Dependency-free fallback parser for simple top-level key/value YAML.
    out: dict[str, str] = {}
    for raw_line in _CONFIG_PATH.read_text(encoding="utf-8").splitlines():
        if not raw_line.strip() or raw_line.lstrip().startswith("#"):
            continue
        if raw_line.startswith((" ", "\t")):
            continue
        if ":" not in raw_line:
            continue

        key, raw_value = raw_line.split(":", 1)
        key = key.strip()
        if not key:
            continue

        value = _strip_inline_comment(raw_value.strip())
        if value in {"", "~", "null", "Null", "NULL"}:
            out[key] = ""
            continue
        out[key] = _unquote(value)

    return out


_load_dotenv()
_CONFIG_DEFAULTS: dict[str, str] = _load_yaml_defaults()


def inject_config_defaults() -> None:
    """Expose config.yml defaults through os.environ without overriding env vars."""
    for key, value in _CONFIG_DEFAULTS.items():
        os.environ.setdefault(key, value)


inject_config_defaults()


def get_config(name: str, default: str) -> str:
    """Read config with env-first precedence and config.yml fallback."""
    if name in os.environ:
        return os.environ[name]
    return _CONFIG_DEFAULTS.get(name, default)
