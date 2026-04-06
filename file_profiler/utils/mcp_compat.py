"""FastMCP compatibility and host-validation patch helpers."""

from __future__ import annotations

import importlib
import logging
import sys
from typing import Any

_HOST_VALIDATION_ATTRS = ("validate_request_origin", "_validate_request_origin")
_PATCH_MARKER_ATTR = "__profiler_host_validation_patch__"


def _allow_all_origins(*args: Any, **kwargs: Any) -> bool:
    """Permissive request-origin validator for internal Docker/K8s traffic."""
    return True


setattr(_allow_all_origins, _PATCH_MARKER_ATTR, True)


def create_fastmcp_with_fallback(
    *,
    name: str,
    instructions: str,
    logger: logging.Logger | None = None,
):
    """Create FastMCP with allowed_origins when supported by installed version."""
    from mcp.server.fastmcp import FastMCP

    try:
        return FastMCP(name=name, instructions=instructions, allowed_origins=["*"])
    except TypeError:
        if logger is not None:
            logger.debug(
                "FastMCP allowed_origins is unsupported by installed version; "
                "falling back to default constructor"
            )
        return FastMCP(name=name, instructions=instructions)


def _patch_attr(module_name: str, module_obj: Any, attr_name: str) -> tuple[str, bool] | None:
    """Patch a callable validation attribute; return (qualified_name, patched_now)."""
    current = getattr(module_obj, attr_name, None)
    if not callable(current):
        return None

    qualified_name = f"{module_name}.{attr_name}"
    if getattr(current, _PATCH_MARKER_ATTR, False):
        return qualified_name, False

    setattr(module_obj, attr_name, _allow_all_origins)
    return qualified_name, True


def patch_host_validation_permissive(
    logger: logging.Logger | None = None,
) -> list[str]:
    """Patch known MCP validation hooks to allow internal host/origin traffic.

    Returns a sorted list of active patched hook names.
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    active_hooks: set[str] = set()
    patched_now: set[str] = set()

    explicit_modules = (
        "mcp.server.transport_security",
        "mcp.server.sse",
        "mcp.server.streamable_http",
    )

    for module_name in explicit_modules:
        try:
            module_obj = importlib.import_module(module_name)
        except Exception:
            continue

        for attr_name in _HOST_VALIDATION_ATTRS:
            result = _patch_attr(module_name, module_obj, attr_name)
            if result is None:
                continue
            qualified_name, was_patched = result
            active_hooks.add(qualified_name)
            if was_patched:
                patched_now.add(qualified_name)

    # Patch any currently loaded mcp.server.* modules that expose known hooks.
    for module_name, module_obj in list(sys.modules.items()):
        if not module_name.startswith("mcp.server"):
            continue
        if module_obj is None:
            continue

        for attr_name in _HOST_VALIDATION_ATTRS:
            result = _patch_attr(module_name, module_obj, attr_name)
            if result is None:
                continue
            qualified_name, was_patched = result
            active_hooks.add(qualified_name)
            if was_patched:
                patched_now.add(qualified_name)

    if patched_now:
        logger.info(
            "Disabled strict host validation for internal deployment; patched hooks: %s",
            ", ".join(sorted(patched_now)),
        )
    elif active_hooks:
        logger.info(
            "Host-validation patch already active for hooks: %s",
            ", ".join(sorted(active_hooks)),
        )
    else:
        logger.debug("No MCP host-validation hooks found to patch")

    return sorted(active_hooks)


__all__ = [
    "create_fastmcp_with_fallback",
    "patch_host_validation_permissive",
]
