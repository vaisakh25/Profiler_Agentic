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

_LOOPBACK_HOSTS = {"127.0.0.1", "localhost", "::1"}


def _make_transport_security(host: str):
    """Build a transport-security config compatible with the installed MCP version."""
    try:
        from mcp.server.transport_security import TransportSecuritySettings
    except Exception:
        return None

    if host in _LOOPBACK_HOSTS:
        return None

    return TransportSecuritySettings(enable_dns_rebinding_protection=False)


def create_fastmcp_with_fallback(
    *,
    name: str,
    instructions: str,
    host: str,
    port: int,
    logger: logging.Logger | None = None,
):
    """Create FastMCP with runtime host/port across MCP version differences."""
    from mcp.server.fastmcp import FastMCP

    attempts: list[dict[str, Any]] = []
    transport_security = _make_transport_security(host)

    if transport_security is not None:
        attempts.append(
            {
                "name": name,
                "instructions": instructions,
                "host": host,
                "port": port,
                "transport_security": transport_security,
            }
        )

    attempts.extend(
        [
            {
                "name": name,
                "instructions": instructions,
                "host": host,
                "port": port,
                "allowed_origins": ["*"],
            },
            {
                "name": name,
                "instructions": instructions,
                "host": host,
                "port": port,
            },
            {
                "name": name,
                "instructions": instructions,
            },
        ]
    )

    last_exc: TypeError | None = None
    for kwargs in attempts:
        try:
            return FastMCP(**kwargs)
        except TypeError as exc:
            last_exc = exc
            continue

    if logger is not None and last_exc is not None:
        logger.debug("FastMCP compatibility fallback exhausted: %s", last_exc)
    return FastMCP(name=name, instructions=instructions)


def configure_fastmcp_network(
    mcp: Any,
    *,
    host: str,
    port: int,
    logger: logging.Logger | None = None,
) -> None:
    """Sync host/port and transport security after CLI args are parsed."""
    if logger is None:
        logger = logging.getLogger(__name__)

    mcp.settings.host = host
    mcp.settings.port = port

    transport_security = _make_transport_security(host)
    if transport_security is None:
        return

    if hasattr(mcp.settings, "transport_security"):
        mcp.settings.transport_security = transport_security
        logger.info(
            "Configured permissive transport security for non-loopback host '%s'",
            host,
        )


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
    "configure_fastmcp_network",
    "create_fastmcp_with_fallback",
    "patch_host_validation_permissive",
]
