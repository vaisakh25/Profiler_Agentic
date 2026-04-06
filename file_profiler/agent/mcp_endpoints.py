"""Helpers for resolving MCP endpoints and transport modes."""

from __future__ import annotations

import re
from urllib.parse import urlsplit, urlunsplit

from file_profiler.config.env import CONNECTOR_MCP_PORT

DEFAULT_FILE_MCP_URL = "http://localhost:8080/sse"


def _normalize_url(url: str | None, default: str = DEFAULT_FILE_MCP_URL) -> str:
    """Return a trimmed URL or a safe default."""
    if isinstance(url, str) and url.strip():
        return url.strip()
    return default


def derive_connector_url(base_url: str, connector_port: int = CONNECTOR_MCP_PORT) -> str:
    """Derive connector URL by replacing the port in the base MCP URL."""
    resolved = _normalize_url(base_url)

    # Gateway-style routing keeps the same port and swaps the path segment.
    if "/mcp/file/" in resolved:
        return resolved.replace("/mcp/file/", "/mcp/connector/", 1)
    if resolved.endswith("/mcp/file"):
        return f"{resolved[:-len('/mcp/file')]}/mcp/connector"

    swapped = re.sub(r":\d+(?=/)", f":{connector_port}", resolved, count=1)
    if swapped != resolved:
        return swapped

    split = urlsplit(resolved)
    if not split.scheme or not split.hostname:
        return resolved

    host = split.hostname
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"

    auth = ""
    if split.username:
        auth = split.username
        if split.password:
            auth += f":{split.password}"
        auth += "@"

    netloc = f"{auth}{host}:{connector_port}"
    path = split.path or "/sse"
    return urlunsplit((split.scheme, netloc, path, split.query, split.fragment))


def resolve_transport(mcp_url: str) -> str:
    """Resolve the MCP transport from the endpoint path."""
    path = urlsplit(_normalize_url(mcp_url)).path.lower().rstrip("/")

    # SSE endpoints are explicit and should always map to SSE transport.
    if path.endswith("/sse"):
        return "sse"

    # Streamable HTTP endpoints are typically rooted at /mcp.
    if path.endswith("/mcp") or "/mcp/" in f"{path}/":
        return "streamable_http"

    return "sse"


def resolve_mcp_endpoints(
    mcp_url: str | None,
    connector_mcp_url: str | None = None,
) -> tuple[str, str, str]:
    """Return normalized file URL, connector URL, and transport mode."""
    file_url = _normalize_url(mcp_url)

    if isinstance(connector_mcp_url, str) and connector_mcp_url.strip():
        connector_url = connector_mcp_url.strip()
    else:
        connector_url = derive_connector_url(file_url)

    transport = resolve_transport(file_url)
    return file_url, connector_url, transport


__all__ = [
    "DEFAULT_FILE_MCP_URL",
    "derive_connector_url",
    "resolve_mcp_endpoints",
    "resolve_transport",
]
