"""Unit tests for MCP endpoint and transport resolution helpers."""

from file_profiler.agent.mcp_endpoints import (
    derive_connector_url,
    resolve_mcp_endpoints,
    resolve_transport,
)


def test_resolve_transport_for_direct_sse_endpoint() -> None:
    assert resolve_transport("http://localhost:8080/sse") == "sse"


def test_resolve_transport_for_gateway_sse_endpoint() -> None:
    assert resolve_transport("http://localhost:9050/mcp/file/sse") == "sse"


def test_resolve_transport_for_streamable_endpoint() -> None:
    assert resolve_transport("http://localhost:9050/mcp") == "streamable_http"


def test_derive_connector_url_for_direct_sse() -> None:
    assert derive_connector_url("http://localhost:8080/sse") == "http://localhost:8081/sse"


def test_derive_connector_url_for_gateway_sse() -> None:
    assert (
        derive_connector_url("http://localhost:9050/mcp/file/sse")
        == "http://localhost:9050/mcp/connector/sse"
    )


def test_resolve_mcp_endpoints_derives_connector_and_transport() -> None:
    file_url, connector_url, transport = resolve_mcp_endpoints(
        "http://localhost:8080/sse"
    )

    assert file_url == "http://localhost:8080/sse"
    assert connector_url == "http://localhost:8081/sse"
    assert transport == "sse"
