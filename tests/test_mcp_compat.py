from __future__ import annotations

from mcp.server.fastmcp import FastMCP

from file_profiler.utils.mcp_compat import (
    configure_fastmcp_network,
    create_fastmcp_with_fallback,
)


def test_create_fastmcp_uses_requested_host_and_port() -> None:
    mcp = create_fastmcp_with_fallback(
        name="test-profiler",
        instructions="test",
        host="0.0.0.0",
        port=9050,
    )

    assert mcp.settings.host == "0.0.0.0"
    assert mcp.settings.port == 9050

    if hasattr(mcp.settings, "transport_security"):
        transport_security = mcp.settings.transport_security
        assert (
            transport_security is None
            or transport_security.enable_dns_rebinding_protection is False
        )


def test_configure_fastmcp_network_disables_stale_localhost_transport_security() -> None:
    mcp = FastMCP(name="test-profiler", instructions="test")

    # Current MCP versions auto-enable localhost-only validation here.
    if hasattr(mcp.settings, "transport_security"):
        assert mcp.settings.transport_security is not None

    configure_fastmcp_network(mcp, host="0.0.0.0", port=9050)

    assert mcp.settings.host == "0.0.0.0"
    assert mcp.settings.port == 9050

    if hasattr(mcp.settings, "transport_security"):
        assert mcp.settings.transport_security is not None
        assert mcp.settings.transport_security.enable_dns_rebinding_protection is False
