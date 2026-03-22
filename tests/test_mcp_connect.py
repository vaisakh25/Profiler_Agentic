"""Test MCP connection directly."""
import asyncio
import sys
import traceback

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


async def test():
    from langchain_mcp_adapters.client import MultiServerMCPClient

    print("Creating MCP client...")
    client = MultiServerMCPClient({
        "file-profiler": {
            "url": "http://localhost:8080/sse",
            "transport": "sse",
            "timeout": 60,
            "sse_read_timeout": 3600,
        }
    })

    print("Calling get_tools()...")
    try:
        tools = await client.get_tools()
        print(f"Got {len(tools)} tools:")
        for t in tools:
            print(f"  - {t.name}")
    except Exception as e:
        print(f"ERROR: {e}")
        traceback.print_exc()


asyncio.run(test())
