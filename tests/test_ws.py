"""Quick WebSocket test: profile WWI CSVs via the web UI."""
import asyncio
import json
import sys

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


async def test():
    import websockets

    uri = "ws://localhost:8501/ws/chat"
    async with websockets.connect(uri, max_size=10 * 1024 * 1024) as ws:
        # Send config
        await ws.send(json.dumps({
            "type": "config",
            "session_id": "test-csv-run",
            "mcp_url": "http://localhost:8080/sse",
        }))

        resp = json.loads(await ws.recv())
        print(f"CONFIG: {resp}")

        if resp.get("type") != "connected":
            print("Failed to connect to MCP")
            return

        # Send profile message
        msg = "Profile all the CSV files in C:/Projects/profiler/Profiler_Agentic_LLM/Profiler_Agentic/data/files/wwi_files"
        await ws.send(json.dumps({"type": "message", "content": msg}))
        print(f"SENT: {msg}\n")

        while True:
            try:
                raw = await asyncio.wait_for(ws.recv(), timeout=300)
                data = json.loads(raw)
                t = data.get("type", "?")

                if t == "tool_start":
                    print(f"  TOOL_START: {data.get('tool')}")
                elif t == "tool_result":
                    print(f"  TOOL_RESULT: {data.get('tool')} | success={data.get('success')} | {data.get('summary', '')[:150]}")
                elif t == "progress":
                    pct = data.get("percent", 0)
                    stage = data.get("stage", "")
                    if pct % 20 < 2 or pct >= 99:
                        print(f"  PROGRESS: {pct}% - {stage[:80]}")
                elif t == "assistant":
                    content = data.get("content", "")
                    print(f"\n  ASSISTANT ({len(content)} chars):\n{content[:800]}")
                    break
                elif t == "error":
                    print(f"  ERROR: {data.get('content', '')}")
                    break
                elif t == "er_diagram":
                    print(f"  ER_DIAGRAM received: {len(data.get('content', ''))} chars")
                elif t in ("pipeline_steps", "step_update", "step_complete", "thinking"):
                    pass
                else:
                    print(f"  {t}: {str(data)[:120]}")
            except asyncio.TimeoutError:
                print("TIMEOUT waiting for response")
                break


asyncio.run(test())
