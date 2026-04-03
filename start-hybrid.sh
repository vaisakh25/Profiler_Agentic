#!/bin/bash
# Hybrid Deployment - MCP Servers in Docker, Web UI Local
# =========================================================

echo ""
echo "========================================"
echo "  Agentic Profiler - Hybrid Startup"
echo "========================================"
echo ""

echo "[1/3] Starting MCP servers in Docker..."
docker compose up -d
if [ $? -ne 0 ]; then
    echo "ERROR: Failed to start Docker containers"
    exit 1
fi

echo ""
echo "[2/3] Waiting for MCP servers to initialize..."
sleep 5

echo ""
echo "[3/3] Starting Web UI locally..."
echo ""
echo "Press Ctrl+C to stop the Web UI when done."
echo ""

# Start Web UI in foreground
f:/agentic_profiler/Profiler_Agentic/.venv/Scripts/python.exe -m file_profiler.agent --web --web-port 8501 --mcp-url http://localhost:8080/sse --connector-mcp-url http://localhost:8081/sse

echo ""
echo "Web UI stopped."
echo ""
echo "To stop Docker MCP servers, run: docker compose down"
