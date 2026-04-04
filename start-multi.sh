#!/bin/bash
# Multi-container startup script for Linux/Mac

set -e

echo "================================================"
echo "Data Profiler - Multi Mode (Production)"
echo "================================================"
echo ""
echo "Architecture: 4 Containers + Nginx Reverse Proxy"
echo "- Nginx (Gateway): Port 9050"
echo "- Web UI: Internal"
echo "- MCP File Server: Internal"
echo "- MCP Connector: Internal"
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "ERROR: Docker is not running!"
    echo "Please start Docker and try again."
    exit 1
fi

echo "Starting Data Profiler in multi mode..."
echo ""

docker-compose --profile multi up -d --build

echo ""
echo "================================================"
echo "     SUCCESS! Services are starting..."
echo "================================================"
echo ""
echo "Waiting for all containers to be healthy (20 seconds)..."
sleep 20

echo ""
echo "Services:"
echo "  - Nginx Gateway:      http://localhost:9050"
echo "  - Web UI:             http://localhost:9050"
echo "  - MCP File Server:    Internal (via Nginx)"
echo "  - MCP Connector:      Internal (via Nginx)"
echo ""
echo "Opening Web UI in your browser..."

# Open browser (cross-platform)
if command -v xdg-open > /dev/null; then
    xdg-open http://localhost:9050
elif command -v open > /dev/null; then
    open http://localhost:9050
else
    echo "Please open http://localhost:9050 in your browser"
fi

echo ""
echo "To stop:  docker-compose --profile multi down"
echo "To view logs:  docker-compose --profile multi logs -f"
echo ""
