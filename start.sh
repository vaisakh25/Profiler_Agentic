#!/bin/bash
# Simple single-container startup script for Linux/Mac

set -e

echo "================================================"
echo "Data Profiler - Simple Mode (Single Container)"
echo "================================================"
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "ERROR: Docker is not running!"
    echo "Please start Docker and try again."
    exit 1
fi

echo "Starting Data Profiler in simple mode..."
echo "(All services in one container)"
echo ""

docker-compose --profile simple up -d

echo ""
echo "================================================"
echo "     SUCCESS! Services are starting..."
echo "================================================"
echo ""
echo "Waiting for services to be ready (15 seconds)..."
sleep 15

echo ""
echo "Services:"
echo "  - File Profiler MCP:  http://localhost:8080"
echo "  - Connector MCP:      http://localhost:8081"
echo "  - Web UI:             http://localhost:8501"
echo ""
echo "Opening Web UI in your browser..."

# Open browser (cross-platform)
if command -v xdg-open > /dev/null; then
    xdg-open http://localhost:8501
elif command -v open > /dev/null; then
    open http://localhost:8501
else
    echo "Please open http://localhost:8501 in your browser"
fi

echo ""
echo "To stop:  docker-compose down"
echo "To view logs:  docker-compose logs -f"
echo ""
