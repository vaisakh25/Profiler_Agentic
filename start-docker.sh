#!/bin/bash
# Docker startup script for Agentic Data Profiler

set -e

echo "🚀 Starting Agentic Data Profiler in Docker..."

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null; then
    echo "❌ docker-compose not found. Please install Docker Compose."
    exit 1
fi

# Parse command line arguments
ENABLE_WEB_UI="0"
if [ "$1" == "--web" ] || [ "$1" == "-w" ]; then
    ENABLE_WEB_UI="1"
    echo "📊 Starting with Web UI enabled..."
else
    echo "🔧 Starting MCP servers only (use --web to include Web UI)..."
fi

# Check for .env file
if [ ! -f .env ]; then
    echo "⚠️  No .env file found. Creating from .env.example..."
    if [ -f .env.example ]; then
        cp .env.example .env
        echo "✅ Created .env file. Please edit it with your API keys."
    else
        echo "❌ No .env.example found. Please create .env manually."
    fi
fi

# Build and start services
echo "🔨 Building Docker images..."
ENABLE_WEB_UI=$ENABLE_WEB_UI docker-compose build profiler-suite

echo "▶️  Starting services..."
ENABLE_WEB_UI=$ENABLE_WEB_UI docker-compose up -d profiler-suite

echo ""
echo "⏳ Waiting for services to be healthy..."
sleep 5

# Health check
echo ""
echo "🔍 Checking service health..."
MAX_RETRIES=12
RETRY_COUNT=0

check_health() {
    SERVICE=$1
    PORT=$2
    NAME=$3
    
    while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
        if curl -s http://localhost:$PORT/health > /dev/null 2>&1; then
            echo "✅ $NAME is healthy (port $PORT)"
            return 0
        fi
        RETRY_COUNT=$((RETRY_COUNT + 1))
        echo "⏳ Waiting for $NAME (attempt $RETRY_COUNT/$MAX_RETRIES)..."
        sleep 3
    done
    
    echo "❌ $NAME failed to start (port $PORT)"
    echo "   Check logs with: docker-compose logs $SERVICE"
    return 1
}

# Check File Profiler MCP
if check_health "profiler-suite" 8080 "File Profiler MCP Server"; then
    PROFILER_STATUS="✅"
else
    PROFILER_STATUS="❌"
fi

# Check Connector MCP
if check_health "profiler-suite" 8081 "Data Connector MCP Server"; then
    CONNECTOR_STATUS="✅"
else
    CONNECTOR_STATUS="❌"
fi

# Check Web UI if enabled
WEB_STATUS="⏭️  (not started)"
if [ "$ENABLE_WEB_UI" = "1" ]; then
    if curl -s http://localhost:8501 > /dev/null 2>&1; then
        WEB_STATUS="✅"
    else
        WEB_STATUS="❌"
    fi
fi

echo ""
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "📦 Service Status:"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo "  $PROFILER_STATUS File Profiler MCP: http://localhost:8080/health"
echo "  $CONNECTOR_STATUS Connector MCP:     http://localhost:8081/health"
echo "  $WEB_STATUS Web UI:           http://localhost:8501"
echo "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
echo ""
echo "📝 Next Steps:"
echo "  • View logs:    docker-compose logs -f"
echo "  • Run CLI:      python -m file_profiler.agent --chat"
echo "  • Stop all:     docker-compose down"
echo ""
[ "$ENABLE_WEB_UI" != "1" ] && echo "  💡 Tip: Use ./start-docker.sh --web to enable Web UI"
echo ""
