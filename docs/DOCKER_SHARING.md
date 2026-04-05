# Docker Deployment Guide

## Quick Start

Run the entire stack with a single command:

```bash
docker compose --profile simple up -d
```

This starts:
- **Profiler MCP Server** (port 8080)
- **Connector MCP Server** (port 8081)
- **Web UI** (port 8501)

Access the web UI at: **http://localhost:8501**

## Overview

This Docker image contains the **Agentic Profiler** with MCP servers for data profiling and connector management, plus a web-based UI for interactive use.

## What's Included

✅ **Profiler MCP Server** (port 8080)
- File profiling (CSV, JSON, Parquet, Excel)
- Relationship detection
- Quality analysis
- ER diagram generation

✅ **Connector MCP Server** (port 8081)
- Remote data sources (PostgreSQL, Snowflake, S3, ADLS, GCS)
- Connection management
- Remote profiling

✅ **Web UI** (port 8501)
- Interactive chat interface
- Real-time profiling
- Visual ER diagrams
- Session history

✅ **NVIDIA Embeddings** - Pre-configured for LLM-powered enrichment

## Running the Container

### Option 1: Using docker-compose (Recommended)

**Start all services:**
```bash
docker compose --profile simple up -d
```

**Stop all services:**
```bash
docker compose down
```

**View logs:**
```bash
docker compose logs -f
```

### Option 2: Selective Service Deployment

Run only specific services by setting environment variables:

**MCP servers only (no web UI):**
```bash
ENABLE_WEB_UI=0 docker compose --profile simple up -d
```

**Profiler MCP only:**
```bash
ENABLE_CONNECTOR_MCP=0 ENABLE_WEB_UI=0 docker compose --profile simple up -d
```

## Health Check

```bash
# Test Profiler MCP
curl http://localhost:8080/health

# Test Connector MCP  
curl http://localhost:8081/health

# Test Web UI
curl http://localhost:8501/
```

## Accessing Services

### Web UI (Interactive)
Open in your browser: **http://localhost:8501**

Features:
- Natural language chat interface
- File profiling and analysis
- Relationship detection
- ER diagram visualization
- Session management

### MCP API Endpoints (Programmatic)

### Profiler MCP Server (8080)
- SSE Transport: `http://localhost:8080/sse`
- Available tools: `list_supported_files`, `profile_file`, `profile_directory`, `detect_relationships`, `enrich_relationships`, etc.

### Connector MCP Server (8081)
- SSE Transport: `http://localhost:8081/sse`
- Available tools: `connect_source`, `list_connections`, `profile_remote_source`, `remote_enrich_relationships`, etc.

## Environment Variables

### Required for LLM Features
- `NVIDIA_API_KEY` - For NVIDIA embeddings and LLM
- `OPENAI_API_KEY` - Alternative LLM provider
- `GROQ_API_KEY` - Alternative LLM provider

### Optional Configuration
- `ENABLE_PROFILER_MCP` - Enable/disable Profiler MCP (default: `1`)
- `ENABLE_CONNECTOR_MCP` - Enable/disable Connector MCP (default: `1`)
- `ENABLE_WEB_UI` - Enable/disable Web UI (default: `1`)
- `LLM_PROVIDER` - Default: `openai` (options: `openai`, `groq`, `google`, `anthropic`)
- `LLM_MODEL` - Default: `mistralai/mistral-large-3-675b-instruct-2512`
- `MAX_PARALLEL_WORKERS` - Default: `2`
- `DUCKDB_MEMORY_LIMIT` - Default: `512MB`
- `LOG_LEVEL` - Default: `INFO`

## Sharing the Image

### Option 1: Export as tar.gz

```bash
# Export the image
docker save profiler_agentic-profiler-suite:latest | gzip > profiler-agentic.tar.gz

# On recipient machine, load the image
gunzip -c profiler-agentic.tar.gz | docker load
```

### Option 2: Push to Docker Registry

```bash
# Tag the image
docker tag profiler_agentic-profiler-suite:latest yourregistry/profiler-suite:latest

# Push to registry
docker push yourregistry/profiler-suite:latest

# On recipient machine, pull the image
docker pull yourregistry/profiler-suite:latest
```

## MCP Client Integration

Connect to the MCP servers from your application:

```python
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

# Connect to Profiler MCP
async with stdio_client(StdioServerParameters(
    command="python",
    args=["-m", "file_profiler", "--transport", "sse", "--port", "8080"]
)) as (read, write):
    async with ClientSession(read, write) as session:
        tools = await session.list_tools()
        # Use the tools...
```

## Data Volumes

The container expects data to be mounted at:
- `/data/mounted` - Your data files (read-only)
Example with custom data directory:
```bash
docker compose down
# Edit docker-compose.yml to change ./data to your directory
# Then restart:
docker compose --profile simple up -d
```

## Production Deployment

For production environments where you only need the MCP APIs (no web UI):

**1. Create a `.env` file:**
```bash
ENABLE_WEB_UI=0
NVIDIA_API_KEY=your_nvidia_key
OPENAI_API_KEY=your_openai_key
```

**2. Update docker-compose.yml to remove web UI port:**
```yaml
ports:
  - "8080:8080"
  - "8081:8081"
  # Remove: - "8501:8501" MCP), 8081 (Connector MCP), 8501 (Web UI)
- User: `appuser` (non-root for security)
- Services: All enabled by default (can be disabled via environment variables

**3. Deploy:**
```bash
docker compose --profile simple up -d
```

## Troubleshooting

**Container not starting:**
```bash
# Check logs
docker compose logs -f

# Check container status
docker compose ps
```

**Services not responding:**
```bash
# Restart services
docker compose restart

# Or rebuild from scratch
docker compose down
docker compose --profile simple up -d --build
```

**Web UI not loading:**
```bash
# Check if web UI is enabled
docker compose exec profiler-suite env | grep ENABLE_WEB_UI

# Should show: ENABLE_WEB_UI=1
``o  ENABLE_WEB_UI=${ENABLE_WEB_UI:-1}
```

Then access at: `http://localhost:8501`

## Support

For issues or questions, refer to the main README.md or project documentation.

## Image Details

- Base Image: `python:3.11-slim`
- Size: ~1.5GB
- Exposed Ports: 8080 (Profiler), 8081 (Connector)
- User: `appuser` (non-root for security)
