# Docker Deployment Guide

## Quick Start

### Start MCP Servers Only (Recommended for CLI)
```bash
docker-compose --profile simple up -d
```

This starts:
- **File Profiler MCP Server** on port 8080 (13 tools)
- **Data Connector MCP Server** on port 8081 (16 tools)

### Start with Web UI (Testing/Development)
```bash
docker-compose --profile web up -d
```

This starts all 3 services:
- File Profiler MCP (port 8080)
- Data Connector MCP (port 8081)
- Web UI (port 8501)

## Services Architecture

### 1. profiler-mcp (Port 8080)
- **Purpose**: Core data profiling MCP server
- **Tools**: 13 file profiling tools
  - File discovery, CSV/Excel profiling, quality assessment
  - Column type detection, statistics, PII detection
  - Schema export, profile management
- **Healthcheck**: `http://localhost:8080/health`

### 2. connector-mcp (Port 8081)
- **Purpose**: Cloud/database connector MCP server
- **Tools**: 16 data connector tools
  - PostgreSQL, MySQL, BigQuery, Snowflake, Redshift
  - Delta Lake, S3, Azure Blob, GCS
  - Connection staging, data preview, metadata fetch
- **Healthcheck**: `http://localhost:8081/health`

### 3. web-ui (Port 8501) [Optional]
- **Purpose**: Testing interface for the agent
- **Profile**: `web` (not started by default)
- **Dependencies**: Requires both MCP servers healthy

## Configuration

### Environment Variables
Create a `.env` file in the project root with your LLM and connector credentials:

```env
# LLM Provider (choose one - Anthropic recommended)
ANTHROPIC_API_KEY=sk-ant-...
# OPENAI_API_KEY=sk-...
# GOOGLE_API_KEY=...
# GROQ_API_KEY=gsk_...

# Cloud Connectors (optional - only if using connectors)
# AWS_ACCESS_KEY_ID=...
# AWS_SECRET_ACCESS_KEY=...
# AZURE_STORAGE_CONNECTION_STRING=...
# SNOWFLAKE_ACCOUNT=...
# SNOWFLAKE_USER=...
# SNOWFLAKE_PASSWORD=...
```

The docker-compose will automatically load these as container environment variables.

### Data Volumes
- `profiler-uploads/` - Uploaded files storage (persistent)
- `profiler-output/` - Generated profiles storage (persistent)
- `./data/` - Local data directory (read-only mount)

## CLI Usage with Docker

### Option 1: Use native CLI with Docker MCP servers
```bash
# Start MCP servers
docker-compose --profile simple up -d

# Run CLI from host (recommended)
python -m file_profiler.agent --chat
```

### Option 2: Run CLI in Docker (interactive)
```bash
# Start MCP servers
docker-compose --profile simple up -d

# Run one-off CLI container
docker-compose run --rm profiler-mcp python -m file_profiler.agent --chat
```

### Option 3: CLI in separate container
```yaml
# Add to docker-compose.yml if needed
cli-agent:
  build: .
  container_name: profiler-cli
  stdin_open: true
  tty: true
  depends_on:
    profiler-mcp:
      condition: service_healthy
    connector-mcp:
      condition: service_healthy
  command: ["python", "-m", "file_profiler.agent", "--chat"]
```

## Management Commands

### View Logs
```bash
# All services
docker-compose logs -f

# Specific service
docker-compose logs -f profiler-mcp
docker-compose logs -f connector-mcp
docker-compose logs -f web-ui
```

### Check Health
```bash
# File Profiler MCP
curl http://localhost:8080/health

# Connector MCP
curl http://localhost:8081/health

# List MCP tools
curl http://localhost:8080/tools
curl http://localhost:8081/tools
```

### Stop Services
```bash
# Stop all
docker-compose down

# Stop and remove volumes
docker-compose down -v

# Stop web UI only
docker-compose stop web-ui
```

### Rebuild After Code Changes
```bash
# Rebuild all images
docker-compose build

# Rebuild specific service
docker-compose build profiler-mcp

# Rebuild and restart
docker-compose --profile simple up -d --build
```

## Troubleshooting

### MCP Servers Not Starting
```bash
# Check logs
docker-compose logs profiler-mcp
docker-compose logs connector-mcp

# Verify health
docker-compose ps
```

### CLI Can't Connect to MCP Servers
```bash
# Verify servers are healthy
curl http://localhost:8080/health
curl http://localhost:8081/health

# Check firewall/port bindings
docker-compose ps
netstat -an | findstr "8080 8081"
```

### Port Already in Use
```bash
# Find process using port
netstat -ano | findstr "8080"

# Stop existing servers
docker-compose down

# Or change ports in docker-compose.yml
ports:
  - "9080:8080"  # Use 9080 instead of 8080
```

### Web UI Not Connecting to MCP Servers
```bash
# Check if all services are up
docker-compose ps

# Verify web UI logs
docker-compose logs web-ui

# Restart with dependencies
docker-compose restart web-ui
```

## Production Considerations

For CLI-based internal tools (current use case):
- ✓ Run MCP servers in Docker: `docker-compose --profile simple up -d`
- ✓ Run CLI from host for better interactivity
- ✓ Use pinned dependencies (already in requirements.txt)
- ✓ Enable log rotation (already configured)
- ✗ Skip web UI in production (use `--profile web` only for testing)

For web-based deployment:
- See `PRODUCTION_READINESS_AUDIT.md` for full checklist
- Add HTTPS/TLS termination (nginx/traefik)
- Configure authentication/authorization
- Set up monitoring and logging aggregation
- Use managed database for persistence
- Configure resource limits in docker-compose

## Resource Limits (Optional)

Add to docker-compose.yml for production:
```yaml
services:
  profiler-mcp:
    deploy:
      resources:
        limits:
          cpus: '2'
          memory: 2G
        reservations:
          cpus: '1'
          memory: 1G
```

## Next Steps

1. **Start MCP servers**: `docker-compose --profile simple up -d`
2. **Verify health**: `curl http://localhost:8080/health && curl http://localhost:8081/health`
3. **Run CLI**: `python -m file_profiler.agent --chat`
4. **(Optional) Test Web UI**: `docker-compose --profile web up -d` → Open http://localhost:8501
