# Quick Start - Docker Deployment

## Single Command Deployment

Start the entire Agentic Profiler stack with one command:

```bash
docker compose up -d
```

That's it! 🚀

## What's Running

After running the command, you'll have:

- **🔧 Profiler MCP Server** - Port 8080
  - File profiling, relationship detection, quality analysis
  
- **🔌 Connector MCP Server** - Port 8081  
  - Remote data sources (PostgreSQL, S3, Snowflake, etc.)
  
- **🌐 Web UI** - Port 8501
  - Interactive chat interface at **http://localhost:8501**

## Quick Commands

```bash
# Start all services
docker compose up -d

# Stop all services
docker compose down

# View logs
docker compose logs -f

# Restart services
docker compose restart

# Rebuild and start
docker compose up -d --build
```

## First Time Setup

1. **Configure API Keys** (optional, for LLM features):
   
   Create a `.env` file in the project root:
   ```bash
   NVIDIA_API_KEY=nvapi-your-key-here
   OPENAI_API_KEY=sk-your-key-here
   GROQ_API_KEY=gsk-your-key-here
   ```

2. **Start Services**:
   ```bash
   docker compose up -d
   ```

3. **Access Web UI**:
   Open http://localhost:8501 in your browser

## Testing the Services

```bash
# Test Profiler MCP
curl http://localhost:8080/health

# Test Connector MCP
curl http://localhost:8081/health

# Test Web UI
curl http://localhost:8501/
```

All should return HTTP 200 OK.

## Selective Service Deployment

Run only what you need:

```bash
# MCP servers only (no web UI)
ENABLE_WEB_UI=0 docker compose up -d

# Profiler MCP only
ENABLE_CONNECTOR_MCP=0 ENABLE_WEB_UI=0 docker compose up -d
```

## Sharing the Image

Export the Docker image to share with others:

```bash
# Save as compressed file
docker save profiler_agentic-profiler-suite:latest | gzip > profiler.tar.gz

# On recipient machine, load it
gunzip -c profiler.tar.gz | docker load
docker compose up -d
```

Or push to a Docker registry:

```bash
docker tag profiler_agentic-profiler-suite:latest yourregistry/profiler-suite:latest
docker push yourregistry/profiler-suite:latest
```

## Data Persistence

Your data is automatically persisted in Docker volumes:

- `profiler-uploads` - Uploaded files
- `profiler-output` - Profiling results

To mount your own data directory, edit `docker-compose.yml`:

```yaml
volumes:
  - ./your-data-folder:/data/mounted:ro
```

## Troubleshooting

**Services not starting?**
```bash
# Check logs
docker compose logs -f

# Check status
docker compose ps
```

**Port conflicts?**
```bash
# Stop conflicting services or change ports in docker-compose.yml
ports:
  - "8080:8080"  # Change left number (host port)
  - "8081:8081"
  - "8501:8501"
```

**Need to rebuild?**
```bash
docker compose down
docker compose up -d --build --force-recreate
```

## Documentation

- Full deployment guide: [DOCKER_SHARING.md](DOCKER_SHARING.md)
- Project README: [README.md](README.md)
- Architecture docs: [MCP_ARCHITECTURE_DESIGN.md](MCP_ARCHITECTURE_DESIGN.md)

## Support

For issues or questions, refer to the main project documentation or check container logs:

```bash
docker compose logs -f profiler-suite
```
