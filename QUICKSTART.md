# Data Profiler - Quick Start Guide

## Single Container Deployment (Simplest)

Run all services in one Docker container - perfect for local development and testing.

### Prerequisites

- Docker Desktop installed and running
- At least 4GB RAM available for Docker
- Your LLM API key (OpenAI, NVIDIA, Anthropic, etc.)

### Step 1: Configure API Keys

Create a `.env` file in the project root:

```bash
# Required: LLM Provider API Key
OPENAI_API_KEY=your_key_here
OPENAI_BASE_URL=https://integrate.api.nvidia.com/v1

# Or use NVIDIA directly
NVIDIA_API_KEY=your_nvidia_key_here

# Optional: Other providers
ANTHROPIC_API_KEY=your_key_here
GOOGLE_API_KEY=your_key_here
GROQ_API_KEY=your_key_here
```

### Step 2: Start the Container

**Windows:**
```bash
start.bat
```

**Linux/Mac:**
```bash
chmod +x start.sh
./start.sh
```

**Or manually:**
```bash
docker-compose up -d
```

### Step 3: Access the Web UI

Open your browser to: **http://localhost:8501**

The Web UI will automatically connect to both MCP servers running inside the same container.

---

## What's Running?

The single container runs three services:

| Service | Port | URL | Description |
|---------|------|-----|-------------|
| **File Profiler MCP** | 8080 | http://localhost:8080 | Profiles local CSV, Excel, Parquet, JSON files |
| **Connector MCP** | 8081 | http://localhost:8081 | Connects to PostgreSQL, Snowflake, S3, Azure, GCS |
| **Web UI** | 8501 | http://localhost:8501 | Chat interface for data profiling |

---

## Usage Examples

### In the Web Chat Interface:

1. **List available files:**
   ```
   Show me what files are in the files directory
   ```

2. **Profile a CSV file:**
   ```
   Profile the Application_Cities.csv file
   ```

3. **Detect relationships:**
   ```
   Analyze relationships between the sales tables
   ```

4. **Get quality insights:**
   ```
   What quality issues exist in the customer data?
   ```

### Default Data Location

Your data files should be in:
- **Host:** `./data/files/`
- **Container:** `/data/files/` (read-only)

The container automatically mounts `./data/files` from your project directory.

---

## Stopping the Services

```bash
docker-compose down
```

To also remove volumes (uploads and outputs):
```bash
docker-compose down -v
```

---

## Viewing Logs

```bash
# All services
docker-compose logs -f

# Specific service
docker logs profiler-suite -f
```

---

## Troubleshooting

### Container won't start
1. Check Docker is running: `docker info`
2. Check port availability: `netstat -an | findstr "8080 8081 8501"`
3. View logs: `docker-compose logs`

### "Access denied" errors
- Ensure `./data/files` directory exists
- Check file permissions on Linux/Mac

### MCP connection failures
- Wait 15-20 seconds after startup for services to initialize
- Check health: `docker exec profiler-suite python -c "import urllib.request; urllib.request.urlopen('http://localhost:8080/health')"`

### Out of memory
Increase Docker memory limit in Docker Desktop settings (recommend 8GB minimum for large files).

---

## Advanced: Environment Variables

See `docker-compose.yml` for all available configuration options:

- `MAX_PARALLEL_WORKERS=4` - Concurrent profiling tasks
- `DUCKDB_MEMORY_LIMIT=2GB` - Memory for CSV processing
- `MAX_UPLOAD_SIZE_MB=500` - Maximum file upload size
- `LLM_PROVIDER=openai` - LLM provider (openai, anthropic, google, groq)
- `LLM_MODEL=...` - Specific model to use

---

## Next Steps

- **Docker Quickstart:** See [docs/DOCKER_QUICKSTART.md](docs/DOCKER_QUICKSTART.md) for detailed Docker information
- **Architecture:** See [docs/SYSTEM_ARCHITECTURE_MASTER.md](docs/SYSTEM_ARCHITECTURE_MASTER.md) for technical details
- **Production readiness:** See [docs/DEPLOYMENT_READINESS.md](docs/DEPLOYMENT_READINESS.md) for deployment guidance
