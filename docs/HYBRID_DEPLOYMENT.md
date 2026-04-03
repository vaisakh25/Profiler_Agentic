# Hybrid Deployment Guide

Run **MCP servers in Docker** and **Web UI locally** for optimal development workflow.

## Quick Start

### Start Backend (Docker)
```bash
docker compose up -d
```

### Start Web UI (Local)
```bash
f:/agentic_profiler/Profiler_Agentic/.venv/Scripts/python.exe -m file_profiler.agent --web --web-port 8501 --mcp-url http://localhost:8080/sse --connector-mcp-url http://localhost:8081/sse
```

---

## Architecture

```
┌─────────────────────────────────────────┐
│           Docker Container              │
│                                         │
│  ┌─────────────────────────────────┐   │
│  │  Profiler MCP Server            │   │
│  │  Port: 8080                     │   │
│  └─────────────────────────────────┘   │
│                                         │
│  ┌─────────────────────────────────┐   │
│  │  Connector MCP Server           │   │
│  │  Port: 8081                     │   │
│  └─────────────────────────────────┘   │
│                                         │
└─────────────────────────────────────────┘
              ▲         ▲
              │         │
              │         │  HTTP/SSE
              │         │
┌─────────────┴─────────┴─────────────────┐
│        Local Machine (Host)             │
│                                         │
│  ┌─────────────────────────────────┐   │
│  │  Web UI (FastAPI + WebSocket)   │   │
│  │  Port: 8501                     │   │
│  │  Browser: http://localhost:8501 │   │
│  └─────────────────────────────────┘   │
│                                         │
└─────────────────────────────────────────┘
```

---

## Services

### In Docker (Backend)
- **Profiler MCP Server** - Port 8080
  - File profiling, relationship detection
  - Health: http://localhost:8080/health
  
- **Connector MCP Server** - Port 8081
  - Remote data sources (PostgreSQL, S3, etc.)
  - Health: http://localhost:8081/health

### Running Locally (Frontend)
- **Web UI** - Port 8501
  - Interactive chat interface
  - Direct access to codebase for debugging
  - Hot reload for frontend changes
  - URL: http://localhost:8501

---

## Why Hybrid Deployment?

✅ **Fast Backend** - MCP servers containerized, isolated, consistent  
✅ **Easy Frontend Dev** - Web UI runs locally with full access to code  
✅ **Quick Iterations** - Change frontend code without rebuilding Docker  
✅ **Better Debugging** - Direct access to logs and Python debugger  
✅ **Resource Efficient** - Only backend in Docker, lighter footprint  

---

## Commands

### Start Everything
```bash
docker compose up -d
f:/agentic_profiler/Profiler_Agentic/.venv/Scripts/python.exe -m file_profiler.agent --web --web-port 8501 --mcp-url http://localhost:8080/sse --connector-mcp-url http://localhost:8081/sse
```

### Stop Web UI
Press `Ctrl+C` in the terminal where Web UI is running

### Stop MCP Servers
```bash
docker compose down
```

### Restart MCP Servers Only
```bash
docker compose restart
```

### View Docker Logs
```bash
docker compose logs -f
```

### Check All Services
```bash
# Test Profiler MCP
curl http://localhost:8080/health

# Test Connector MCP
curl http://localhost:8081/health

# Test Web UI
curl http://localhost:8501/
```

---

## Development Workflow

### Typical Development Session

1. **Start backend services:**
   ```bash
   docker compose up -d
   ```

2. **Start frontend for development:**
   ```bash
   f:/agentic_profiler/Profiler_Agentic/.venv/Scripts/python.exe -m file_profiler.agent --web --web-port 8501 --mcp-url http://localhost:8080/sse --connector-mcp-url http://localhost:8081/sse
   ```

3. **Develop** - Make changes to Web UI code (frontend/, file_profiler/agent/)

4. **Restart Web UI** - Press Ctrl+C and run step 2 again

5. **Done?** - Stop everything:
   ```bash
   docker compose down
   ```

### Backend Development

If you need to modify MCP server code:

```bash
# Stop Docker services
docker compose down

# Rebuild with changes
docker compose up -d --build

# Web UI will reconnect automatically
```

---

## Configuration

### Environment Variables

The Web UI reads from your local `.env` file:

```bash
# Required for LLM features
NVIDIA_API_KEY=nvapi-your-key-here
OPENAI_API_KEY=sk-your-key-here
GROQ_API_KEY=gsk-your-key-here

# LLM Configuration
LLM_PROVIDER=openai
LLM_MODEL=mistralai/mistral-large-3-675b-instruct-2512
```

### Changing MCP URLs

By default, Web UI connects to:
- Profiler MCP: `http://localhost:8080/sse`
- Connector MCP: `http://localhost:8081/sse`

To change (e.g., Docker on remote host):
```bash
python -m file_profiler.agent --web --web-port 8501 \
  --mcp-url http://192.168.1.100:8080/sse \
  --connector-mcp-url http://192.168.1.100:8081/sse
```

---

## Troubleshooting

### Port Already in Use

**Error:** `Port 8501 already in use`

**Fix:**
```powershell
# Windows PowerShell
Get-NetTCPConnection -LocalPort 8501 | ForEach-Object { Stop-Process -Id $_.OwningProcess -Force }
```

### Can't Connect to MCP Servers

**Error:** `Could not connect to MCP server`

**Fix:**
```bash
# Check if Docker containers are running
docker compose ps

# Check MCP server health
curl http://localhost:8080/health
curl http://localhost:8081/health

# Restart Docker services
docker compose restart
```

### Web UI Shows "Disconnected"

**Fix:**
```bash
# 1. Check MCP servers are running
docker compose ps

# 2. Restart Web UI (Ctrl+C, then start again)
```

---

## Switching Between Modes

### Full Docker Deployment
```bash
# Stop local Web UI (Ctrl+C)
# Enable Web UI in Docker
docker compose down
# Edit docker-compose.yml: ENABLE_WEB_UI=1
docker compose up -d
```

### Local Development (Everything Local)
```bash
# Stop Docker
docker compose down

# Start all services locally
f:/agentic_profiler/Profiler_Agentic/.venv/Scripts/python.exe -m file_profiler --host 0.0.0.0 --transport sse --port 8080
f:/agentic_profiler/Profiler_Agentic/.venv/Scripts/python.exe -m file_profiler.connectors --host 0.0.0.0 --transport sse --port 8081
f:/agentic_profiler/Profiler_Agentic/.venv/Scripts/python.exe -m file_profiler.agent --web --web-port 8501 --mcp-url http://localhost:8080/sse --connector-mcp-url http://localhost:8081/sse
```

---

## Production Deployment

For production, use full Docker deployment instead:

```bash
# Edit docker-compose.yml: ENABLE_WEB_UI=1
docker compose up -d --build
```

See [DOCKER_QUICKSTART.md](DOCKER_QUICKSTART.md) for full Docker deployment.

---

## Summary

**Current Setup:**
- ✅ **Backend (Docker)**: Profiler MCP (8080) + Connector MCP (8081)
- ✅ **Frontend (Local)**: Web UI (8501)
- ✅ **Benefits**: Fast development, easy debugging, flexible

**Start Commands:** `docker compose up -d` then run the local Web UI command above.
