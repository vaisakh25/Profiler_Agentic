# Data Profiler - Agentic File & Database Profiling System

An AI-powered data profiling system that analyzes CSV, Excel, Parquet, JSON files and remote databases (PostgreSQL, Snowflake, S3, Azure, GCS) using MCP (Model Context Protocol) servers.

---

## 🚀 Quick Start

### Prerequisites
- Docker Desktop installed and running
- LLM API key (OpenAI, NVIDIA, Anthropic, Google, or Groq)
- 4GB+ RAM available for Docker

### 1. Set Up Environment
Create a `.env` file:
```bash
# Required
OPENAI_API_KEY=your_key_here
OPENAI_BASE_URL=https://integrate.api.nvidia.com/v1
NVIDIA_API_KEY=your_nvidia_key

# Optional: Other providers
ANTHROPIC_API_KEY=your_anthropic_key
GOOGLE_API_KEY=your_google_key
GROQ_API_KEY=your_groq_key
```

### 2. Choose Your Deployment Mode

#### Simple Mode (Recommended for Development)
All services in one container - fastest and easiest:

```bash
# Windows
start.bat

# Linux/Mac
chmod +x start.sh && ./start.sh

# Or manually
docker-compose up -d
```

Access: **http://localhost:8501**

#### Multi-Container Mode (Recommended for Production)
Separate containers with Nginx - fault isolation and scaling:

```bash
# Windows
start-multi.bat

# Linux/Mac
chmod +x start-multi.sh && ./start-multi.sh

# Or manually
docker-compose --profile multi up -d
```

Access: **http://localhost:9050**

### Kubernetes / Multi Profile MCP URLs

Use these values in deployment forms (or environment variables) when routing through Nginx on port 9050:

| Variable | Value |
|---------|-------|
| `WEB_MCP_URL` | `http://localhost:9050/mcp/file/sse` |
| `WEB_CONNECTOR_MCP_URL` | `http://localhost:9050/mcp/connector/sse` |
| `MCP_PORT` | `8080` (internal only) |
| `CONNECTOR_MCP_PORT` | `8081` (internal only) |
| `WEB_PORT` | `8501` (internal only) |

`APP_PORT` is not required for this deployment model.

### Verification Sequence

```bash
# Multi profile (single-port)
docker compose --profile multi up -d
curl http://localhost:9050/health
curl http://localhost:9050/mcp/file/health
curl http://localhost:9050/mcp/connector/health
curl http://localhost:9050/

# Simple profile regression
docker compose --profile multi down --remove-orphans
docker compose --profile simple up -d
curl http://localhost:8080/health
curl http://localhost:8081/health
```

---

## 📖 Documentation

### Getting Started
- **[Quick Start](QUICKSTART.md)** - Beginner-friendly guide
- **[Docker Quickstart](docs/DOCKER_QUICKSTART.md)** - Docker-first setup and troubleshooting ⭐
- **[Project Docs Index](docs/README.md)** - Full technical documentation

### Deployment
- **[Docker Compose Profiles](docker-compose.yml)** - Single file for `simple` and `multi` modes
- **[Docker Sharing](docs/DOCKER_SHARING.md)** - Image/export and distribution guidance
- **[Deployment Readiness](docs/DEPLOYMENT_READINESS.md)** - Operational readiness checklist

### Architecture
- **[System Architecture](docs/SYSTEM_ARCHITECTURE_MASTER.md)** - Technical deep dive
- **[MCP Architecture](docs/MCP_ARCHITECTURE_DESIGN.md)** - MCP protocol design
- **[File Profiling Architecture](docs/FILE_PROFILING_ARCHITECTURE.md)** - Profiling pipeline

### Production
- **[Production Readiness](docs/PRODUCTION_READINESS_AUDIT.md)** - Deployment checklist
- **[Security Improvements](docs/SECURITY_IMPROVEMENTS_APPLIED.md)** - Security measures

---

## 🎯 Features

### Data Profiling
- ✅ **CSV, Excel, Parquet, JSON, SQLite, DuckDB** files
- ✅ **Automatic type inference** and statistics
- ✅ **Quality checks** (nulls, duplicates, outliers)
- ✅ **Relationship detection** (foreign keys, joins)
- ✅ **LLM-powered enrichment** (descriptions, insights)
- ✅ **Vector search** for semantic queries

### Remote Data Sources
- ✅ **PostgreSQL** databases
- ✅ **Snowflake** data warehouse
- ✅ **AWS S3** buckets
- ✅ **Azure Data Lake Storage**
- ✅ **Google Cloud Storage**

### AI Chat Interface
- ✅ **Natural language** data exploration
- ✅ **Multi-turn conversations** with context
- ✅ **Real-time progress** tracking
- ✅ **Interactive visualizations**
- ✅ **Session management**

---

## 🏗️ Architecture

###  Simple Mode (Single Container)
```
┌─────────────────────────────────────┐
│      profiler-suite Container       │
│                                     │
│  ┌──────────┐  ┌──────────┐       │
│  │ MCP File │  │ MCP Conn │       │
│  │   :8080  │  │   :8081  │       │
│  └──────────┘  └──────────┘       │
│                                     │
│         ┌──────────┐               │
│         │  Web UI  │               │
│         │  :8501   │               │
│         └──────────┘               │
└─────────────────────────────────────┘
```

### Multi-Container Mode (Production)
```
                     ┌─────────────────────┐
   User Browser ────▶│  Nginx (:9050)     │
                     └──────────┬──────────┘
                                │
              ┌─────────────────┼─────────────────┐
              ▼                 ▼                 ▼
      ┌──────────────┐  ┌──────────────┐  ┌──────────────┐
      │   Web UI     │  │  MCP File    │  │ MCP Connector│
      │  (Internal)  │  │  (Internal)  │  │  (Internal)  │
      └──────────────┘  └──────────────┘  └──────────────┘
```

---

## 📊 Usage Examples

### Via Web Chat Interface

1. **List available files:**
   ```
   Show me what data files are available
   ```

2. **Profile a specific file:**
   ```
   Profile the Sales_Customers.csv file
   ```

3. **Detect relationships:**
   ```
   Find relationships between the sales and customer tables
   ```

4. **Quality analysis:**
   ```
   What quality issues exist in the application_people dataset?
   ```

5. **Semantic search:**
   ```
   Which columns contain email addresses?
   ```

### Default Data
Sample CSV files are in `./data/files/`:
- Application_Cities.csv
- Sales_Customers.csv
- Purchasing_Suppliers.csv
- And more...

---

## 🔄 Switching Modes

### Simple to Multi:
```bash
docker-compose down
docker-compose --profile multi up -d
# URL: http://localhost:8501 → http://localhost:9050
```

### Multi to Simple:
```bash
docker-compose --profile multi down
docker-compose up -d
# URL: http://localhost:9050 → http://localhost:8501
```

---

## 🛠️ Development

### Project Structure
```
.
├── docker-compose.yml          # Unified deployment file ⭐
├── Dockerfile                  # Application image
├── docker_entrypoint.py        # Service orchestrator
├── nginx.conf                  # Reverse proxy config
├── file_profiler/              # Main application
│   ├── mcp_server.py          # File profiling MCP
│   ├── connector_mcp_server.py # Remote data MCP
│   ├── agent/                 # AI chat agent
│   ├── profiling/             # Profiling engine
│   ├── connectors/            # Data source connectors
│   └── ...
├── frontend/                   # Web UI (HTML/JS)
└── tests/                      # Test suite
```

### Running Tests
```bash
# Activate virtual environment
.venv\Scripts\activate         # Windows
source .venv/bin/activate      # Linux/Mac

# Run tests
pytest
```

---

## 🐛 Troubleshooting

### Container Won't Start
```bash
# Check logs
docker-compose logs -f                  # Simple mode
docker-compose --profile multi logs -f  # Multi mode

# Check status
docker-compose ps
```

### Port Conflicts
Simple mode uses ports 8080, 8081, 8501. If these are in use:
```yaml
# Edit docker-compose.yml, change:
ports:
  - "18080:8080"   # Use different external port
```

### Access Denied Errors
Ensure `./data/files` directory exists:
```bash
mkdir -p data/files
```

### Out of Memory
Increase Docker memory in Docker Desktop settings (recommend 8GB).

---

## 📝 License

[Include your license here]

---

## 🤝 Contributing

[Include contribution guidelines here]

---

## 📧 Support

For issues or questions:
- Check [docs/DOCKER_QUICKSTART.md](docs/DOCKER_QUICKSTART.md) for detailed troubleshooting
- Review [Documentation](#-documentation) section
- Open an issue on GitHub

---

## ⭐ Key Commands

```bash
# Simple Mode (Default)
docker-compose up -d              # Start
docker-compose down               # Stop
docker-compose logs -f            # Logs

# Multi Mode (Production)
docker-compose --profile multi up -d     # Start
docker-compose --profile multi down      # Stop
docker-compose --profile multi logs -f   # Logs

# Convenience
./start.bat | ./start.sh          # Simple mode script
./start-multi.bat | ./start-multi.sh   # Multi mode script

# Cleanup
docker-compose down -v --remove-orphans   # Full cleanup
```

---

**🎉 Everything in ONE file! Choose simple or multi mode with just a flag.**

Start with simple mode, scale to multi when needed. No complexity, maximum flexibility!
