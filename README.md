# Agentic Data Profiler

A production-grade data profiling engine exposed as an **MCP (Model Context Protocol) server** with an **interactive LangGraph chatbot**, **web UI**, **LLM-powered enrichment**, and **multi-source connector framework**. Profile CSV, Parquet, JSON, Excel files and remote sources (S3, ADLS Gen2, GCS, Snowflake, PostgreSQL) — detect schemas, infer types, assess quality, discover cross-table foreign key relationships, and get AI-generated descriptions, join recommendations, and enriched ER diagrams.

## Key Features

- **11-layer profiling pipeline** — intake validation, content-sniffing format detection, memory-safe size strategy, format-specific engines (CSV, Parquet, JSON, Excel, DuckDB/SQLite), column standardization, type inference with confidence scoring, structural quality checks, and cross-table relationship detection.
- **Dual MCP servers** — File Profiler server (13 tools, port 8080) for local file profiling + Data Connector server (16 tools, port 8081) for remote sources. Both independently deployable. Connect from LangGraph, Claude Desktop, Claude Code, or any MCP client. Supports stdio, SSE, and streamable-http transports.
- **Interactive chatbot** — multi-turn conversational interface powered by LangGraph. Point it at a folder and get profiling results, ER diagrams, and enriched analysis through natural language.
- **Web UI** — FastAPI + WebSocket backend with real-time progress tracking, live stats, chart rendering, Mermaid ER diagrams, drag-and-drop file upload, connection management modal, dark/light themes, and session history with PostgreSQL persistence.
- **LLM enrichment (Map-Reduce + RAG)** — five-phase pipeline: MAP (per-table LLM summaries), APPLY (write descriptions back to profiles), EMBED (ChromaDB with persistent fingerprinting), DISCOVER+CLUSTER (column-affinity matrix + DBSCAN clustering), REDUCE (synthesized LLM analysis), and META-REDUCE (optional cross-cluster synthesis).
- **Multi-source connectors** — profile remote data via URI-based routing: `s3://`, `abfss://`, `gs://`, `snowflake://`, `postgresql://`. DuckDB as the universal connectivity layer with native SDK fallback.
- **Secure credential management** — credentials bypass the LLM entirely, flowing directly from the UI to REST endpoints. Fernet encryption at rest (PROFILER_SECRET_KEY), encrypted file persistence, environment variable fallback, SDK default chain support.
- **Multi-provider LLM support** — Anthropic, OpenAI, Google, and Groq with automatic fallback chains. Separate stronger model configurable for REDUCE/META-REDUCE phases.
- **Chart generation** — matplotlib/seaborn charts (null distribution, type distribution, cardinality, completeness, quality heatmap, relationship confidence, and more) with dark/light themes.
- **Chat persistence** — PostgreSQL-backed session checkpointing (PostgresSaver) with MemorySaver fallback. Full conversation history restored on session resume.
- **Progress tracking** — animated spinner with elapsed time, weighted progress bar, rotating stage hints, smart result summaries, real-time IPC for web UI progress updates, and per-table progress cards during enrichment.
- **Format-agnostic output** — identical JSON profile schema regardless of source format (CSV, Parquet, JSON, Excel, remote).
- **Memory-safe** — three-tier read strategy (MEMORY_SAFE / LAZY_SCAN / STREAM_ONLY) auto-selected based on file size. Handles multi-GB files without OOM.
- **Content sniffing** — never trusts file extensions. Detects format via magic bytes and structural analysis.
- **Containerized** — Dockerfile and docker-compose included. Deploy on Docker, Cloud Run, ECS, Azure Container Apps, or Kubernetes.

## Architecture

```
                      User / Browser
                           │
              ┌────────────┴────────────┐
              │                         │
    ┌─────────┴─────────┐    ┌─────────┴─────────┐
    │   Web UI           │    │   CLI Chatbot      │
    │   (FastAPI +       │    │   (Terminal)        │
    │    WebSocket)       │    │                    │
    │                    │    │                    │
    │  REST /api/        │    │                    │
    │  connections ──────┼────┼──► ConnectionManager│
    │  (creds bypass LLM)│    │    + CredentialStore│
    └─────────┬─────────┘    └─────────┬──────────┘
              │                         │
              └────────────┬────────────┘
                           │
                ┌──────────┴──────────┐
                │   LangGraph Agent    │  ← ReAct-style agent loop
                │   (multi-turn chat)  │     with PostgreSQL checkpointing
                │   MultiServerMCP     │     graceful degradation
                └──────────┬──────────┘
                           │ MCP protocol (SSE / stdio / streamable-http)
          ┌────────────────┴────────────────┐
          │                                 │
┌─────────┴─────────┐            ┌─────────┴─────────┐
│  File Profiler     │            │  Data Connector    │
│  MCP Server :8080  │            │  MCP Server :8081  │
│  13 tools          │            │  16 tools          │
│  2 resources       │            │  2 resources       │
│  3 prompts         │            │  3 prompts         │
└─────────┬─────────┘            └─────────┬─────────┘
          │                                 │
  ┌───────┼───────┐               ┌────────┴────────┐
  │       │       │               │  Connector       │
┌─┴──┐ ┌──┴──┐ ┌──┴──┐          │  Framework       │
│Det.│ │Rel. │ │LLM  │          │  S3│ADLS│GCS│    │
│Pipe│ │Det. │ │Enr. │          │  SF │ PG        │
│line│ │     │ │     │          │  + Staging Dir   │
└────┘ └─────┘ └─────┘          └─────────────────┘
```

## Quick Start

### Prerequisites

- Python 3.11+
- pip

### Install

```bash
# Clone the repository
git clone <repo-url>
cd Profiler_Agentic

# Install in editable mode
pip install -e ".[dev]"
```

### Run the MCP Servers

The system uses two independent MCP servers: one for local file profiling and one for remote data connectors. The connector server is optional — the agent gracefully degrades to file-profiler-only mode if it's unavailable.

```bash
# File Profiler server (required)
python -m file_profiler --transport sse --host 0.0.0.0 --port 8080

# Data Connector server (optional — for remote sources)
python -m file_profiler.connectors --transport sse --host 0.0.0.0 --port 8081

# stdio transport (local — for Claude Desktop, Claude Code)
python -m file_profiler --transport stdio
python -m file_profiler.connectors --transport stdio
```

### Run the Interactive Chatbot

Start the MCP server(s) in one terminal (SSE transport), then in another:

```bash
# Default provider
python -m file_profiler.agent --chat

# Specify provider and model
python -m file_profiler.agent --chat --provider google --model gemini-2.5-flash

# Custom MCP server URL
python -m file_profiler.agent --chat --mcp-url http://localhost:8080/sse
```

### Run the Web UI

```bash
# Start file-profiler MCP server (Terminal 1)
python -m file_profiler --transport sse --host 0.0.0.0 --port 8080

# Start connector MCP server (Terminal 2 — optional)
python -m file_profiler.connectors --transport sse --host 0.0.0.0 --port 8081

# Start web server (Terminal 3)
python -m file_profiler.agent --web --web-port 8501
```

The web UI provides:
- Real-time progress bar with percentage and current step
- Live stats dashboard (tables, rows, columns, FKs, elapsed time)
- Per-table preview cards with expandable column details
- Inline Mermaid ER diagram rendering with zoom/pan controls
- Generated chart images (dark/light themes)
- Drag-and-drop file upload (up to 500 MB)
- **Connection management modal** — register/test/remove remote data source credentials (S3, ADLS, GCS, Snowflake, PostgreSQL)
- LLM provider selection (Google, Groq, Anthropic, OpenAI)
- Dark/light theme toggle
- Session history with conversation restore

### Run the Autonomous Agent

```bash
python -m file_profiler.agent --data-path ./data/files --provider google
```

### Use as a Python Library

```python
from file_profiler import profile_file, profile_directory, analyze_relationships

# Profile a single file
profile = profile_file("data/customers.csv")
print(profile.row_count, len(profile.columns))

# Profile all files in a directory
profiles = profile_directory("data/", parallel=True)

# Detect cross-table relationships
report = analyze_relationships(profiles)
for fk in report.candidates:
    print(f"{fk.fk.table_name}.{fk.fk.column_name} → "
          f"{fk.pk.table_name}.{fk.pk.column_name} "
          f"(confidence: {fk.confidence:.2f})")
```

## Docker Deployment

### Build and Run

```bash
# Using docker compose
docker compose up -d

# Server available at http://localhost:8080/sse
```

### Connect Your Agent

Point your MCP client to both SSE endpoints:

```json
{
  "mcpServers": {
    "file-profiler": {
      "url": "http://localhost:8080/sse"
    },
    "data-connector": {
      "url": "http://localhost:8081/sse"
    }
  }
}
```

### Volume Mounts

Place your data files in the `./data` directory. They are mounted read-only at `/data/mounted` inside the container. Alternatively, use the `upload_file` tool to send files via base64.

## MCP Tools Reference

### File Profiler Server (port 8080) — 13 tools

| Tool | Description |
|------|-------------|
| `profile_file(file_path)` | Profile a single file through the full 11-layer pipeline. |
| `profile_directory(dir_path, parallel)` | Profile all supported files in a directory. |
| `detect_relationships(dir_path, confidence_threshold)` | Detect FK relationships across tables (deterministic scoring). |
| `enrich_relationships(dir_path, provider, model)` | Full Map-Reduce RAG + LLM enrichment pipeline. |
| `check_enrichment_status(dir_path)` | Fast fingerprint-based check if enrichment is already complete. |
| `visualize_profile(chart_type, table_name, column_name, theme)` | Generate matplotlib/seaborn charts (12+ chart types). |
| `list_supported_files(dir_path)` | List files the profiler can handle (intake + classification only). |
| `upload_file(file_name, file_content_base64)` | Upload a base64-encoded file to the server. |
| `get_quality_summary(file_path)` | Get quality summary for a file. |
| `query_knowledge_base(question, top_k)` | Semantic search over the ChromaDB vector store. |
| `get_table_relationships(table_name)` | Get all relationships for a specific table. |
| `compare_profiles(dir_path)` | Detect schema drift vs previously profiled state. |
| `reset_vector_store()` | Clear ChromaDB and caches for fresh enrichment. |

### Data Connector Server (port 8081) — 16 tools

| Tool | Description |
|------|-------------|
| `connect_source(connection_id, scheme, credentials)` | Register credentials for a remote data source. |
| `list_connections()` | List all registered remote connections with status. |
| `test_connection(connection_id)` | Test connectivity for a registered connection. |
| `remove_connection(connection_id)` | Remove a connection and its stored credentials. |
| `list_schemas(uri, connection_id)` | List schemas in a remote database. |
| `list_tables(uri, connection_id)` | List tables/files at a remote source without profiling. |
| `profile_remote_source(uri, connection_id, table_filter)` | Profile remote tables — materialises to staging directory. |
| `remote_detect_relationships(connection_id)` | Detect FK relationships across staged remote tables. |
| `remote_enrich_relationships(connection_id, provider, model)` | Full Map-Reduce enrichment on remote data. |
| `remote_check_enrichment_status(connection_id)` | Check if enrichment is complete for a connection. |
| `remote_reset_vector_store(connection_id)` | Clear caches for remote data. |
| `remote_visualize_profile(chart_type, table_name, connection_id)` | Generate charts for remote profiled data. |
| `remote_get_quality_summary(table_name)` | Quality summary for a remote table. |
| `remote_query_knowledge_base(question, top_k)` | Semantic search over remote profiled data. |
| `remote_get_table_relationships(table_name, connection_id)` | Get relationships for a remote table. |
| `remote_compare_profiles(connection_id)` | Schema drift detection for remote data. |

## REST API Endpoints

These endpoints handle credentials securely — they bypass the LLM entirely.

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/connections` | List all connections (no secrets in response) |
| `POST` | `/api/connections` | Register a new connection with credentials |
| `DELETE` | `/api/connections/{id}` | Remove a stored connection |
| `POST` | `/api/connections/{id}/test` | Test connectivity and measure latency |
| `GET` | `/api/sessions` | List recent chat sessions |
| `POST` | `/api/sessions` | Create/update a session |
| `DELETE` | `/api/sessions/{id}` | Delete a session |
| `POST` | `/api/upload` | Upload a file via multipart form |

## Pipeline Layers

| Layer | Module | Purpose |
|-------|--------|---------|
| 1 | `intake/validator.py` | File existence, encoding detection (BOM + chardet), compression detection, delimiter sniffing |
| 2 | `classification/classifier.py` | Content-sniffing format detection via magic bytes (Parquet, Excel, JSON, CSV) |
| 3 | `strategy/size_strategy.py` | Auto-select MEMORY_SAFE (<100 MB), LAZY_SCAN (100 MB-2 GB), or STREAM_ONLY (>2 GB) |
| 4 | `engines/csv_engine.py` | CSV structure detection, header detection, row counting, sampling (Vitter's Algorithm R) |
| 5 | `engines/parquet_engine.py` | Parquet metadata reading, schema flattening, column-pruned row-group iteration |
| 5 | `engines/json_engine.py` | JSON shape detection (single object, array, NDJSON, deep nested), union schema discovery, flatten strategies |
| 5 | `engines/excel_engine.py` | Excel sheet detection, named range handling, row sampling |
| 5 | `engines/db_engine.py` | DuckDB/SQLite database engine — multi-table profiling |
| 6.5 | `standardization/normalizer.py` | Name normalization, null sentinel detection, boolean unification, numeric cleaning |
| 7 | `profiling/column_profiler.py` | Statistics: null count, distinct count, min/max, cardinality, top-N values, string length distribution |
| 7.5 | `profiling/type_inference.py` | Type detection with 90% confidence threshold (INTEGER, FLOAT, DATE, TIMESTAMP, UUID, BOOLEAN, CATEGORICAL, FREE_TEXT, STRING) |
| 8 | `quality/structural_checker.py` | Quality flags: duplicate columns, fully null, constant, high null ratio, column shift errors, encoding inconsistency |
| 9 | `analysis/relationship_detector.py` | Cross-table FK scoring: name similarity (0.50), type compatibility (0.20), cardinality (0.25), value overlap (0.15) |
| 11 | `output/profile_writer.py` | Atomic JSON serialization with QualitySummary computation |

## Project Structure

```
Profiler_Agentic/
├── file_profiler/                  # Main package
│   ├── __init__.py                 # Public API exports
│   ├── __main__.py                 # python -m file_profiler entry point
│   ├── main.py                     # Pipeline orchestrator (local + remote)
│   ├── mcp_server.py               # File Profiler MCP server (13 tools, :8080)
│   ├── connector_mcp_server.py     # Data Connector MCP server (16 tools, :8081)
│   │
│   ├── agent/                      # LangGraph agent + chatbot + web UI
│   │   ├── __init__.py
│   │   ├── __main__.py             # python -m file_profiler.agent entry point
│   │   ├── chatbot.py              # Interactive multi-turn chatbot with streaming
│   │   ├── graph.py                # ReAct-style StateGraph (agent ↔ tools loop)
│   │   ├── cli.py                  # Autonomous / human-in-the-loop CLI runner
│   │   ├── state.py                # AgentState TypedDict with message history
│   │   ├── llm_factory.py          # Multi-provider LLM factory (Google, Groq, OpenAI, Anthropic)
│   │   ├── enrichment.py           # RAG document builder (schemas + samples → Documents)
│   │   ├── enrichment_mapreduce.py # 5-phase Map-Reduce enrichment pipeline
│   │   ├── enrichment_progress.py  # IPC progress/manifest files for enrichment tracking
│   │   ├── vector_store.py         # ChromaDB persistent vector store with fingerprinting
│   │   ├── web_server.py           # FastAPI + WebSocket backend + REST API for connections
│   │   ├── session_manager.py      # PostgreSQL session persistence
│   │   └── progress.py             # Terminal progress tracking (spinner, bar, summaries)
│   │
│   ├── connectors/                 # Multi-source connector framework
│   │   ├── __init__.py             # Public API (parse_uri, registry, ConnectionManager)
│   │   ├── __main__.py             # python -m file_profiler.connectors entry point
│   │   ├── base.py                 # SourceDescriptor, BaseConnector ABC, RemoteObject
│   │   ├── uri_parser.py           # URI parsing (s3://, abfss://, gs://, snowflake://, postgresql://)
│   │   ├── registry.py             # ConnectorRegistry with lazy loading
│   │   ├── connection_manager.py   # Credential store + resolution (stored → env → SDK defaults)
│   │   ├── credential_store.py     # Fernet encryption at rest (PROFILER_SECRET_KEY)
│   │   ├── cloud_storage.py        # S3/ADLS/GCS connector (DuckDB + native SDK listing)
│   │   ├── database.py             # PostgreSQL/Snowflake connector (DuckDB + native SDK)
│   │   └── duckdb_remote.py        # DuckDB remote connection + extension management
│   │
│   ├── analysis/                   # Cross-table relationship detection
│   ├── classification/             # Format detection via content sniffing
│   ├── config/
│   │   ├── settings.py             # Pipeline tuning constants
│   │   ├── env.py                  # Environment-based deployment config
│   │   └── database.py             # PostgreSQL checkpointer + pool management
│   ├── engines/                    # Format-specific profiling engines
│   │   ├── csv_engine.py           # CSV/TSV/PSV with ZIP and gzip support
│   │   ├── parquet_engine.py       # Parquet with nested struct flattening
│   │   ├── json_engine.py          # JSON/NDJSON with shape detection
│   │   ├── excel_engine.py         # Excel (.xlsx, .xls) with sheet detection
│   │   ├── db_engine.py            # DuckDB/SQLite multi-table engine
│   │   └── duckdb_sampler.py       # DuckDB-accelerated sampling for large files
│   ├── intake/                     # File validation and encoding detection
│   ├── models/                     # Data classes and enums
│   │   ├── file_profile.py         # FileProfile (+ source_uri, connection_id for remote)
│   │   ├── relationships.py        # ForeignKeyCandidate, RelationshipReport
│   │   └── enums.py                # FileFormat, QualityFlag, SizeStrategy, SourceType
│   ├── output/                     # JSON serialization, ER diagrams, chart generation
│   │   ├── profile_writer.py
│   │   ├── er_diagram_writer.py
│   │   └── chart_generator.py      # matplotlib/seaborn chart pipeline
│   ├── profiling/                  # Column profiling and type inference
│   ├── quality/                    # Structural quality checks
│   ├── standardization/            # Data normalization
│   ├── strategy/                   # Size-based read strategy selection
│   └── utils/                      # File resolver (local + remote), logging setup
│
├── frontend/                       # Web UI
│   ├── index.html                  # Main HTML (sidebar + chat + connection modal)
│   ├── app.js                      # WebSocket client, progress, charts, connection management
│   └── style.css                   # Dark/light theme styles + modal styles
│
├── tests/                          # Test suite
├── data/                           # Sample data and output profiles
├── .env                            # Environment configuration
├── FILE_PROFILING_ARCHITECTURE.md  # Detailed architecture documentation
├── CURRENT_SYSTEM_DESIGN.md        # Deterministic pipeline design
├── MCP_ARCHITECTURE_DESIGN.md      # MCP server design
├── pyproject.toml                  # Package metadata and dependencies
├── Dockerfile                      # Container image definition
├── docker-compose.yml              # Orchestration with volumes
└── requirements.txt                # Dependency pinning
```

## LLM Enrichment (Map-Reduce + RAG)

The `enrich_relationships` tool runs a multi-phase Map-Reduce pipeline on top of the deterministic profiling results:

```
Deterministic Pipeline Output
        │
        ▼
Phase 0: PROFILE + DETECT ─→ Full pipeline + FK candidates
        │
        ▼
Phase 1: MAP ──────────────→ Per-table LLM summaries + column descriptions
        │                      (parallel, configurable workers)
        ▼
Phase 2: APPLY ────────────→ Write descriptions back into profile JSONs
        │
        ▼
Phase 3: EMBED ────────────→ ChromaDB Vector Store
        │                      (all-MiniLM-L6-v2, persistent with fingerprinting)
        ▼
Phase 4: DISCOVER ─────────→ Table-to-table affinity matrix
  + CLUSTER                    (column embedding similarity, DBSCAN clustering)
        │                      Derives new FK candidates from clusters
        ▼
Phase 5: REDUCE ───────────→ Synthesized LLM analysis
        │                      (vector-discovered relationships prioritized)
        │                      (configurable stronger model for REDUCE)
        ▼
Phase 6: META-REDUCE ──────→ Optional per-cluster + cross-cluster synthesis
                               (for large datasets with many tables)
```

### Persistent vector store

The ChromaDB store persists at `OUTPUT_DIR/chroma_store` with table fingerprinting (table_name + row_count + col_count hash). On subsequent runs, only changed tables are re-embedded. An enrichment manifest file (`.enrichment_manifest.json`) tracks completion state across restarts.

## Multi-Source Connector Framework

### Supported Sources

| Source | URI Scheme | Connector | DuckDB Extension |
|--------|-----------|-----------|-----------------|
| AWS S3 | `s3://bucket/path` | CloudStorageConnector | `httpfs` |
| Azure ADLS Gen2 | `abfss://container@account.dfs.core.windows.net/path` | CloudStorageConnector | `azure` |
| Google Cloud Storage | `gs://bucket/path` | CloudStorageConnector | `httpfs` |
| Snowflake | `snowflake://account/database/schema` | DatabaseConnector | Native SDK only |
| PostgreSQL | `postgresql://host:port/dbname` | DatabaseConnector | `postgres_scanner` |

### Credential Resolution Priority

1. **Explicit connection_id** → stored encrypted credentials
2. **Environment variables** → scheme-specific defaults (AWS_ACCESS_KEY_ID, etc.)
3. **SDK default chains** → boto3 credential chain, ADC for GCS, etc.

### Security Model

```
Frontend Connection Modal
        │
        ▼  POST /api/connections  (REST — NOT through LLM/chat)
   web_server.py
        │
        ▼  ConnectionManager.register()
   connection_manager.py
        │
        ▼  CredentialStore.encrypt_credentials()
   credential_store.py  (Fernet symmetric encryption)
        │
        ▼  SHA256(PROFILER_SECRET_KEY) → Fernet key
   .connections.enc  (double-encrypted on disk)
```

**Key security properties:**
- Credentials NEVER pass through the LLM
- Credentials NEVER appear in chat history
- Credentials NEVER stored in LangGraph checkpoints
- Encrypted at rest with Fernet (PROFILER_SECRET_KEY)
- REST API responses never include credential values

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PROFILER_DATA_DIR` | `./data/files` | Root directory for data files |
| `PROFILER_UPLOAD_DIR` | `./data/uploads` | Upload storage directory |
| `PROFILER_OUTPUT_DIR` | `./data/output` | Profile output directory |
| `PROFILER_VECTOR_STORE_DIR` | `{OUTPUT_DIR}/chroma_store` | ChromaDB persistence directory |
| `PROFILER_SECRET_KEY` | — | Passphrase for Fernet credential encryption |
| `MAX_UPLOAD_SIZE_MB` | `500` | Maximum upload file size |
| `UPLOAD_TTL_HOURS` | `1` | Upload file retention period |
| `MCP_TRANSPORT` | `sse` | Transport protocol (`stdio`, `sse`, `streamable-http`) |
| `MCP_HOST` | `0.0.0.0` | Server bind host |
| `MCP_PORT` | `8080` | File Profiler server bind port |
| `CONNECTOR_MCP_PORT` | `8081` | Data Connector server bind port |
| `LOG_LEVEL` | `INFO` | Logging level |

#### LLM Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `LLM_PROVIDER` | `google` | LLM provider: `google`, `groq`, `openai`, `anthropic` |
| `LLM_MODEL` | (per provider) | Model override |
| `REDUCE_LLM_PROVIDER` | — | Separate provider for REDUCE/META-REDUCE phases |
| `REDUCE_LLM_MODEL` | — | Separate model for REDUCE/META-REDUCE phases |
| `GOOGLE_API_KEY` | — | Required for Google/Gemini provider |
| `GROQ_API_KEY` | — | Required for Groq provider |

#### Database / Chat Persistence

| Variable | Default | Description |
|----------|---------|-------------|
| `POSTGRES_HOST` | `localhost` | PostgreSQL host for chat persistence |
| `POSTGRES_PORT` | `5432` | PostgreSQL port |
| `POSTGRES_USER` | `profiler` | PostgreSQL user |
| `POSTGRES_PASSWORD` | — | PostgreSQL password |
| `POSTGRES_DB` | `profiler` | PostgreSQL database |

#### Remote Connectors

| Variable | Description |
|----------|-------------|
| `AWS_ACCESS_KEY_ID` | AWS access key (S3) |
| `AWS_SECRET_ACCESS_KEY` | AWS secret key (S3) |
| `AWS_DEFAULT_REGION` | AWS region (default: us-east-1) |
| `AZURE_STORAGE_CONNECTION_STRING` | Azure storage connection string |
| `AZURE_TENANT_ID` / `AZURE_CLIENT_ID` / `AZURE_CLIENT_SECRET` | Azure service principal |
| `GOOGLE_APPLICATION_CREDENTIALS` | GCS service account JSON path |
| `SNOWFLAKE_ACCOUNT` / `SNOWFLAKE_USER` / `SNOWFLAKE_PASSWORD` | Snowflake credentials |
| `PROFILER_PG_CONNSTRING` | PostgreSQL profiling target connection string |
| `CONNECTOR_TIMEOUT` | Remote connector timeout in seconds (default: 30) |

#### Enrichment Tuning

| Variable | Default | Description |
|----------|---------|-------------|
| `ENRICHMENT_MAP_WORKERS` | `8` | Parallel workers for MAP phase |
| `ENRICHMENT_MAP_TOKEN_BUDGET` | `2000` | Token limit per MAP summary |
| `ENRICHMENT_REDUCE_TOP_K` | `15` | Vector search results per table in REDUCE |
| `ENRICHMENT_REDUCE_TOKEN_BUDGET` | `12000` | Token limit for REDUCE prompt |
| `COLUMN_AFFINITY_THRESHOLD` | `0.65` | Vector similarity threshold for affinity matrix |

## Supported Formats

| Format | Status | Engine |
|--------|--------|--------|
| CSV (including .tsv, .dat, .psv) | Supported | `csv_engine.py` |
| Parquet (.parquet, .pq, .parq) | Supported | `parquet_engine.py` |
| Gzip-compressed CSV | Supported | Transparent decompression |
| ZIP archives (single or multi-CSV) | Supported | Partition-aware profiling |
| JSON / NDJSON | Supported | `json_engine.py` |
| Excel (.xlsx, .xls) | Supported | `excel_engine.py` |
| DuckDB / SQLite (.duckdb, .db, .sqlite) | Supported | `db_engine.py` |
| Remote S3/ADLS/GCS files | Supported | `cloud_storage.py` + DuckDB |
| Remote PostgreSQL tables | Supported | `database.py` + DuckDB postgres_scanner |
| Remote Snowflake tables | Supported | `database.py` + native SDK |

## Dependencies

### Core Pipeline
- `pyarrow` — Parquet engine
- `chardet` — Encoding detection
- `duckdb` — Accelerated sampling + remote data access
- `mcp[cli]` — MCP server framework

### Agent + Chatbot
- `langgraph` — Agent graph framework
- `langchain-core` — Message types and base classes
- `langchain-mcp-adapters` — MCP client for LangChain tools
- `langgraph-checkpoint-postgres` — PostgreSQL chat persistence
- `fastapi` + `uvicorn` — Web server for web UI
- `websockets` — WebSocket support

### RAG Enrichment
- `chromadb` — Vector store (persistent)
- `langchain-chroma` — LangChain ChromaDB integration
- `langchain-huggingface` / `sentence-transformers` — Local embeddings (all-MiniLM-L6-v2)

### Credential Security
- `cryptography` — Fernet symmetric encryption for credential storage

### LLM Providers
- `langchain-google-genai` — Google Gemini (default)
- `langchain-groq` — Groq (automatic fallback)
- `langchain-openai` — OpenAI
- `langchain-anthropic` — Anthropic Claude

### Chart Generation
- `matplotlib` — Chart rendering
- `seaborn` — Statistical visualization themes

## Roadmap

### Recently Completed
- **Dual MCP server architecture** — split the monolithic MCP server into File Profiler (port 8080) and Data Connector (port 8081) servers. The connector server runs the full end-to-end pipeline (profile, detect, enrich, visualize) on remote data via a staging directory pattern.
- **Column-level DBSCAN enrichment** — enrichment pipeline where DBSCAN clustering operates at the column embedding level, producing precise FK derivations and semantic groupings.

### Future
- **Authentication layer** — API key / OAuth / JWT for production multi-tenant deployments
- **Prometheus metrics** — `profiler_files_processed_total`, `profiler_processing_duration_seconds`, etc.
- **Structured JSON logging** — per-tool invocation logs for observability
- **Upload cleanup background task** — TTL-based automatic cleanup of uploaded files
- **Rate limiting** — per-client concurrency limits for SSE transport

## License

Proprietary. All rights reserved.
