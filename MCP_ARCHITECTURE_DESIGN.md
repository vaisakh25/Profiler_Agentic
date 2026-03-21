# MCP Architecture Design — Agentic Data Profiler

## 1. Vision

Transform the existing file profiling pipeline into an MCP (Model Context Protocol) server that can be containerized and deployed anywhere. Any MCP-compatible client (Claude Desktop, Claude Code, custom agents) connects to the server and invokes profiling tools over the standard protocol — getting structured JSON results back for reasoning, summarization, or chaining into larger workflows.

> **Status (March 2026):** All core phases (1-4) plus agent, web UI, enrichment pipeline, multi-source connectors, and secure credential management are complete and operational. The system has 15 MCP tools, 2 resources, 3 prompts, a full connector framework (S3, ADLS, GCS, Snowflake, PostgreSQL), and Fernet-encrypted credential storage. See [README.md](README.md) for current usage instructions.

---

## 2. Current State

The profiler is a Python package (`file_profiler/`) implementing an 11-layer pipeline:

```
Intake → Classification → Size Strategy → Engine (CSV/Parquet/JSON/Excel/DB) → Standardization
→ Column Profiling → Type Inference → Quality Checks → Relationship Detection → Output
```

Entry points:
- `run(path, output_dir, parallel)` — auto-detect file vs directory
- `profile_file(path, output_dir)` — single file through all layers
- `profile_directory(dir_path, output_dir, parallel)` — batch with optional parallelism
- `analyze_relationships(profiles, output_path, er_diagram_path)` — cross-table FK detection
- `profile_remote(uri, connection_id, table_filter, output_dir)` — remote source profiling

All output is format-agnostic JSON. The code is well-factored with clean separation between layers. Remote sources enter the pipeline at the RawColumnData level, bypassing intake/classify/strategy layers.

---

## 3. Target Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│   MCP Client                                                      │
│   (Claude Desktop / Claude Code / Custom Agent / Web UI)         │
└────────────────────┬─────────────────────────────────────────────┘
                     │ MCP Protocol (stdio or SSE/HTTP)
                     ▼
┌──────────────────────────────────────────────────────────────────┐
│                MCP Server (FastMCP :8080)                          │
│                                                                  │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐           │
│  │   Tools (15)  │  │  Resources   │  │   Prompts    │           │
│  │              │  │              │  │              │           │
│  │ profile_file │  │ profiles://  │  │ summarize    │           │
│  │ profile_dir  │  │ relations:// │  │ migration    │           │
│  │ detect_rels  │  │              │  │ quality_rpt  │           │
│  │ enrich_rels  │  │              │  │              │           │
│  │ check_status │  │              │  │              │           │
│  │ visualize    │  │              │  │              │           │
│  │ list_files   │  │              │  │              │           │
│  │ upload_file  │  │              │  │              │           │
│  │ quality_sum  │  │              │  │              │           │
│  │ query_kb     │  │              │  │              │           │
│  │ get_table_rel│  │              │  │              │           │
│  │ compare_prof │  │              │  │              │           │
│  │ connect_src  │  │              │  │              │           │
│  │ list_conns   │  │              │  │              │           │
│  │ profile_rmt  │  │              │  │              │           │
│  └──────┬───────┘  └──────┬───────┘  └──────────────┘           │
│         │                 │                                      │
│         ▼                 ▼                                      │
│  ┌──────────────────────────────────┐                            │
│  │       File Resolver Layer        │                            │
│  │  (local / upload / remote URI)   │                            │
│  └──────────────┬───────────────────┘                            │
│                 │                                                 │
└─────────────────┼─────────────────────────────────────────────────┘
                  │ Calls existing pipeline
                  ▼
┌─────────────────────────────────────────────────────────────────┐
│            Existing file_profiler package                        │
│  (intake → classify → engine → profile → output)                │
│                                                                 │
│  + Connector Framework                                          │
│  (URI parser → registry → connector → DuckDB remote)            │
│  (ConnectionManager → CredentialStore → Fernet encryption)       │
└─────────────────────────────────────────────────────────────────┘
```

---

## 4. Transport Strategy

Support multiple transports from a single codebase. FastMCP handles this — only the startup command changes.

### 4.1 stdio (Local Development)

- Used when the MCP client and server run on the same machine.
- Client spawns the server as a subprocess; communication over stdin/stdout.
- Zero network setup. Ideal for Claude Desktop and Claude Code local use.

```bash
python -m file_profiler --transport stdio
```

### 4.2 SSE / Streamable HTTP (Remote / Containerized)

- Used when the server runs in a container, VM, or cloud service.
- Client connects over HTTP. Server exposes SSE endpoint for streaming.
- Supports multiple concurrent clients.

```bash
python -m file_profiler --transport sse --host 0.0.0.0 --port 8080
python -m file_profiler --transport streamable-http --host 0.0.0.0 --port 8080
```

### 4.3 Transport Selection

| Scenario                        | Transport        | Why                                      |
|---------------------------------|------------------|------------------------------------------|
| Local dev with Claude Desktop   | stdio            | Simplest, no network                     |
| Local dev with Claude Code      | stdio            | Direct subprocess                        |
| Docker on same machine          | SSE              | Container isolation needs network        |
| Cloud deployment                | SSE              | Remote access, multi-client              |
| CI/CD pipeline integration      | SSE              | Headless, API-driven                     |
| Web UI (LangGraph agent)        | SSE              | WebSocket ↔ MCP over SSE                 |

---

## 5. MCP Tools Design

### 5.1 Tool Summary Table

| Tool                     | Wraps                          | Use Case                                  | Status |
|--------------------------|--------------------------------|-------------------------------------------|--------|
| `profile_file`           | `main.profile_file()`          | Deep-dive into a single file              | Built |
| `profile_directory`      | `main.profile_directory()`     | Batch profiling                           | Built |
| `detect_relationships`   | `main.analyze_relationships()` | Cross-table FK discovery (deterministic)  | Built |
| `enrich_relationships`   | `enrichment_mapreduce`         | Full Map-Reduce RAG + LLM enrichment      | Built |
| `check_enrichment_status`| Manifest fingerprint check     | Fast stale/complete check                 | Built |
| `visualize_profile`      | `chart_generator`              | matplotlib/seaborn chart generation       | Built |
| `list_supported_files`   | Intake + Classifier only       | Reconnaissance before profiling           | Built |
| `upload_file`            | File receiver                  | Remote clients sending files to profile   | Built |
| `get_quality_summary`    | Pipeline minus relationships   | Quick health check                        | Built |
| `query_knowledge_base`   | ChromaDB semantic search       | RAG questions about profiled data         | Built |
| `get_table_relationships`| Cached relationship lookup     | Get all rels for a specific table         | Built |
| `compare_profiles`       | Fingerprint diff               | Schema drift detection                    | Built |
| `connect_source`         | `ConnectionManager.register()` | Register remote source credentials        | Built |
| `list_connections`       | `ConnectionManager.list()`     | List registered connections               | Built |
| `profile_remote_source`  | `main.profile_remote()`        | Profile S3/ADLS/GCS/SF/PG sources        | Built |

### 5.2 REST API Endpoints (Credential Management)

These endpoints handle credentials securely — they bypass the LLM entirely and are NOT MCP tools.

| Method | Endpoint | Description |
|--------|----------|-------------|
| `GET` | `/api/connections` | List all connections (no secrets in response) |
| `POST` | `/api/connections` | Register a new connection with credentials |
| `DELETE` | `/api/connections/{id}` | Remove a stored connection |
| `POST` | `/api/connections/{id}/test` | Test connectivity and measure latency |

---

## 6. MCP Resources

Resources expose cached/generated artifacts that the client can read on demand.

```python
@mcp.resource("profiles://{table_name}")
async def get_cached_profile(table_name: str) -> str:
    """Return a previously generated profile by table name."""

@mcp.resource("relationships://latest")
async def get_cached_relationships() -> str:
    """Return the most recent relationship report."""
```

Resources are read-only. They serve cached results from prior tool invocations.

---

## 7. MCP Prompts

Pre-built prompt templates for common analysis patterns.

```python
@mcp.prompt()
async def summarize_profile(table_name: str) -> str:
    """Generate a natural-language summary of a profiled table."""

@mcp.prompt()
async def migration_readiness(dir_path: str) -> str:
    """Assess migration readiness for a set of data files."""

@mcp.prompt()
async def quality_report(table_name: str) -> str:
    """Generate a detailed quality report for a table."""
```

---

## 8. File & Data Access Strategy

### 8.1 Supported Access Modes

#### Mode A: Volume Mount (Batch / Local Docker)

Mount a host directory into the container at `/data`.

```yaml
volumes:
  - ./my-data-files:/data:ro
```

#### Mode B: File Upload (Remote / Ad-Hoc)

Client sends file content via the `upload_file` tool (base64) or drag-and-drop in the web UI (multipart upload).

#### Mode C: Remote Sources (Production Pipelines)

Server connects to remote data sources via the connector framework:

```python
@mcp.tool()
async def profile_remote_source(
    uri: str,                    # s3://bucket/path, postgresql://host/db
    connection_id: str = "",     # Links to stored credentials
    table_filter: str = "",      # Filter tables by pattern
) -> "dict | list[dict]":
    """Profile a remote data source."""
```

Supported URI schemes: `s3://`, `abfss://`, `gs://`, `snowflake://`, `postgresql://`

### 8.2 File Resolver Layer

Abstract file access behind a resolver so tools don't care about the source:

```python
def resolve_source(path_or_uri: str) -> Path | SourceDescriptor:
    """
    Given a path or URI, return a local Path or SourceDescriptor.
    - /data/foo.csv            → Path (local file)
    - s3://bucket/file.parquet → SourceDescriptor (remote)
    - postgresql://host/db     → SourceDescriptor (remote)
    """
```

---

## 9. Connector Framework Architecture

### 9.1 Component Diagram

```
┌─────────────────────────────────────────────────────────────┐
│  connectors/                                                 │
│                                                             │
│  ┌──────────────┐    ┌──────────────────┐                   │
│  │  uri_parser   │    │  registry        │                   │
│  │              │    │                  │                   │
│  │ parse_uri() ─┼───►│ ConnectorRegistry │                   │
│  │ is_remote()  │    │ get(scheme)      │                   │
│  └──────────────┘    │ register(scheme) │                   │
│                      │ (lazy loading)   │                   │
│                      └────────┬─────────┘                   │
│                               │                             │
│                    ┌──────────┴──────────┐                  │
│                    │                     │                  │
│           ┌────────┴────────┐  ┌────────┴────────┐         │
│           │ CloudStorage    │  │ Database         │         │
│           │ Connector       │  │ Connector        │         │
│           │                 │  │                  │         │
│           │ S3, ADLS, GCS   │  │ PostgreSQL,      │         │
│           │                 │  │ Snowflake        │         │
│           │ DuckDB httpfs/  │  │                  │         │
│           │ azure extension │  │ DuckDB pg_scanner│         │
│           │ + native SDK    │  │ + native SDK     │         │
│           │ for listing     │  │ for Snowflake    │         │
│           └─────────────────┘  └──────────────────┘         │
│                                                             │
│  ┌──────────────────┐    ┌──────────────────────┐          │
│  │ connection_mgr   │    │ credential_store     │          │
│  │                  │    │                      │          │
│  │ register()      │    │ Fernet encryption    │          │
│  │ resolve_creds() │───►│ encrypt/decrypt      │          │
│  │ test()          │    │ file persistence     │          │
│  │ Priority:       │    │ (.connections.enc)   │          │
│  │ 1. connection_id│    │                      │          │
│  │ 2. env vars     │    └──────────────────────┘          │
│  │ 3. SDK defaults │                                      │
│  └──────────────────┘                                      │
│                                                             │
│  ┌──────────────────┐                                      │
│  │ duckdb_remote    │                                      │
│  │                  │                                      │
│  │ create_remote_   │                                      │
│  │ connection()     │                                      │
│  │ remote_count()   │                                      │
│  │ remote_sample()  │                                      │
│  │ remote_schema()  │                                      │
│  └──────────────────┘                                      │
└─────────────────────────────────────────────────────────────┘
```

### 9.2 Supported Sources

| Source | URI Scheme | DuckDB Extension | Native SDK |
|--------|-----------|-----------------|------------|
| AWS S3 | `s3://` | `httpfs` | `boto3` (listing) |
| Azure ADLS Gen2 | `abfss://` | `azure` | `azure-storage-blob` (listing) |
| Google Cloud Storage | `gs://` | `httpfs` | `google-cloud-storage` (listing) |
| Snowflake | `snowflake://` | Not supported | `snowflake-connector-python` |
| PostgreSQL | `postgresql://` | `postgres_scanner` | — |

---

## 10. Security

### 10.1 Credential Security

Credentials flow from the UI directly to REST endpoints — they **never** pass through the LLM, chat history, or LangGraph checkpoints.

- **Encryption:** Fernet symmetric encryption using SHA-256 of `PROFILER_SECRET_KEY`
- **Storage:** Double-encrypted `.connections.enc` file (individual creds + entire file)
- **Fallback:** In-memory only when no `PROFILER_SECRET_KEY` is configured (no disk persistence)
- **API:** REST list responses never include credential values

### 10.2 Path Traversal

All file paths are validated to prevent directory traversal attacks. The file resolver enforces that resolved paths are within allowed directories.

### 10.3 Upload Limits

- Maximum file size: configurable (default 500 MB)
- Upload TTL: auto-cleanup after configurable period (default 1 hour)
- Upload directory isolation: each upload gets a UUID subdirectory

### 10.4 Rate Limiting

WebSocket chat endpoint has:
- Max 10 concurrent sessions
- 1 second minimum interval between messages

### 10.5 Authentication (Future)

For cloud deployments, add authentication before the MCP layer:
- **API key**: Simple header-based auth for internal services
- **OAuth / JWT**: For multi-tenant or user-facing deployments
- **Cloud IAM**: Leverage cloud-native auth (GCP IAP, AWS Cognito, Azure AD)

---

## 11. Deployment Options

### 11.1 Local (Development)

```bash
# stdio — Claude Desktop / Claude Code
python -m file_profiler --transport stdio

# SSE — for chatbot / web UI
python -m file_profiler --transport sse --port 8080
```

### 11.2 Web UI

```bash
# Terminal 1: MCP server
python -m file_profiler --transport sse --port 8080

# Terminal 2: Web server
python -m file_profiler.agent --web --web-port 8501
```

### 11.3 Docker

```bash
docker compose up -d
# MCP server at http://localhost:8080
# Web UI at http://localhost:8501
```

### 11.4 Cloud Deployment

Supports Cloud Run (GCP), ECS/Fargate (AWS), and Azure Container Apps. Pair with respective cloud storage (S3, GCS, Azure Blob) via the connector framework.

---

## 12. Implementation Phases

### Phase 1: MCP Server Core — COMPLETE
- [x] FastMCP server with 6 core tools
- [x] stdio, SSE, and streamable-http transports
- [x] LRU profile cache (200 entries)

### Phase 2: File Upload — COMPLETE
- [x] `upload_file` tool + web UI drag-and-drop
- [x] UUID isolation, TTL cleanup, size validation

### Phase 3: Containerization — COMPLETE
- [x] Dockerfile, docker-compose.yml
- [x] Health check endpoint

### Phase 4: Resources & Prompts — COMPLETE
- [x] Profile and relationship cache resources
- [x] Summarize, migration readiness, quality report prompts

### Phase 4.5: Agent, Web UI & Enrichment — COMPLETE
- [x] LangGraph ReAct agent with MCP tool integration
- [x] Interactive chatbot + web UI with real-time progress
- [x] Map-Reduce enrichment (MAP, APPLY, EMBED, CLUSTER, REDUCE, META-REDUCE)
- [x] ChromaDB vector store with fingerprinting
- [x] Multi-provider LLM factory with fallback
- [x] Chart generation (12 chart types, dark/light themes)
- [x] JSON, Excel, DuckDB/SQLite engines
- [x] Chat persistence (PostgresSaver + MemorySaver fallback)
- [x] Session management + conversation history restore

### Phase 5: Multi-Source Connectors — COMPLETE
- [x] Connector framework (base, URI parser, registry)
- [x] Cloud storage connector (S3, ADLS Gen2, GCS)
- [x] Database connector (PostgreSQL, Snowflake)
- [x] DuckDB remote layer (extensions, query helpers)
- [x] Connection manager with credential resolution
- [x] Secure credential storage (Fernet encryption at rest)
- [x] REST API endpoints (`/api/connections` CRUD + test)
- [x] Frontend connection management modal
- [x] Environment variable fallbacks for all schemes
- [x] `connect_source`, `list_connections`, `profile_remote_source` MCP tools

### Phase 6: Production Hardening — IN PROGRESS
- [x] Health check endpoint
- [x] Rate limiting (WebSocket sessions)
- [ ] Authentication layer (OAuth/JWT) — **Future**
- [ ] Structured JSON logging — **Future**
- [ ] Prometheus metrics — **Future**
- [ ] Upload cleanup background task — **Future**

### Phase 7: Advanced Enrichment — IN PROGRESS
- [ ] Column-level DBSCAN enrichment redesign — **In design phase.** Enrichment pipeline redesign where DBSCAN clustering operates at the column embedding level rather than table level, producing more precise FK derivations and semantic groupings.

---

## 13. Dependencies

```
# Core pipeline
pyarrow >= 21.0.0
chardet >= 5.2.0
duckdb >= 1.4.0
mcp[cli] >= 1.0.0

# Agent + chatbot
langgraph >= 1.0.0
langchain-core >= 1.2.0
langchain-mcp-adapters >= 0.2.0
langgraph-checkpoint-postgres >= 3.0.0

# Web server
fastapi >= 0.115.0
uvicorn >= 0.30.0
websockets >= 13.0.0

# LLM providers
langchain-google-genai >= 4.2.0
langchain-groq >= 1.0.0
langchain-anthropic >= 1.0.0
langchain-openai >= 0.3.0

# RAG enrichment
chromadb >= 1.5.0
langchain-chroma >= 1.1.0
langchain-huggingface >= 0.1.0
sentence-transformers >= 3.0.0

# Credential security
cryptography >= 46.0.0

# Charts
matplotlib >= 3.9.0
seaborn >= 0.13.0

# Remote connectors (install as needed)
boto3 >= 1.35.0                    # AWS S3
azure-storage-blob >= 12.23.0     # Azure ADLS
google-cloud-storage >= 2.18.0    # GCS
snowflake-connector-python >= 3.0 # Snowflake
```
