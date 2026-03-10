# MCP Architecture Design — Agentic Data Profiler

## 1. Vision

Transform the existing file profiling pipeline into an MCP (Model Context Protocol) server that can be containerized and deployed anywhere. Any MCP-compatible client (Claude Desktop, Claude Code, custom agents) connects to the server and invokes profiling tools over the standard protocol — getting structured JSON results back for reasoning, summarization, or chaining into larger workflows.

---

## 2. Current State

The profiler is a Python package (`file_profiler/`) implementing an 11-layer pipeline:

```
Intake → Classification → Size Strategy → Engine (CSV/Parquet) → Standardization
→ Column Profiling → Type Inference → Quality Checks → Relationship Detection → Output
```

Entry points today:
- `run(path, output_dir, parallel)` — auto-detect file vs directory
- `profile_file(path, output_dir)` — single file through all layers
- `profile_directory(dir_path, output_dir, parallel)` — batch with optional parallelism
- `analyze_relationships(profiles, output_path, er_diagram_path)` — cross-table FK detection

All output is format-agnostic JSON. The code is well-factored with clean separation between layers.

---

## 3. Target Architecture

```
┌──────────────────────────────────────────────────────┐
│   MCP Client                                         │
│   (Claude Desktop / Claude Code / Custom Agent)      │
└────────────────────┬─────────────────────────────────┘
                     │ MCP Protocol (stdio or SSE/HTTP)
                     ▼
┌──────────────────────────────────────────────────────┐
│                MCP Server (FastMCP)                   │
│                                                      │
│  ┌─────────────┐  ┌─────────────┐  ┌──────────────┐ │
│  │   Tools      │  │  Resources  │  │   Prompts    │ │
│  │             │  │             │  │              │ │
│  │ profile_file│  │ profiles/   │  │ summarize    │ │
│  │ profile_dir │  │ relations   │  │ migration    │ │
│  │ detect_rels │  │ quality/    │  │ quality_rpt  │ │
│  │ list_files  │  │             │  │              │ │
│  │ quality_chk │  │             │  │              │ │
│  │ upload_file │  │             │  │              │ │
│  └──────┬──────┘  └──────┬──────┘  └──────────────┘ │
│         │                │                           │
│         ▼                ▼                           │
│  ┌─────────────────────────────────┐                 │
│  │       File Resolver Layer       │                 │
│  │  (local / upload / cloud store) │                 │
│  └──────────────┬──────────────────┘                 │
│                 │                                    │
└─────────────────┼────────────────────────────────────┘
                  │ Calls existing pipeline
                  ▼
┌──────────────────────────────────────────────────────┐
│            Existing file_profiler package             │
│  (intake → classify → engine → profile → output)     │
└──────────────────────────────────────────────────────┘
```

---

## 4. Transport Strategy

Support both transports from a single codebase. FastMCP handles this — only the startup command changes.

### 4.1 stdio (Local Development)

- Used when the MCP client and server run on the same machine.
- Client spawns the server as a subprocess; communication over stdin/stdout.
- Zero network setup. Ideal for Claude Desktop and Claude Code local use.

```bash
# Local usage — client spawns this
python mcp_server.py --transport stdio
```

### 4.2 SSE / Streamable HTTP (Remote / Containerized)

- Used when the server runs in a container, VM, or cloud service.
- Client connects over HTTP. Server exposes SSE endpoint for streaming.
- Supports multiple concurrent clients.

```bash
# Remote usage — container entrypoint
python mcp_server.py --transport sse --host 0.0.0.0 --port 8080
```

### 4.3 Transport Selection

| Scenario                        | Transport | Why                                      |
|---------------------------------|-----------|------------------------------------------|
| Local dev with Claude Desktop   | stdio     | Simplest, no network                     |
| Local dev with Claude Code      | stdio     | Direct subprocess                        |
| Docker on same machine          | SSE       | Container isolation needs network        |
| Cloud deployment                | SSE       | Remote access, multi-client              |
| CI/CD pipeline integration      | SSE       | Headless, API-driven                     |

---

## 5. MCP Tools Design

### 5.1 Core Profiling Tools

These are thin wrappers around the existing pipeline. No business logic duplication.

```python
@mcp.tool()
def profile_file(file_path: str) -> dict:
    """
    Profile a single data file (CSV, Parquet, JSON, Excel).

    Runs the full 11-layer pipeline:
    intake → classification → strategy → engine → standardization
    → column profiling → type inference → quality checks → output.

    Args:
        file_path: Absolute path to the file (inside mounted volume or upload dir).

    Returns:
        FileProfile as JSON — contains columns[], quality_summary,
        structural_issues[], row_count, inferred types, and more.
    """

@mcp.tool()
def profile_directory(dir_path: str, parallel: bool = True) -> list[dict]:
    """
    Profile all supported files in a directory.

    Scans for CSV, Parquet, JSON, Excel files. Profiles each through
    the full pipeline. Optionally runs in parallel (ProcessPoolExecutor).

    Args:
        dir_path: Path to directory containing data files.
        parallel: Whether to profile files in parallel (default True).

    Returns:
        List of FileProfile JSON objects, one per successfully profiled file.
        Failed files are logged but do not block other files.
    """

@mcp.tool()
def detect_relationships(dir_path: str, confidence_threshold: float = 0.30) -> dict:
    """
    Analyze cross-table foreign key relationships.

    Profiles all files in the directory (if not already cached), then
    runs the relationship detector across all table pairs.

    Args:
        dir_path: Path to directory with data files.
        confidence_threshold: Minimum confidence to include a candidate (default 0.30).

    Returns:
        RelationshipReport JSON — FK candidates sorted by confidence,
        each with evidence signals (name similarity, type compat, cardinality, value overlap).
    """
```

### 5.2 Utility Tools

```python
@mcp.tool()
def list_supported_files(dir_path: str) -> list[dict]:
    """
    List files in a directory that the profiler can handle.

    Returns file name, size, and detected format for each supported file.
    Does NOT run full profiling — just intake + classification.

    Useful for the agent to understand what's available before
    deciding which files to profile.
    """

@mcp.tool()
def get_quality_summary(file_path: str) -> dict:
    """
    Quick quality check on a single file.

    Runs intake + classification + engine + quality checker only.
    Skips relationship detection. Faster than full profile_file.

    Returns:
        Quality flags, structural issues, null-heavy columns,
        type conflict columns, and corrupt row counts.
    """

@mcp.tool()
def upload_file(file_name: str, file_content_base64: str) -> dict:
    """
    Upload a file for profiling (base64-encoded).

    Decodes and writes the file to the server's temp/upload directory.
    Returns the server-side path that can be passed to profile_file.

    Args:
        file_name: Original file name (used for format hints and naming).
        file_content_base64: Base64-encoded file content.

    Returns:
        {"server_path": "/data/uploads/<uuid>/<file_name>", "size_bytes": ...}
    """
```

### 5.3 Tool Summary Table

| Tool                  | Wraps                          | Use Case                                  |
|-----------------------|--------------------------------|-------------------------------------------|
| `profile_file`        | `main.profile_file()`          | Deep-dive into a single file              |
| `profile_directory`   | `main.profile_directory()`     | Batch profiling                           |
| `detect_relationships`| `main.analyze_relationships()` | Cross-table FK discovery                  |
| `list_supported_files`| Intake + Classifier only       | Reconnaissance before profiling           |
| `get_quality_summary` | Pipeline minus relationships   | Quick health check                        |
| `upload_file`         | New — file receiver            | Remote clients sending files to profile   |

---

## 6. MCP Resources

Resources expose cached/generated artifacts that the client can read on demand.

```python
@mcp.resource("profiles://{table_name}")
def get_profile(table_name: str) -> dict:
    """Return a previously generated profile by table name."""

@mcp.resource("relationships://latest")
def get_relationships() -> dict:
    """Return the most recent relationship report."""

@mcp.resource("quality://{table_name}")
def get_quality(table_name: str) -> dict:
    """Return quality summary for a previously profiled table."""
```

Resources are read-only. They serve cached results from prior tool invocations. This lets the agent re-read profiles without re-running the pipeline.

---

## 7. MCP Prompts

Pre-built prompt templates that the client can invoke for common analysis patterns.

```python
@mcp.prompt()
def summarize_profile(table_name: str) -> str:
    """
    Generate a natural-language summary of a profiled table.
    Includes: row count, column types, key candidates, quality issues, 
    List of low cardinality columns.
    """

@mcp.prompt()
def migration_readiness(dir_path: str) -> str:
    """
    Assess migration readiness for a set of data files.
    Checks: type consistency, null ratios, key candidates,
    relationship coverage, encoding issues.
    """

@mcp.prompt()
def quality_report(table_name: str) -> str:
    """
    Generate a detailed quality report for a table.
    Lists all flags, affected columns, and suggested remediations.
    """
```

---

## 8. File Access Strategy

### 8.1 The Problem

The profiler reads files from the local filesystem. In a container, "local" is the container's filesystem — not the client's machine. We need a strategy for getting files to the server.

### 8.2 Supported Access Modes

#### Mode A: Volume Mount (Batch / Local Docker)

Mount a host directory into the container at `/data`.

```yaml
# docker-compose.yml
volumes:
  - ./my-data-files:/data:ro
```

The agent passes paths like `/data/customers.csv` to `profile_file`. Simple, fast, no upload overhead. Best for batch jobs where files are already on disk.

#### Mode B: File Upload (Remote / Ad-Hoc)

Client sends file content (base64-encoded) via the `upload_file` tool. Server writes to a temp directory, returns the server-side path. Client then calls `profile_file` with that path.

Flow:
```
Client → upload_file(name, base64_content) → server writes to /data/uploads/<uuid>/file.csv
Client → profile_file("/data/uploads/<uuid>/file.csv") → FileProfile JSON
Server → cleanup after TTL (configurable, default 1 hour)
```

Best for ad-hoc profiling of individual files from remote clients.

#### Mode C: Cloud Storage (Future — Production Pipelines)

Server pulls files from S3, GCS, or Azure Blob Storage.

```python
@mcp.tool()
def profile_cloud_file(uri: str) -> dict:
    """
    Profile a file from cloud storage.

    Supported URI schemes:
      s3://bucket/key
      gs://bucket/object
      az://container/blob

    Downloads to temp dir, profiles, cleans up.
    """
```

This is Phase 5 work. Not needed for initial release.

### 8.3 File Resolver Layer

Abstract file access behind a resolver so tools don't care about the source:

```python
class FileResolver:
    def resolve(self, path_or_uri: str) -> Path:
        """
        Given a path or URI, return a local filesystem path.
        - /data/foo.csv        → return as-is (volume mount)
        - upload://<uuid>/file → return upload dir path
        - s3://bucket/key      → download to temp, return path
        """
```

All tools call `resolver.resolve(path)` before passing to the pipeline. This keeps the existing `file_profiler` package unchanged.

---

## 9. Containerization

### 9.1 Dockerfile

```dockerfile
FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir "mcp[cli]"

# Copy application code
COPY file_profiler/ file_profiler/
COPY mcp_server.py .

# Create directories for data and uploads
RUN mkdir -p /data /data/uploads /data/output

# Expose port for SSE transport
EXPOSE 8080

# Default: SSE transport for containerized use
ENTRYPOINT ["python", "mcp_server.py"]
CMD ["--transport", "sse", "--host", "0.0.0.0", "--port", "8080"]
```

### 9.2 docker-compose.yml

```yaml
version: "3.8"

services:
  profiler-mcp:
    build: .
    ports:
      - "8080:8080"
    volumes:
      - ./data:/data:ro           # Mount local data files (read-only)
      - profiler-uploads:/data/uploads  # Persist uploads
      - profiler-output:/data/output    # Persist output profiles
    environment:
      - MAX_UPLOAD_SIZE_MB=500
      - UPLOAD_TTL_HOURS=1
      - MAX_PARALLEL_WORKERS=4
      - LOG_LEVEL=INFO
    restart: unless-stopped
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:8080/health"]
      interval: 30s
      timeout: 10s
      retries: 3

volumes:
  profiler-uploads:
  profiler-output:
```

### 9.3 .dockerignore

```
tests/
data/
*.md
*.svg
*.txt
__pycache__/
.pytest_cache/
.git/
```

---

## 10. MCP Server Implementation Skeleton

```python
# mcp_server.py

import argparse
import asyncio
import base64
import json
import logging
import tempfile
import uuid
from pathlib import Path

from mcp.server.fastmcp import FastMCP

from file_profiler.main import run, profile_file, profile_directory, analyze_relationships

# --- Server setup ---
mcp = FastMCP(
    name="file-profiler",
    version="1.0.0",
    description="Agentic Data Profiler — profile CSV, Parquet, JSON, Excel files via MCP",
)

# --- Configuration ---
DATA_DIR = Path("/data")
UPLOAD_DIR = DATA_DIR / "uploads"
OUTPUT_DIR = DATA_DIR / "output"

# --- Profile cache (in-memory, keyed by table_name) ---
_profile_cache: dict[str, dict] = {}
_relationship_cache: dict | None = None


# --- Tools ---

@mcp.tool()
def profile_file_tool(file_path: str) -> dict:
    """Profile a single data file through the full 11-layer pipeline."""
    resolved = _resolve_path(file_path)
    result = profile_file(resolved, OUTPUT_DIR)
    profile_dict = _serialize_profile(result)
    _profile_cache[result.table_name] = profile_dict
    return profile_dict


@mcp.tool()
def profile_directory_tool(dir_path: str, parallel: bool = True) -> list[dict]:
    """Profile all supported files in a directory."""
    resolved = _resolve_path(dir_path)
    results = profile_directory(resolved, OUTPUT_DIR, parallel=parallel)
    profiles = []
    for r in results:
        d = _serialize_profile(r)
        _profile_cache[r.table_name] = d
        profiles.append(d)
    return profiles


@mcp.tool()
def detect_relationships_tool(dir_path: str, confidence_threshold: float = 0.30) -> dict:
    """Detect foreign key relationships across tables in a directory."""
    global _relationship_cache
    resolved = _resolve_path(dir_path)
    profiles = profile_directory(resolved, OUTPUT_DIR, parallel=True)
    report = analyze_relationships(profiles, OUTPUT_DIR / "relationships.json")
    result = _serialize_relationships(report, confidence_threshold)
    _relationship_cache = result
    return result


@mcp.tool()
def list_supported_files_tool(dir_path: str) -> list[dict]:
    """List files that the profiler can handle in a directory."""
    # Intake + classification only — no full profiling
    ...


@mcp.tool()
def upload_file_tool(file_name: str, file_content_base64: str) -> dict:
    """Upload a base64-encoded file for profiling. Returns server-side path."""
    upload_id = str(uuid.uuid4())
    upload_path = UPLOAD_DIR / upload_id
    upload_path.mkdir(parents=True, exist_ok=True)
    file_path = upload_path / file_name
    file_path.write_bytes(base64.b64decode(file_content_base64))
    return {"server_path": str(file_path), "size_bytes": file_path.stat().st_size}


# --- Resources ---

@mcp.resource("profiles://{table_name}")
def get_cached_profile(table_name: str) -> dict:
    """Return a previously generated profile by table name."""
    if table_name not in _profile_cache:
        return {"error": f"No cached profile for '{table_name}'. Run profile_file first."}
    return _profile_cache[table_name]


@mcp.resource("relationships://latest")
def get_cached_relationships() -> dict:
    """Return the most recent relationship report."""
    if _relationship_cache is None:
        return {"error": "No relationship report cached. Run detect_relationships first."}
    return _relationship_cache


# --- Prompts ---

@mcp.prompt()
def summarize_profile(table_name: str) -> str:
    """Generate a natural-language summary prompt for a profiled table."""
    return f"""Summarize the following data profile for the table "{table_name}".
Include: row count, number of columns, column types breakdown, key candidates,
quality issues found, and any notable patterns.

Profile data:
{{{{profiles://{table_name}}}}}
"""


@mcp.prompt()
def migration_readiness(dir_path: str) -> str:
    """Assess migration readiness for a set of data files."""
    return f"""Analyze the profiling results for all tables in "{dir_path}".
Assess migration readiness based on:
1. Type consistency across columns
2. Null ratios and data completeness
3. Key candidate coverage (do all tables have identifiable PKs?)
4. Relationship coverage (are FK relationships detected?)
5. Encoding and structural issues
6. Data quality flags

Provide a readiness score (High / Medium / Low) with justification.
"""


# --- Helpers ---

def _resolve_path(path: str) -> Path:
    """Resolve a path — supports volume-mounted and uploaded files."""
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"Path not found: {path}")
    return p


def _serialize_profile(profile) -> dict:
    """Convert a FileProfile to a JSON-serializable dict."""
    # Delegates to existing profile_writer serialization logic
    ...


def _serialize_relationships(report, threshold: float) -> dict:
    """Convert a RelationshipReport to a JSON-serializable dict."""
    ...


# --- Entry point ---

def main():
    parser = argparse.ArgumentParser(description="File Profiler MCP Server")
    parser.add_argument("--transport", choices=["stdio", "sse"], default="stdio")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()

    if args.transport == "stdio":
        mcp.run(transport="stdio")
    else:
        mcp.run(transport="sse", host=args.host, port=args.port)


if __name__ == "__main__":
    main()
```

---

## 11. Deployment Options

### 11.1 Local (Development)

```bash
# stdio — Claude Desktop / Claude Code spawns this
python mcp_server.py --transport stdio

# Or register in Claude Desktop config:
# ~/.claude/claude_desktop_config.json
{
  "mcpServers": {
    "file-profiler": {
      "command": "python",
      "args": ["C:/Projects/profiler/Agentic_Data_Profiler_Files/mcp_server.py", "--transport", "stdio"]
    }
  }
}
```

### 11.2 Docker (Single Machine / VM)

```bash
docker compose up -d
# Server available at http://localhost:8080
# Mount data files via volume in docker-compose.yml
```

### 11.3 Cloud Run (Google Cloud)

```bash
gcloud run deploy profiler-mcp \
  --source . \
  --port 8080 \
  --memory 2Gi \
  --cpu 2 \
  --allow-unauthenticated \
  --set-env-vars MAX_PARALLEL_WORKERS=2
```

Pair with GCS for file access. Add `profile_cloud_file` tool for `gs://` URIs.

### 11.4 AWS ECS / Fargate

```bash
# Build and push to ECR
docker build -t profiler-mcp .
docker tag profiler-mcp:latest <account>.dkr.ecr.<region>.amazonaws.com/profiler-mcp:latest
docker push <account>.dkr.ecr.<region>.amazonaws.com/profiler-mcp:latest

# Deploy via ECS task definition with:
# - 2 vCPU, 4GB memory
# - Port 8080 exposed via ALB
# - EFS or S3 for file access
```

### 11.5 Azure Container Apps

```bash
az containerapp up \
  --name profiler-mcp \
  --source . \
  --ingress external \
  --target-port 8080 \
  --env-vars MAX_PARALLEL_WORKERS=2
```

Pair with Azure Blob Storage for file access.

---

## 12. Security Considerations

### 12.1 Path Traversal

All file paths must be validated to prevent directory traversal attacks. The file resolver must enforce that resolved paths are within allowed directories (`/data`).

```python
def _resolve_path(path: str) -> Path:
    resolved = Path(path).resolve()
    if not resolved.is_relative_to(DATA_DIR):
        raise PermissionError(f"Access denied: path outside data directory")
    return resolved
```

### 12.2 Upload Limits

- Maximum file size: configurable (default 500 MB)
- Upload TTL: auto-cleanup after configurable period (default 1 hour)
- Upload directory isolation: each upload gets a UUID subdirectory

### 12.3 Authentication (Production)

For cloud deployments, add authentication before the MCP layer:

- **API key**: Simple header-based auth for internal services
- **OAuth / JWT**: For multi-tenant or user-facing deployments
- **Cloud IAM**: Leverage cloud-native auth (GCP IAP, AWS Cognito, Azure AD)

Not needed for local stdio transport.

### 12.4 Rate Limiting

For SSE transport, consider rate limiting to prevent abuse:
- Max concurrent profiling jobs per client
- Max file size per upload
- Cooldown between large directory scans

---

## 13. Observability

### 13.1 Logging

Structured JSON logging for all tool invocations:

```json
{
  "timestamp": "2026-02-26T10:30:00Z",
  "tool": "profile_file",
  "file_path": "/data/customers.csv",
  "file_size_bytes": 524288,
  "duration_ms": 1230,
  "status": "success",
  "columns_profiled": 12,
  "quality_issues": 2
}
```

### 13.2 Health Check

```python
@mcp.tool()
def health_check() -> dict:
    """Server health and status."""
    return {
        "status": "healthy",
        "cached_profiles": len(_profile_cache),
        "upload_dir_size_mb": _dir_size_mb(UPLOAD_DIR),
        "version": "1.0.0",
    }
```

### 13.3 Metrics (Future)

Expose Prometheus metrics for production deployments:
- `profiler_files_processed_total` (counter)
- `profiler_processing_duration_seconds` (histogram)
- `profiler_upload_size_bytes` (histogram)
- `profiler_cache_entries` (gauge)

---

## 14. Implementation Phases

### Phase 1: MCP Server Core (Priority: HIGH)
- [ ] Create `mcp_server.py` with FastMCP
- [ ] Implement `profile_file`, `profile_directory`, `detect_relationships` tools
- [ ] Wire tools to existing `file_profiler` pipeline (thin wrappers)
- [ ] Add `list_supported_files` utility tool
- [ ] Support stdio transport
- [ ] Test with Claude Desktop or Claude Code locally
- [ ] Add `mcp` dependency to requirements.txt

### Phase 2: File Upload Support (Priority: HIGH)
- [ ] Implement `upload_file` tool with base64 decode
- [ ] Add upload directory management (UUID isolation, TTL cleanup)
- [ ] Add file size validation
- [ ] Implement file resolver layer

### Phase 3: Containerization (Priority: HIGH)
- [ ] Create Dockerfile
- [ ] Create docker-compose.yml with volume mounts
- [ ] Create .dockerignore
- [ ] Add SSE transport support
- [ ] Test container build and run
- [ ] Verify profiling works inside container

### Phase 4: Resources & Prompts (Priority: MEDIUM)
- [ ] Implement profile cache and resources
- [ ] Add MCP prompts for summarization, migration readiness, quality reports
- [ ] Add relationship cache resource

### Phase 5: Cloud Storage Integration (Priority: LOW)
- [ ] Add S3 file resolver
- [ ] Add GCS file resolver
- [ ] Add Azure Blob file resolver
- [ ] Implement `profile_cloud_file` tool

### Phase 6: Production Hardening (Priority: LOW)
- [ ] Add authentication layer
- [ ] Add rate limiting
- [ ] Add structured logging
- [ ] Add health check endpoint
- [ ] Add Prometheus metrics
- [ ] Add upload cleanup background task

---

## 15. Dependencies (New)

```
# Add to requirements.txt
mcp[cli]>=1.0.0          # MCP SDK with FastMCP and CLI tools
uvicorn>=0.30.0           # ASGI server for SSE transport (pulled by mcp)

# Optional — Phase 5
boto3>=1.35.0             # AWS S3 access
google-cloud-storage>=2.18.0  # GCS access
azure-storage-blob>=12.23.0   # Azure Blob access
```

---

## 16. Client Configuration Examples

### Claude Desktop (`claude_desktop_config.json`)

```json
{
  "mcpServers": {
    "file-profiler": {
      "command": "python",
      "args": ["/path/to/mcp_server.py", "--transport", "stdio"]
    }
  }
}
```

### Claude Desktop — Docker (SSE)

```json
{
  "mcpServers": {
    "file-profiler": {
      "url": "http://localhost:8080/sse"
    }
  }
}
```

### Claude Code (`.claude/settings.json`)

```json
{
  "mcpServers": {
    "file-profiler": {
      "command": "python",
      "args": ["mcp_server.py", "--transport", "stdio"]
    }
  }
}
```

---

## 17. Open Questions / Decision Log

| # | Question | Status | Decision |
|---|----------|--------|----------|
| 1 | Need to have the codebase in a production ready structure. Decouple things into utils, configs , requirements etc. It's in a good shape already, make improvements if possible.
| 2 | Primary MCP client? (Claude Desktop, Claude Code, custom) ---> Custom agent(langgraph) |
| 3 | File access: upload-only, volume-mount-only, or both? --> both  |
| 4 | Start with stdio or jump to SSE? | OPEN | Proposed: stdio first |
| 5 | Deployment target? (Docker Compose, Cloud Run, ECS, K8s) | OPEN | 
| 6 | Auth needed for initial release? | OPEN | Proposed: no (add later) |
| 7 | Max file size for upload? | OPEN | Proposed: 500 MB |
| 8 | Profile caching strategy? (in-memory, disk, Redis) | OPEN | Proposed: in-memory + disk |

| 9 | Should JSON/Excel engines be implemented before MCP wrap? | --> Not now (Later)
| 10 | I need a progress bar for every tool call and its progress.