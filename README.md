# Agentic Data Profiler

A production-grade data profiling engine exposed as an **MCP (Model Context Protocol) server** with an **interactive LangGraph chatbot** and **LLM-powered enrichment**. Profile CSV, Parquet, and other tabular data files — detect schemas, infer types, assess quality, discover cross-table foreign key relationships, and get AI-generated descriptions, join recommendations, and enriched ER diagrams.

## Key Features

- **11-layer profiling pipeline** — intake validation, content-sniffing format detection, memory-safe size strategy, format-specific engines, column standardization, type inference with confidence scoring, structural quality checks, and cross-table relationship detection.
- **MCP server** — 7 tools, 2 resources, 3 prompt templates. Connect from LangGraph, Claude Desktop, Claude Code, or any MCP client.
- **Interactive chatbot** — multi-turn conversational interface powered by LangGraph + Gemini 2.5 Flash. Point it at a folder and get profiling results, ER diagrams, and enriched analysis through natural language.
- **LLM enrichment (RAG)** — embeds profiling results, sample rows, and low-cardinality values into ChromaDB, then uses an LLM to produce semantic descriptions, PK/FK reassessment, join recommendations, and enriched ER diagrams.
- **Progress tracking** — animated spinner with elapsed time, weighted progress bar, rotating stage hints, and smart result summaries during long-running operations.
- **Format-agnostic output** — identical JSON profile schema regardless of source format (CSV, Parquet, JSON, Excel).
- **Memory-safe** — three-tier read strategy (MEMORY_SAFE / LAZY_SCAN / STREAM_ONLY) auto-selected based on file size. Handles multi-GB files without OOM.
- **Content sniffing** — never trusts file extensions. Detects format via magic bytes and structural analysis.
- **Containerized** — Dockerfile and docker-compose included. Deploy on Docker, Cloud Run, ECS, Azure Container Apps, or Kubernetes.

## Architecture

```
                          User / Chatbot
                               │
                    ┌──────────┴──────────┐
                    │   LangGraph Agent    │  ← Interactive chatbot (Gemini 2.5 Flash)
                    │   (multi-turn chat)  │     with progress tracking
                    └──────────┬──────────┘
                               │ MCP protocol (SSE / stdio)
                    ┌──────────┴──────────┐
                    │   MCP Server         │  ← 7 tools, 2 resources, 3 prompts
                    │   (FastMCP)          │
                    └──────────┬──────────┘
                               │
          ┌────────────────────┼────────────────────┐
          │                    │                    │
   ┌──────┴──────┐    ┌───────┴───────┐    ┌───────┴───────┐
   │ Deterministic│    │  Relationship │    │ LLM Enrichment│
   │ Pipeline     │    │  Detector     │    │ (RAG Layer)   │
   │ (11 layers)  │    │              │    │ ChromaDB +    │
   │              │    │              │    │ Gemini        │
   └──────────────┘    └──────────────┘    └───────────────┘
```

## Quick Start

### Prerequisites

- Python 3.11+
- pip

### Install

```bash
# Clone the repository
git clone <repo-url>
cd Agentic_Data_Profiler_Files

# Install in editable mode
pip install -e ".[dev]"
```

### Run the MCP Server

```bash
# stdio transport (local — for Claude Desktop, Claude Code, LangGraph)
python -m file_profiler --transport stdio

# SSE transport (remote — for chatbot / containerized deployment)
set PROFILER_DATA_DIR=C:\path\to\your\data
python -m file_profiler --transport sse --host 0.0.0.0 --port 8080
```

### Run the Interactive Chatbot

Start the MCP server in one terminal (SSE transport), then in another:

```bash
# Default (Gemini 2.5 Flash)
python -m file_profiler.agent --chat

# Specify provider and model
python -m file_profiler.agent --chat --provider google --model gemini-2.5-flash

# Custom MCP server URL
python -m file_profiler.agent --chat --mcp-url http://localhost:8080/sse
```

The chatbot provides:
- Natural language interface — tell it where your data is and it profiles it
- Multi-turn memory — ask follow-up questions about your data
- Animated progress tracking — spinner, progress bar, and stage hints during tool execution
- Smart result summaries — parsed tool outputs (file counts, row counts, FK candidates)

Example session:
```
============================================================
  Data Profiler Chatbot
============================================================

  Tell me where your data is and I'll profile it for you.

  Commands: 'help' for tips, 'quit' to exit
============================================================

 You: My data is in data/files

  [1] list_supported_files(dir_path=data/files)
      ✓ Done in 1.2s — 39 files found (39 parquet)
      ██████████████████████████████ 100%

  [2] enrich_relationships(dir_path=data/files)
      ⠹ Building document embeddings... (45.3s)
      ✓ Done in 92.1s — 39 tables, 12 relationships, 41 docs embedded
      ██████████████████████████████ 100%

      Pipeline complete: 2 steps in 93.3s

 Assistant:
  Here's what I found in your data...
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

Point your MCP client to the SSE endpoint:

```json
{
  "mcpServers": {
    "file-profiler": {
      "url": "http://localhost:8080/sse"
    }
  }
}
```

### Volume Mounts

Place your data files in the `./data` directory. They are mounted read-only at `/data/mounted` inside the container. Alternatively, use the `upload_file` tool to send files via base64.

## MCP Tools Reference

| Tool | Description |
|------|-------------|
| `profile_file(file_path)` | Profile a single file through the full 11-layer pipeline. Returns FileProfile with columns, types, quality flags, and statistics. |
| `profile_directory(dir_path, parallel)` | Profile all supported files in a directory. Returns a list of FileProfile dicts. |
| `detect_relationships(dir_path, confidence_threshold)` | Detect foreign key relationships across tables. Scores by name similarity, type compatibility, cardinality, and value overlap. Returns ER diagram (Mermaid). |
| `enrich_relationships(dir_path, provider, model)` | Full pipeline + RAG + LLM enrichment. Profiles all files, detects relationships, extracts sample rows, embeds into ChromaDB, and uses an LLM to produce semantic descriptions, PK/FK reassessment, join recommendations, and an enriched ER diagram. |
| `list_supported_files(dir_path)` | List files the profiler can handle (intake + classification only, no full profiling). |
| `upload_file(file_name, file_content_base64)` | Upload a base64-encoded file to the server. Returns the server-side path for use with `profile_file`. |
| `get_quality_summary(file_path)` | Get quality summary for a file. Returns cached results if available. |

## Pipeline Layers

| Layer | Module | Purpose |
|-------|--------|---------|
| 1 | `intake/validator.py` | File existence, encoding detection (BOM + chardet), compression detection, delimiter sniffing |
| 2 | `classification/classifier.py` | Content-sniffing format detection via magic bytes (Parquet, Excel, JSON, CSV) |
| 3 | `strategy/size_strategy.py` | Auto-select MEMORY_SAFE (<100 MB), LAZY_SCAN (100 MB–2 GB), or STREAM_ONLY (>2 GB) |
| 4 | `engines/csv_engine.py` | CSV structure detection, header detection, row counting, sampling (Vitter's Algorithm R) |
| 5 | `engines/parquet_engine.py` | Parquet metadata reading, schema flattening, column-pruned row-group iteration |
| 6.5 | `standardization/normalizer.py` | Name normalization, null sentinel detection, boolean unification, numeric cleaning |
| 7 | `profiling/column_profiler.py` | Statistics: null count, distinct count, min/max, cardinality, top-N values, string length distribution |
| 7.5 | `profiling/type_inference.py` | Type detection with 90% confidence threshold (INTEGER, FLOAT, DATE, TIMESTAMP, UUID, BOOLEAN, CATEGORICAL, FREE_TEXT, STRING) |
| 8 | `quality/structural_checker.py` | Quality flags: duplicate columns, fully null, constant, high null ratio, column shift errors, encoding inconsistency |
| 9 | `analysis/relationship_detector.py` | Cross-table FK scoring: name similarity (0.50), type compatibility (0.20), cardinality (0.25), value overlap (0.15) |
| 11 | `output/profile_writer.py` | Atomic JSON serialization with QualitySummary computation |

## Project Structure

```
Profiler/
├── file_profiler/                  # Main package
│   ├── __init__.py                 # Public API exports
│   ├── __main__.py                 # python -m file_profiler entry point
│   ├── main.py                     # Pipeline orchestrator
│   ├── mcp_server.py               # MCP server (7 tools, 2 resources, 3 prompts)
│   │
│   ├── agent/                      # LangGraph agent + chatbot
│   │   ├── __init__.py             # Agent exports
│   │   ├── __main__.py             # python -m file_profiler.agent entry point
│   │   ├── chatbot.py              # Interactive multi-turn chatbot with streaming
│   │   ├── graph.py                # ReAct-style StateGraph (agent ↔ tools loop)
│   │   ├── cli.py                  # Autonomous / human-in-the-loop CLI runner
│   │   ├── state.py                # AgentState TypedDict with message history
│   │   ├── llm_factory.py          # Multi-provider LLM factory (Google, OpenAI, Anthropic)
│   │   ├── enrichment.py           # RAG enrichment (ChromaDB + LLM analysis)
│   │   └── progress.py             # Terminal progress tracking (spinner, bar, summaries)
│   │
│   ├── analysis/                   # Cross-table relationship detection
│   ├── classification/             # Format detection via content sniffing
│   ├── config/
│   │   ├── settings.py             # Pipeline tuning constants
│   │   └── env.py                  # Environment-based deployment config
│   ├── engines/                    # Format-specific profiling engines
│   │   ├── csv_engine.py
│   │   ├── parquet_engine.py
│   │   ├── duckdb_sampler.py
│   │   ├── json_engine.py
│   │   └── excel_engine.py
│   ├── intake/                     # File validation and encoding detection
│   ├── models/                     # Data classes and enums
│   ├── output/                     # JSON serialization and ER diagrams
│   ├── profiling/                  # Column profiling and type inference
│   ├── quality/                    # Structural quality checks
│   ├── standardization/            # Data normalization
│   ├── strategy/                   # Size-based read strategy selection
│   └── utils/                      # File resolver, logging setup
├── tests/                          # Test suite
│   ├── test_progress.py            # Progress tracking unit tests
│   ├── test_enrichment_e2e.py      # Enrichment pipeline E2E test
│   └── test_chatbot_progress_e2e.py # Chatbot + progress E2E test
├── data/                           # Sample data and output profiles
├── FILE_PROFILING_ARCHITECTURE.md  # Detailed architecture documentation
├── pyproject.toml                  # Package metadata and dependencies
├── Dockerfile                      # Container image definition
├── docker-compose.yml              # Orchestration with volumes
└── requirements.txt                # Dependency pinning
```

## LLM Enrichment (RAG Layer)

The `enrich_relationships` tool runs a RAG pipeline on top of the deterministic profiling results:

```
Deterministic Pipeline Output
        │
        ▼
  Document Builder ──→ ChromaDB Vector Store ──→ LLM Analysis (Gemini 2.5 Flash)
  (schemas, samples,    (local embeddings:        (semantic descriptions, PK/FK
   relationships,        all-MiniLM-L6-v2)         reassessment, join paths,
   quality metrics)                                enriched ER diagram)
```

**What gets embedded:**

| Data | Source | Purpose |
|------|--------|---------|
| Column schemas | `ColumnProfile` fields | Types, cardinality, key candidates, quality flags |
| Low-cardinality values | `top_values` (up to 15 per column) | Understand categorical columns (gender codes, status values) |
| Sample rows | Source file via PyArrow/CSV (10 rows) | Row-level context — see which values co-occur together |
| Relationships | `ForeignKeyCandidate` objects | FK/PK pairs with confidence scores and evidence codes |
| Quality summary | `QualitySummary` per table | Aggregate quality metrics for recommendations |

**LLM produces:**
1. Semantic table and column descriptions
2. Primary key confirmation/revision
3. Foreign key reassessment + new FK suggestions
4. JOIN type recommendations (INNER/LEFT/etc.)
5. Join path recommendations for analytical queries
6. Enriched ER diagram (Mermaid) with descriptive labels
7. Data quality remediation recommendations

**Embeddings:** local `all-MiniLM-L6-v2` via HuggingFace `sentence-transformers` — fast, free, no API key needed.

## Progress Tracking

The chatbot displays real-time progress during tool execution:

```
  [1] list_supported_files(dir_path=data/files)
      ⠹ Scanning directory... (0.8s)
      ✓ Done in 1.2s — 39 files found (39 parquet)
      ██████████████████████████████ 100%

  [2] enrich_relationships(dir_path=data/files)
      ⠼ Building document embeddings... (45.3s)
      ✓ Done in 92.1s — 5 tables, 3 relationships, 7 docs embedded
      ████████████████████░░░░░░░░░░ 67%
```

Features:
- **Animated spinner** (`⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏`) with elapsed time
- **Weighted progress bar** — tools have different relative costs (e.g. `enrich_relationships=60`, `profile_directory=35`, `list_supported_files=5`)
- **Rotating stage hints** — tool-specific status messages that cycle during long operations (e.g. "Profiling tables" → "Detecting relationships" → "Building document embeddings" → "Running LLM analysis")
- **Smart result summaries** — parses JSON tool results to show meaningful info (file counts, row counts, FK candidates, LLM analysis length)
- **Thinking indicator** — shows while the LLM is processing between tool calls
- **Pipeline summary** — total steps and elapsed time at the end of each turn

## Configuration

### Pipeline Settings (`file_profiler/config/settings.py`)

| Setting | Default | Description |
|---------|---------|-------------|
| `MEMORY_SAFE_MAX_BYTES` | 100 MB | Threshold for full in-memory load |
| `LAZY_SCAN_MAX_BYTES` | 2 GB | Threshold for chunked/lazy reads |
| `SAMPLE_ROW_COUNT` | 10,000 | Rows held in reservoir sample |
| `CATEGORICAL_MAX_DISTINCT` | 50 | Max distinct values for CATEGORICAL type |
| `NULL_HEAVY_THRESHOLD` | 0.70 | Null ratio to flag HIGH_NULL_RATIO |
| `MAX_PARALLEL_WORKERS` | 4 | Parallel file processing workers |

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PROFILER_DATA_DIR` | `/data` | Root directory for data files |
| `PROFILER_UPLOAD_DIR` | `/data/uploads` | Upload storage directory |
| `PROFILER_OUTPUT_DIR` | `/data/output` | Profile output directory |
| `MAX_UPLOAD_SIZE_MB` | `500` | Maximum upload file size |
| `MCP_TRANSPORT` | `stdio` | Transport protocol (`stdio`, `sse`) |
| `MCP_HOST` | `0.0.0.0` | Server bind host |
| `MCP_PORT` | `8080` | Server bind port |
| `LOG_LEVEL` | `INFO` | Logging level |
| `GOOGLE_API_KEY` | — | Required for Gemini LLM provider |
| `GROQ_API_KEY` | — | Required for Groq provider (automatic fallback when Google quota is exhausted) |
| `GROQ_MODEL` | — | Groq model override (default: `llama-3.3-70b-versatile`) |
| `LLM_PROVIDER` | `google` | LLM provider: `google`, `groq`, `openai`, `anthropic` |
| `LLM_MODEL` | (per provider) | Model override (default: `gemini-2.5-flash` for Google) |

## Testing

```bash
# Run the full test suite
pytest

# Run with coverage
pytest --cov=file_profiler --cov-report=term-missing

# Run specific test module
pytest tests/test_mcp_server.py -v

# Run progress tracking unit tests
python tests/test_progress.py

# Run enrichment E2E test (requires GOOGLE_API_KEY in .env)
python tests/test_enrichment_e2e.py

# Run chatbot + progress E2E test (starts MCP server automatically)
python tests/test_chatbot_progress_e2e.py
```

## Supported Formats

| Format | Status | Engine |
|--------|--------|--------|
| CSV (including .tsv, .dat, .psv) | Supported | `csv_engine.py` |
| Parquet (.parquet, .pq, .parq) | Supported | `parquet_engine.py` |
| Gzip-compressed CSV | Supported | Transparent decompression |
| ZIP archives (single or multi-CSV) | Supported | Partition-aware profiling |
| JSON / NDJSON | Planned | Design complete |
| Excel (.xlsx, .xls) | Planned | Design complete |

## Output Schema

Every profiled file produces a unified `FileProfile` JSON structure:

```json
{
  "source_type": "file",
  "file_format": "csv",
  "table_name": "customers",
  "row_count": 10500,
  "is_row_count_exact": true,
  "encoding": "utf-8",
  "size_bytes": 524288,
  "size_strategy": "MEMORY_SAFE",
  "columns": [
    {
      "name": "customer_id",
      "inferred_type": "INTEGER",
      "confidence_score": 1.0,
      "null_count": 0,
      "distinct_count": 10500,
      "cardinality": "HIGH",
      "is_key_candidate": true,
      "quality_flags": [],
      "top_values": [{"value": "1", "count": 1}],
      "sample_values": ["1", "2", "3"]
    }
  ],
  "structural_issues": [],
  "quality_summary": {
    "columns_profiled": 12,
    "columns_with_issues": 0,
    "null_heavy_columns": 0,
    "type_conflict_columns": 0,
    "corrupt_rows_detected": 0
  }
}
```

## Dependencies

### Core Pipeline
- `pyarrow` — Parquet engine
- `chardet` — Encoding detection
- `mcp[cli]` — MCP server framework

### Agent + Chatbot
- `langgraph` — Agent graph framework
- `langchain-core` — Message types and base classes
- `langchain-mcp-adapters` — MCP client for LangChain tools
- `langchain-google-genai` — Gemini LLM provider (default)

### RAG Enrichment
- `chromadb` — Vector store
- `langchain-chroma` — LangChain ChromaDB integration
- `langchain-huggingface` / `sentence-transformers` — Local embeddings (all-MiniLM-L6-v2)

### LLM Fallback
- `langchain-groq` — Groq (automatic fallback when Google quota is exhausted, uses `llama-3.3-70b-versatile`)

### Optional LLM Providers
- `langchain-openai` — OpenAI
- `langchain-anthropic` — Anthropic Claude

## License

Proprietary. All rights reserved.
