# File Profiling Architecture

## Overview

The **Agentic Data Profiler** is a production-grade data profiling engine for tabular data (CSV, Parquet, JSON, Excel, DuckDB/SQLite) and remote sources (S3, ADLS Gen2, GCS, Snowflake, PostgreSQL). It combines a deterministic 11-layer pipeline with a multi-phase Map-Reduce LLM enrichment layer, a multi-source connector framework, and secure credential management to produce comprehensive data profiles, relationship maps, and ER diagrams.

The output is **format-agnostic**: regardless of whether the source was a local CSV, a Parquet file on S3, or a PostgreSQL table, the final profile object is identical, allowing downstream logic to remain source-unaware.

```
                      User / Browser
                           │
              ┌────────────┴────────────┐
              │                         │
    ┌─────────┴─────────┐    ┌─────────┴─────────┐
    │   Web UI           │    │   CLI Chatbot      │
    │   (FastAPI :8501)  │    │   (Terminal)        │
    │                    │    │                    │
    │   WebSocket        │    │                    │
    │   /ws/chat ────────┼────┼──► LangGraph Agent │
    │                    │    │    (PostgresSaver   │
    │   REST             │    │     checkpointing) │
    │   /api/connections─┼────┼──► ConnectionManager│
    │   (creds bypass    │    │    + CredentialStore│
    │    LLM entirely)   │    │    (Fernet encrypt) │
    └─────────┬─────────┘    └─────────┬──────────┘
              │                         │
              └────────────┬────────────┘
                           │
                ┌──────────┴──────────┐
                │   LangGraph Agent    │  ← ReAct-style agent loop
                │   MultiServerMCP     │     with PostgreSQL checkpointing
                │   (graceful degrad.) │     + graceful degradation
                └──────────┬──────────┘
                           │ MCP protocol (SSE / stdio)
          ┌────────────────┴────────────────┐
          │                                 │
┌─────────┴─────────┐            ┌─────────┴─────────┐
│  File Profiler     │            │  Data Connector    │
│  MCP Server :8080  │            │  MCP Server :8081  │
│  13 tools          │            │  16 tools          │
└─────────┬─────────┘            └─────────┬─────────┘
          │                                 │
  ┌───────┼───────┐               ┌────────┴────────┐
  │       │       │               │  Connector       │
┌─┴──┐ ┌──┴──┐ ┌──┴──┐          │  Framework       │
│Det.│ │Rel. │ │LLM  │          │  S3│ADLS│GCS│    │
│Pipe│ │Det. │ │Enr. │          │  SF │ PG        │
│line│ │     │ │     │          │  + Staging Dir   │
└────┘ └─────┘ └─────┘          │  (reuses same    │
                                 │   pipeline)      │
                                 └─────────────────┘
```

---

## Design Principles

- **Never trust the file extension.** Use content sniffing (magic bytes) to determine format.
- **Never load large files fully into memory.** Use lazy reads, chunked streaming, or DuckDB pushdown.
- **Be defensive at every layer.** Files are corrupted, partial, misformatted, and misrepresented far more often than database tables.
- **Tolerate partial corruption.** Log bad rows and continue — do not abort the entire profile.
- **Unified output.** Every source type produces the same JSON profile schema.
- **No LLM in the core pipeline.** Core profiling is pure deterministic logic (pattern matching, statistics, heuristics). LLM enrichment is an optional overlay.
- **Credentials never touch the LLM.** All credential flows bypass the chat agent, going directly from UI to encrypted storage via REST endpoints.

---

## System Components

### 1. MCP Servers (Dual Architecture)

Two independent FastMCP servers, each with its own tools, resources, and prompts. Both support stdio and SSE transports.

#### 1a. File Profiler Server (`file_profiler/mcp_server.py`, port 8080)

Handles local file profiling and the full pipeline for local data.

**Tools (13):** `profile_file`, `profile_directory`, `detect_relationships`, `enrich_relationships`, `check_enrichment_status`, `reset_vector_store`, `visualize_profile`, `list_supported_files`, `upload_file`, `get_quality_summary`, `query_knowledge_base`, `get_table_relationships`, `compare_profiles`

**Resources (2):** `profiles://{table_name}`, `relationships://latest`

**Prompts (3):** `summarize_profile`, `migration_readiness`, `quality_report`

**Caching:** LRU profile cache (200 entries), directory-level caching, relationship cache.

#### 1b. Data Connector Server (`file_profiler/connector_mcp_server.py`, port 8081)

Handles remote data sources (PostgreSQL, Snowflake, S3, ADLS Gen2, GCS). Runs the full end-to-end pipeline on remote data using a staging directory pattern — `profile_remote_source` materialises FileProfile objects to `OUTPUT_DIR/connectors/{connection_id}/`, then pipeline tools operate on that staging directory.

**Tools (16):** `connect_source`, `list_connections`, `test_connection`, `remove_connection`, `list_schemas`, `list_tables`, `profile_remote_source`, `remote_detect_relationships`, `remote_enrich_relationships`, `remote_check_enrichment_status`, `remote_reset_vector_store`, `remote_visualize_profile`, `remote_get_quality_summary`, `remote_query_knowledge_base`, `remote_get_table_relationships`, `remote_compare_profiles`

> Pipeline tools are prefixed with `remote_` to avoid name collisions when `MultiServerMCPClient` merges tools from both servers.

**Resources (2):** `connector-profiles://{table_name}`, `connector-relationships://latest`

**Prompts (3):** `summarize_profile`, `migration_readiness`, `quality_report`

**Caching:** LRU profile cache (200 entries), staging cache (connection_id -> FileProfile list), relationship cache.

### 2. LangGraph Agent (`file_profiler/agent/`)

Interactive chatbot, autonomous agent, and web server built on LangGraph.

| Module | Purpose | Status |
|--------|---------|--------|
| `chatbot.py` | Multi-turn interactive chat loop with message trimming (12K chars per ToolMessage) | Built |
| `graph.py` | ReAct-style StateGraph (agent ↔ tools loop) with enrichment status checking | Built |
| `cli.py` | Autonomous / human-in-the-loop CLI runner | Built |
| `state.py` | `AgentState` TypedDict with message history and mode tracking | Built |
| `llm_factory.py` | Multi-provider LLM factory (Google, Groq, OpenAI, Anthropic) with automatic fallback chain | Built |
| `enrichment.py` | RAG document builder (schemas + samples → LangChain Documents) | Built |
| `enrichment_mapreduce.py` | Multi-phase Map-Reduce enrichment pipeline (MAP, APPLY, EMBED, CLUSTER, REDUCE, META-REDUCE) | Built |
| `enrichment_progress.py` | IPC progress file + manifest file for enrichment tracking across restarts | Built |
| `vector_store.py` | ChromaDB persistent vector store with table fingerprinting and incremental updates | Built |
| `web_server.py` | FastAPI + WebSocket backend + REST API for connections + session endpoints | Built |
| `session_manager.py` | PostgreSQL session persistence (create/update/delete/list) | Built |
| `progress.py` | Terminal progress tracking (weighted spinner, bar, stage hints, smart summaries) | Built |

### 3. Web UI (`frontend/`)

Browser-based interface with WebSocket connectivity and secure credential management.

| Feature | Implementation |
|---------|---------------|
| Real-time progress | Animated progress bar + current step + live stats + per-table cards |
| Quick actions | Profile Directory, Detect Relationships, Enrich & Analyze, List Files |
| Markdown rendering | `marked.js` |
| ER diagrams | Inline Mermaid rendering with zoom/pan controls via `mermaid.js` |
| Chart display | Generated PNG images with dark/light theme variants |
| File upload | Drag-and-drop with multipart upload (max 500 MB) |
| Connection modal | Register/test/remove remote data sources (S3, ADLS, GCS, Snowflake, PostgreSQL) |
| Provider selection | Google, Groq, Anthropic, OpenAI sidebar selector |
| Themes | Dark/light toggle |
| Sessions | Persistent session history with PostgreSQL backend + conversation restore |

### 4. Connector Framework (`file_profiler/connectors/`)

Multi-source connector architecture for profiling remote data sources.

| Module | Purpose | Status |
|--------|---------|--------|
| `base.py` | `SourceDescriptor` dataclass, `BaseConnector` ABC, `RemoteObject`, `ConnectorError` | Built |
| `uri_parser.py` | Parse `s3://`, `abfss://`, `gs://`, `snowflake://`, `postgresql://` URIs | Built |
| `registry.py` | `ConnectorRegistry` with lazy loading (avoid importing heavy SDKs until needed) | Built |
| `connection_manager.py` | Credential store + resolution priority (connection_id → env vars → SDK defaults) | Built |
| `credential_store.py` | Fernet encryption at rest using `PROFILER_SECRET_KEY`, file-based persistence | Built |
| `cloud_storage.py` | S3/ADLS/GCS connector using DuckDB extensions + native SDK for listing | Built |
| `database.py` | PostgreSQL (DuckDB postgres_scanner) + Snowflake (native SDK) | Built |
| `duckdb_remote.py` | DuckDB in-memory connection with auto-loaded extensions, remote query helpers | Built |

### 5. Chart Generator (`file_profiler/output/chart_generator.py`)

| Chart Type | Description |
|------------|-------------|
| `null_distribution` | Per-column null ratios |
| `type_distribution` | Column type breakdown |
| `cardinality` | HIGH/MEDIUM/LOW distribution |
| `completeness` | Data completeness heatmap |
| `skewness` | Numeric column skew |
| `top_values` | Most frequent values per column |
| `string_lengths` | Length distribution (p10/p50/p90) |
| `row_counts` | Row counts across tables |
| `quality_heatmap` | Quality flags per table |
| `relationship_confidence` | FK candidate confidence scores |
| `overview` | Single-file summary dashboard |
| `overview_directory` | Multi-file summary dashboard |

Charts are rendered as PNG at `OUTPUT_DIR/charts/` with automatic cleanup (>24h files removed, 200 file cap).

### 6. Deterministic Pipeline (11 Layers)

Pure deterministic logic — no LLM. Pattern matching, statistics, and heuristics.

```
Layer 1   Intake Validator     →  file exists, readable, size, encoding
Layer 2   Format Classifier    →  CSV / Parquet / JSON / Excel / DB / Unknown
Layer 3   Size Strategy        →  MEMORY_SAFE (<100MB) / LAZY_SCAN (100MB-2GB) / STREAM_ONLY (>2GB)
Layer 4   Format Engine        →  csv / parquet / json / excel / db engine
Layer 5   Standardization      →  column name normalisation, null sentinel replacement
Layer 6   Column Profiler      →  null counts, distinct counts, top-N values, sample values
Layer 7   Type Inference       →  INTEGER / FLOAT / BOOLEAN / DATE / TIMESTAMP / UUID / STRING / CATEGORICAL / FREE_TEXT
Layer 8   Quality Checker      →  HIGH_NULL_RATIO, CONSTANT_COLUMN, TYPE_CONFLICT, STRUCTURAL_CORRUPTION
Layer 9   Relationship Detect  →  cross-table FK candidates (name + type + cardinality + value overlap)
Layer 10  Output Writers       →  JSON profiles, relationships.json, er_diagram.md, charts
Layer 11  MCP Server           →  tool handlers, caching, progress reporting
```

---

## Pipeline Detail

### Layer 1 — File Intake (`file_profiler/intake/validator.py`)

Validates the file is readable and well-formed before any profiling begins.

| Check | Failure Behavior |
|-------|-----------------|
| File exists | Raise `FileNotFoundError` |
| File size > 0 | Raise `EmptyFileError` |
| Encoding detection | Log, attempt UTF-8 fallback |
| Delimiter detection | Best-guess via content sniff |
| Compression check | Detect `.gz`, `.zip`, decompress before read |

### Layer 2 — Format Classification (`file_profiler/classification/classifier.py`)

Determines actual file format using content sniffing (magic bytes), not file extension.

| Format | Detection Signal |
|--------|-----------------|
| Parquet | Magic bytes `PAR1` at file start and end |
| JSON | Starts with `{` or `[`, or valid NDJSON |
| CSV | Consistent delimiter pattern across rows |
| Excel | OLE2 (.xls) or ZIP with `xl/workbook.xml` (.xlsx) |
| DuckDB/SQLite | Magic bytes `SQLite format 3` or DuckDB header |
| UNKNOWN | None of the above match — skip profiling |

### Layer 3 — Size Strategy (`file_profiler/strategy/size_strategy.py`)

| Strategy | File Size | Behavior |
|----------|-----------|----------|
| `MEMORY_SAFE` | < 100 MB | Full read into memory |
| `LAZY_SCAN` | 100 MB – 2 GB | Chunked reads, reservoir sampling |
| `STREAM_ONLY` | > 2 GB | Stream with skip-interval sampling, DuckDB pushdown |

### Layer 4 — Format Engines (`file_profiler/engines/`)

| Engine | Key Approach |
|--------|-------------|
| `csv_engine.py` | Structure detection → header detection → row count estimation → sampling (Vitter's Algorithm R) → column pivot. ZIP archive support (multi-CSV shards), gzip transparent decompression, DuckDB acceleration for >100K rows |
| `parquet_engine.py` | Schema from metadata (zero I/O) → nested struct flattening (underscore-joined paths) → row group sampling. Lists/maps serialized to JSON strings |
| `json_engine.py` | Shape detection (SINGLE_OBJECT, ARRAY_OF_OBJECTS, NDJSON, DEEP_NESTED) → union schema discovery → flatten strategy (HYBRID) → sampling |
| `excel_engine.py` | Sheet detection → named range handling → row sampling |
| `db_engine.py` | DuckDB/SQLite multi-table engine — enumerates tables, profiles each via DuckDB SQL |
| `duckdb_sampler.py` | DuckDB-based reservoir sampling and row counting for CSV/Parquet/JSON > 100K rows |

### Layers 6-7 — Column Profiling & Type Inference (`file_profiler/profiling/`)

**Metrics per column:** `null_count`, `distinct_count`, `unique_ratio`, `cardinality`, `min`, `max`, `skewness`, `avg_length`, `length_p10/p50/p90/max`, `top_values` (top 10), `sample_values` (5 raw values).

**Type inference order** (most specific to least):

| Priority | Type | Detection |
|----------|------|-----------|
| 1 | `NULL_ONLY` | All values null |
| 2 | `INTEGER` | Pattern `^-?\d+$` |
| 3 | `FLOAT` | Numeric with decimal point |
| 4 | `BOOLEAN` | Values in {true, false, 0, 1, yes, no} |
| 5 | `DATE` | ISO 8601 date patterns |
| 6 | `TIMESTAMP` | ISO 8601 datetime patterns |
| 7 | `UUID` | 8-4-4-4-12 hex pattern |
| 8 | `CATEGORICAL` | distinct/total < 10% AND distinct < 50 |
| 9 | `FREE_TEXT` | avg length > 100 chars |
| 10 | `STRING` | Default fallback |

### Layer 8 — Quality Checks (`file_profiler/quality/structural_checker.py`)

| Flag | Severity | Description |
|------|----------|-------------|
| `FULLY_NULL` | Critical | Every value in the column is null |
| `HIGH_NULL_RATIO` | Warning | > 70% null values |
| `CONSTANT_COLUMN` | Info | Only one distinct non-null value |
| `TYPE_CONFLICT` | Warning | Same column has mixed types |
| `MIXED_DATE_FORMATS` | Warning | Multiple date format patterns |
| `MIXED_TIMEZONES` | Warning | Inconsistent timezones |
| `DUPLICATE_COLUMN_NAME` | Critical | Two columns share the same name |
| `COLUMN_SHIFT_ERROR` | Critical | Row field count != header field count |
| `STRUCTURAL_CORRUPTION` | Critical | > 5% rows have structural issues |
| `NULL_VARIANT_NORMALIZED` | Info | Null sentinels converted to None |

### Layer 9 — Relationship Detection (`file_profiler/analysis/relationship_detector.py`)

Cross-table FK candidate scoring using four additive signals:

| Signal | Max Score | Evidence Codes |
|--------|-----------|----------------|
| Name match | 0.50 | `name:direct_prefix` (0.50), `name:singular_prefix` (0.45), `name:exact` (0.40), `name:embedded` (0.35) |
| Type compatibility | 0.20 | `type:exact` (0.20), `type:numeric_compat` (0.10), `type:string_compat` (0.05) |
| Cardinality | 0.25 | `pk:key_candidate` (0.20), `pk:high_unique` (0.15), `pk:soft_id` (0.10), `cardinality:fk_subset` (0.05) |
| Value overlap | 0.15 | `overlap:high` (>=80%, 0.15), `overlap:medium` (50-80%, 0.10) |

Relationships can also be discovered via vector similarity in the enrichment layer (marked with `signal_source: "vector_discovered"`).

---

## LLM Enrichment Layer (`file_profiler/agent/enrichment_mapreduce.py`)

Multi-phase Map-Reduce pipeline that uses LLMs to enrich the deterministic pipeline's output. This layer is **optional** — the deterministic pipeline produces complete results on its own.

### Architecture

```
Deterministic Pipeline Output
        │
        ▼
┌──────────────────────────────┐
│  Phase 0: PROFILE + DETECT   │
│  Full pipeline + FK candidates│
└──────────────┬───────────────┘
               │
               ▼
┌──────────────────────────────┐
│  Phase 1: MAP                │
│  Per-table LLM summaries:    │
│  - Table description         │
│  - Column descriptions       │
│  - Key observations          │
│  Parallel (8 workers default)│
│  Token budget: 2000/table    │
└──────────────┬───────────────┘
               │
               ▼
┌──────────────────────────────┐
│  Phase 2: APPLY              │
│  Write descriptions back     │
│  into profile JSON files     │
└──────────────┬───────────────┘
               │
               ▼
┌──────────────────────────────┐
│  Phase 3: EMBED              │
│  ChromaDB persistent store:  │
│  - nvidia/llama-3.2-        │
│    nemoretriever-300m-embed │
│  - Table fingerprinting      │
│  - Skip unchanged tables     │
│  - Enriched signals (sample  │
│    values, cardinality, tops)│
└──────────────┬───────────────┘
               │
               ▼
┌──────────────────────────────┐
│  Phase 4: DISCOVER + CLUSTER │
│  Column embedding similarity │
│  → DBSCAN clustering         │
│  → Derive new FK candidates  │
│  → Table affinity matrix     │
│  Threshold: 0.65             │
└──────────────┬───────────────┘
               │
               ▼
┌──────────────────────────────┐
│  Phase 5: REDUCE             │
│  Synthesized LLM analysis:   │
│  - Vector-discovered rels    │
│    prioritized over det.     │
│  - PK/FK reassessment        │
│  - JOIN recommendations      │
│  - Enriched ER diagram       │
│  Configurable stronger model │
│  Token budget: 12000         │
└──────────────┬───────────────┘
               │
               ▼
┌──────────────────────────────┐
│  Phase 6: META-REDUCE        │
│  (optional, large datasets)  │
│  Per-cluster + cross-cluster │
│  synthesis                   │
│  Token budget: 8000          │
└──────────────────────────────┘
```

### Enrichment Progress IPC (`enrichment_progress.py`)

Two files enable cross-process progress tracking:

| File | Purpose | Fields |
|------|---------|--------|
| `.enrichment_progress.json` | Per-phase progress (polled by web server at 1s intervals) | `step`, `name`, `detail`, `ts`, `stats` (tables_done, rows, columns, fk, profiles_preview) |
| `.enrichment_manifest.json` | Persistent completion state across restarts | `dir_path`, `table_fingerprints`, `enrichment_result` |

The manifest's fingerprints detect schema changes between runs, triggering re-enrichment only when data actually changes.

### Vector Store (`vector_store.py`)

- **Persistence:** ChromaDB at `OUTPUT_DIR/chroma_store` (not transient)
- **Embeddings:** `nvidia/llama-3.2-nemoretriever-300m-embed-v1` via NVIDIA OpenAI-compatible API
- **Fingerprinting:** `table_name + row_count + col_count` hash to detect stale summaries
- **Incremental:** Only changed tables are re-embedded on subsequent runs
- **Similarity search:** Used in DISCOVER phase to build affinity matrix and derive FK candidates

### LLM Factory (`llm_factory.py`)

Multi-provider LLM factory with automatic fallback:

| Provider | Default Model | Fallback |
|----------|---------------|----------|
| Google | `gemini-3.1-flash-lite-preview` | → Groq |
| Groq | `llama-3.3-70b-versatile` | — |
| OpenAI | `gpt-4o` | — |
| Anthropic | `claude-sonnet-4-20250514` | — |

`get_reduce_llm()` returns a separate (optionally stronger) LLM for REDUCE/META-REDUCE phases, configurable via `REDUCE_LLM_PROVIDER` and `REDUCE_LLM_MODEL` environment variables.

---

## Multi-Source Connector Architecture

### Connector Flow

```
User provides URI (e.g. "s3://bucket/data/")
        │
        ▼
┌──────────────────────┐
│  URI Parser           │
│  parse_uri(uri)       │
│  → SourceDescriptor   │
│    scheme, bucket,    │
│    path, connection_id│
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│  ConnectionManager    │
│  resolve_credentials()│
│  Priority:            │
│  1. connection_id     │
│  2. env vars          │
│  3. empty (SDK chain) │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│  ConnectorRegistry    │
│  registry.get(scheme) │
│  → BaseConnector      │
│  (lazy-loaded)        │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────────────────────┐
│  Connector (Cloud or Database)       │
│                                      │
│  Cloud (S3/ADLS/GCS):               │
│  ├─ configure_duckdb() → httpfs/azure│
│  ├─ list_objects() → native SDK      │
│  └─ duckdb_scan_expression()         │
│                                      │
│  Database (PG/Snowflake):            │
│  ├─ configure_duckdb() → pg_scanner  │
│  ├─ duckdb_scan_expression()         │
│  └─ snowflake: native SDK path       │
└──────────┬───────────────────────────┘
           │
           ▼
┌──────────────────────┐
│  DuckDB Remote Layer  │
│  remote_count()       │
│  remote_sample()      │
│  remote_schema()      │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│  Standard Pipeline    │
│  (enters at           │
│   RawColumnData level)│
│  → profiling          │
│  → quality checks     │
│  → output             │
└──────────────────────┘
```

### Credential Security Architecture

```
┌──────────────────────────────────────────────────────┐
│  FRONTEND (Browser)                                   │
│                                                      │
│  Connection Modal                                    │
│  ┌─────────────────────────────────┐                 │
│  │ Scheme: [S3 ▾]                  │                 │
│  │ Connection ID: [prod-s3      ]  │                 │
│  │ Access Key:    [AKIA...      ]  │                 │
│  │ Secret Key:    [••••••••     ]  │                 │
│  │ Region:        [us-east-1    ]  │                 │
│  │                                 │                 │
│  │ [Save]  [Save & Test]          │                 │
│  └──────────────┬──────────────────┘                 │
│                 │  POST /api/connections              │
│                 │  (REST — NOT WebSocket/chat)        │
└─────────────────┼────────────────────────────────────┘
                  │
                  ▼
┌──────────────────────────────────────────────────────┐
│  WEB SERVER (web_server.py)                           │
│                                                      │
│  /api/connections endpoints                          │
│  ├─ GET    → list (no secrets in response)           │
│  ├─ POST   → register + encrypt                     │
│  ├─ DELETE → remove + re-persist                     │
│  └─ POST /test → connector.test_connection()         │
│                                                      │
│  ┌─────────────────────────────────────┐             │
│  │  ConnectionManager                   │             │
│  │  ├─ register(id, scheme, creds)     │             │
│  │  ├─ get(id) → ConnectionInfo        │             │
│  │  ├─ test(id) → TestResult           │             │
│  │  └─ resolve_credentials(descriptor) │             │
│  └──────────────┬──────────────────────┘             │
│                 │                                     │
│  ┌──────────────┴──────────────────────┐             │
│  │  CredentialStore                     │             │
│  │  ├─ Fernet(SHA256(SECRET_KEY))      │             │
│  │  ├─ encrypt_credentials(dict)       │             │
│  │  ├─ decrypt_credentials(str)        │             │
│  │  ├─ save_to_file() → .connections.enc│            │
│  │  └─ load_from_file()               │             │
│  └─────────────────────────────────────┘             │
└──────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────┐
│                    SECURITY GUARANTEES                │
│                                                      │
│  ✓ LLM NEVER sees credentials                       │
│  ✓ Chat history has NO secrets                       │
│  ✓ LangGraph checkpoints store NO credential data    │
│  ✓ REST API list responses NEVER include secrets     │
│  ✓ Credentials encrypted at rest (Fernet)            │
│  ✓ No persistence without PROFILER_SECRET_KEY        │
└──────────────────────────────────────────────────────┘
```

---

## Data Flow

### Standard Flow (Local Files — Deterministic Only)

```
User: "Profile my data in ./data/files"
  │
  ├─ list_supported_files(./data/files)
  │    → [{file_name, format, size}, ...]
  │
  ├─ profile_directory(./data/files)
  │    → Layers 1-8 per file → [FileProfile, ...]
  │
  ├─ detect_relationships(./data/files)
  │    → Layer 9 → RelationshipReport + ER diagram
  │
  └─ Agent summarises findings
```

### Enriched Flow (Deterministic + Map-Reduce RAG)

```
User: "Profile my data in ./data/files"
  │
  ├─ list_supported_files(./data/files)
  │    → [{file_name, format, size}, ...]
  │
  ├─ check_enrichment_status(./data/files)
  │    → {status: "stale"} or {status: "complete"}
  │
  ├─ enrich_relationships(./data/files)
  │    │
  │    ├─ Phase 0: profile all files + detect relationships
  │    ├─ Phase 1 MAP: per-table LLM summaries (parallel)
  │    ├─ Phase 2 APPLY: write descriptions into profiles
  │    ├─ Phase 3 EMBED: store in ChromaDB (skip unchanged)
  │    ├─ Phase 4 DISCOVER+CLUSTER: affinity matrix + DBSCAN
  │    ├─ Phase 5 REDUCE: synthesized analysis (stronger model)
  │    ├─ Phase 6 META-REDUCE: cross-cluster synthesis (optional)
  │    └─ Write enrichment manifest for next run
  │
  └─ Agent presents enriched analysis + ER diagram
```

### Remote Source Flow

```
User: "Profile s3://my-bucket/data/"
  │
  ├─ profile_remote_source(uri="s3://my-bucket/data/", connection_id="prod-s3")
  │    │
  │    ├─ parse_uri() → SourceDescriptor(scheme="s3", bucket="my-bucket", ...)
  │    ├─ resolve_credentials() → {aws_access_key_id, aws_secret_access_key}
  │    ├─ CloudStorageConnector.list_objects() → [RemoteObject, ...]
  │    ├─ For each file:
  │    │   ├─ DuckDB: configure_duckdb() + duckdb_scan_expression()
  │    │   ├─ remote_count() + remote_sample() + remote_schema()
  │    │   └─ Enter pipeline at RawColumnData → profiling → quality → output
  │    └─ Returns [FileProfile, ...]
  │
  └─ Agent summarises findings
```

---

## Key Data Models

### FileProfile (`file_profiler/models/file_profile.py`)

```
FileProfile
├── source_type: "file" | "database" | "remote_storage" | "remote_database"
├── file_format: CSV | Parquet | JSON | Excel | DuckDB | SQLite
├── file_path: str
├── table_name: str  (derived from filename stem)
├── row_count: int
├── is_row_count_exact: bool
├── encoding: str
├── size_bytes: int
├── size_strategy: MEMORY_SAFE | LAZY_SCAN | STREAM_ONLY
├── corrupt_row_count: int
├── source_uri: Optional[str]       # For remote sources
├── connection_id: Optional[str]    # Links to stored credentials
├── columns: [ColumnProfile]
│   ├── name, declared_type, inferred_type, confidence_score
│   ├── null_count, distinct_count, unique_ratio, cardinality
│   ├── is_key_candidate, is_low_cardinality, is_nullable, is_constant, is_sparse
│   ├── min, max, skewness (numeric)
│   ├── avg_length, length_p10/p50/p90/max (string)
│   ├── top_values: [{value, count}]  (top 10)
│   ├── sample_values: [str]  (5 raw values)
│   ├── quality_flags: [QualityFlag]
│   ├── semantic_type: str | None  (from LLM enrichment)
│   └── description: str | None  (from LLM enrichment)
├── structural_issues: [str]
├── standardization_applied: bool
└── quality_summary: QualitySummary
    ├── columns_profiled, columns_with_issues
    ├── null_heavy_columns, type_conflict_columns
    └── corrupt_rows_detected
```

### SourceDescriptor (`file_profiler/connectors/base.py`)

```
SourceDescriptor
├── scheme: str             # "s3", "abfss", "gs", "snowflake", "postgresql"
├── bucket_or_host: str
├── path: str
├── raw_uri: str
├── connection_id: Optional[str]
├── database: Optional[str]     # Snowflake/PostgreSQL
├── schema_name: Optional[str]  # Snowflake
├── table: Optional[str]        # Snowflake/PostgreSQL
├── container: Optional[str]    # ADLS
├── storage_account: Optional[str]  # ADLS
├── is_remote: bool
├── is_object_storage: bool
├── is_database: bool
└── is_directory_like: bool
```

---

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PROFILER_DATA_DIR` | `./data/files` | Root data directory |
| `PROFILER_UPLOAD_DIR` | `./data/uploads` | Upload staging area |
| `PROFILER_OUTPUT_DIR` | `./data/output` | Profile output directory |
| `PROFILER_VECTOR_STORE_DIR` | `{OUTPUT_DIR}/chroma_store` | ChromaDB persistence |
| `PROFILER_SECRET_KEY` | — | Passphrase for Fernet credential encryption |
| `MCP_TRANSPORT` | `sse` | Transport: `stdio`, `sse` |
| `MCP_HOST` | `0.0.0.0` | Server bind address |
| `MCP_PORT` | `8080` | Server port |
| `LLM_PROVIDER` | `google` | Provider: `google`, `groq`, `openai`, `anthropic` |
| `REDUCE_LLM_PROVIDER` | — | Separate provider for REDUCE/META-REDUCE |
| `POSTGRES_HOST` / `POSTGRES_PORT` / `POSTGRES_DB` | localhost:5432/profiler | Chat persistence database |

---

## Project Structure

```
file_profiler/
├── __init__.py
├── __main__.py              # Entry point → mcp_server.main()
├── main.py                  # Pipeline orchestrator (local + remote)
├── mcp_server.py            # File Profiler MCP server (13 tools, :8080)
├── connector_mcp_server.py  # Data Connector MCP server (16 tools, :8081)
│
├── agent/                   # LangGraph agent + chatbot + web UI
│   ├── chatbot.py           # Interactive multi-turn chatbot
│   ├── graph.py             # ReAct StateGraph with enrichment status check
│   ├── cli.py               # Autonomous / interactive CLI
│   ├── state.py             # AgentState TypedDict
│   ├── llm_factory.py       # Multi-provider LLM factory with fallback
│   ├── enrichment.py        # RAG document builder
│   ├── enrichment_mapreduce.py  # Multi-phase Map-Reduce pipeline
│   ├── enrichment_progress.py   # IPC progress + manifest files
│   ├── vector_store.py      # ChromaDB persistent store with fingerprinting
│   ├── web_server.py        # FastAPI + WebSocket + REST /api/connections
│   ├── session_manager.py   # PostgreSQL session persistence
│   └── progress.py          # Terminal progress (spinner, bar, summaries)
│
├── connectors/              # Multi-source connector framework
│   ├── __init__.py          # Public API exports
│   ├── __main__.py          # python -m file_profiler.connectors entry point
│   ├── base.py              # SourceDescriptor, BaseConnector ABC
│   ├── uri_parser.py        # URI parsing for all schemes
│   ├── registry.py          # Lazy-loaded connector registry
│   ├── connection_manager.py # Credential store + resolution
│   ├── credential_store.py  # Fernet encryption at rest
│   ├── cloud_storage.py     # S3/ADLS/GCS connector
│   ├── database.py          # PostgreSQL/Snowflake connector
│   └── duckdb_remote.py     # DuckDB remote connection helpers
│
├── config/
│   ├── env.py               # Environment-based config
│   ├── settings.py          # Tuning constants
│   └── database.py          # PostgreSQL checkpointer + pool
│
├── engines/                 # Format-specific engines
│   ├── csv_engine.py
│   ├── parquet_engine.py
│   ├── json_engine.py
│   ├── excel_engine.py
│   ├── db_engine.py         # DuckDB/SQLite multi-table
│   └── duckdb_sampler.py
│
├── models/
│   ├── file_profile.py      # FileProfile (+ source_uri, connection_id)
│   ├── relationships.py
│   └── enums.py             # FileFormat, QualityFlag, SizeStrategy, SourceType
│
├── intake/ → classification/ → strategy/ → standardization/
├── profiling/ → quality/ → analysis/ → output/
└── utils/

frontend/                    # Web UI
├── index.html               # Chat + connection modal
├── app.js                   # WebSocket + REST connection management
└── style.css                # Dark/light themes + modal styles
```

---

## Implementation Status

| Component | Status |
|-----------|--------|
| File Intake Validator (Layer 1) | Built |
| File Type Classifier (Layer 2) | Built |
| Size Strategy Selector (Layer 3) | Built |
| CSV Profiling Engine (Layer 4) | Built |
| Parquet Profiling Engine (Layer 4) | Built |
| JSON Profiling Engine (Layer 4) | Built |
| Excel Profiling Engine (Layer 4) | Built |
| DuckDB/SQLite Engine (Layer 4) | Built |
| DuckDB Sampler (Layer 4) | Built |
| Standardization (Layer 5) | Built |
| Column Profiler (Layer 6) | Built |
| Type Inference (Layer 7) | Built |
| Structural Quality Checker (Layer 8) | Built |
| Relationship Detector (Layer 9) | Built |
| Profile Writer (Layer 10) | Built |
| ER Diagram Writer (Layer 10) | Built |
| Chart Generator (Layer 10) | Built |
| MCP Servers — File Profiler (13 tools) + Data Connector (16 tools) | Built |
| LangGraph Agent + Chatbot | Built |
| Web UI (FastAPI + WebSocket) | Built |
| Map-Reduce Enrichment Pipeline | Built |
| Persistent Vector Store | Built |
| Enrichment Progress IPC | Built |
| Multi-Provider LLM Factory | Built |
| Chat Persistence (PostgresSaver) | Built |
| Session Management | Built |
| Connector Framework (base, URI parser, registry) | Built |
| Cloud Storage Connector (S3/ADLS/GCS) | Built |
| Database Connector (PostgreSQL/Snowflake) | Built |
| DuckDB Remote Layer | Built |
| Secure Credential Management (Fernet encryption) | Built |
| REST API for Connections | Built |
| Frontend Connection Modal | Built |
| Docker Packaging | Built |
| Column-level DBSCAN enrichment redesign | In progress — design phase |
| Authentication layer (OAuth/JWT) | Future |
| Prometheus metrics | Future |
| Structured JSON logging | Future |
| Upload cleanup background task | Future |
| Rate limiting (per-client) | Future |
