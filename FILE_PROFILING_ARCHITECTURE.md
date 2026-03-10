# File Profiling Architecture

## Overview

The **Agentic Data Profiler** is a production-grade data profiling engine for tabular data (CSV, Parquet, JSON, Excel). It combines a deterministic 11-layer pipeline with LLM-powered enrichment to produce comprehensive data profiles, relationship maps, and ER diagrams.

The output is **format-agnostic**: regardless of whether the source was CSV or Parquet, the final profile object is identical, allowing downstream logic to remain source-unaware.

```
                          User / Chatbot
                               │
                    ┌──────────┴──────────┐
                    │   LangGraph Agent    │  ← Interactive chatbot (Gemini 2.5 Flash)
                    │   (multi-turn chat)  │
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

---

## Design Principles

- **Never trust the file extension.** Use content sniffing (magic bytes) to determine format.
- **Never load large files fully into memory.** Use lazy reads, chunked streaming, or DuckDB pushdown.
- **Be defensive at every layer.** Files are corrupted, partial, misformatted, and misrepresented far more often than database tables.
- **Tolerate partial corruption.** Log bad rows and continue — do not abort the entire profile.
- **Unified output.** Every file type produces the same JSON profile schema.
- **No LLM in the pipeline.** Core profiling is pure deterministic logic (pattern matching, statistics, heuristics). LLM enrichment is an optional overlay.

---

## System Components

### 1. MCP Server (`file_profiler/mcp_server.py`)

FastMCP server exposing the profiler as standardised tools. Supports stdio (local) and SSE (remote) transports.

**Tools (7):**

| Tool | Description | Status |
|------|-------------|--------|
| `profile_file` | Profile a single file through the full 11-layer pipeline | Built |
| `profile_directory` | Profile all supported files in a directory | Built |
| `detect_relationships` | Detect FK relationships + generate ER diagram (deterministic) | Built |
| `enrich_relationships` | Full pipeline + RAG + LLM enrichment (descriptions, PK/FK reassessment, join recommendations, enriched ER diagram) | Built |
| `list_supported_files` | Scan a directory for supported data files | Built |
| `upload_file` | Upload a base64-encoded file for profiling | Built |
| `get_quality_summary` | Quality summary for a specific file | Built |

**Resources (2):** `profiles://{table_name}`, `relationships://latest`

**Prompts (3):** `summarize_profile`, `migration_readiness`, `quality_report`

### 2. LangGraph Agent (`file_profiler/agent/`)

Interactive chatbot and autonomous agent built on LangGraph.

| Module | Purpose | Status |
|--------|---------|--------|
| `chatbot.py` | Multi-turn interactive chat loop with streaming | Built |
| `graph.py` | ReAct-style StateGraph (agent ↔ tools loop) | Built |
| `cli.py` | Autonomous / human-in-the-loop CLI runner | Built |
| `state.py` | `AgentState` TypedDict with message history | Built |
| `llm_factory.py` | Multi-provider LLM factory (Google, Groq, OpenAI, Anthropic) with automatic fallback | Built |
| `enrichment.py` | RAG enrichment layer (ChromaDB + LLM analysis) | Built |
| `progress.py` | Terminal progress tracking (spinner, bar, summaries) | Built |

### 3. Deterministic Pipeline (11 Layers)

Pure deterministic logic — no LLM. Pattern matching, statistics, and heuristics.

```
Layer 1   Intake Validator     →  file exists, readable, size, encoding
Layer 2   Format Classifier    →  CSV / Parquet / JSON / Excel / Unknown
Layer 3   Size Strategy        →  MEMORY_SAFE (<100MB) / LAZY_SCAN (100MB-2GB) / STREAM_ONLY (>2GB)
Layer 4   Format Engine        →  csv_engine / parquet_engine / json_engine / excel_engine
Layer 5   Standardization      →  column name normalisation, null sentinel replacement
Layer 6   Column Profiler      →  null counts, distinct counts, top-N values, sample values
Layer 7   Type Inference       →  INTEGER / FLOAT / BOOLEAN / DATE / TIMESTAMP / UUID / STRING / CATEGORICAL / FREE_TEXT
Layer 8   Quality Checker      →  HIGH_NULL_RATIO, CONSTANT_COLUMN, TYPE_CONFLICT, STRUCTURAL_CORRUPTION
Layer 9   Relationship Detect  →  cross-table FK candidates (name + type + cardinality + value overlap)
Layer 10  Output Writers       →  JSON profiles, relationships.json, er_diagram.md
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

Critical edge cases: corrupted files, binary files with `.csv` extension, BOM characters, UTF-16 encoding.

### Layer 2 — Format Classification (`file_profiler/classification/classifier.py`)

Determines actual file format using content sniffing (magic bytes), not file extension.

| Format | Detection Signal |
|--------|-----------------|
| Parquet | Magic bytes `PAR1` at file start and end |
| JSON | Starts with `{` or `[`, or valid NDJSON |
| CSV | Consistent delimiter pattern across rows |
| Excel | OLE2 or ZIP (XLSX) magic bytes |
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
| `csv_engine.py` | Structure detection → header detection → row count estimation → sampling → column pivot |
| `parquet_engine.py` | Schema from metadata (zero I/O) → row group sampling (Vitter's Algorithm R) |
| `json_engine.py` | Shape detection → union schema discovery → flatten strategy (EXPLODE / STRINGIFY / HYBRID) |
| `excel_engine.py` | Sheet iteration → row sampling |
| `duckdb_sampler.py` | DuckDB-based reservoir sampling for >2GB files |

### Layer 5 — Standardization (`file_profiler/standardization/normalizer.py`)

- Column name normalisation (lowercase, underscores)
- Null sentinel replacement ("NULL", "n/a", "nil", etc. → `None`)
- Stores `original_name` for reverse mapping

### Layer 6-7 — Column Profiling & Type Inference (`file_profiler/profiling/`)

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
| Cardinality | 0.25 | `pk:key_candidate` (0.20), `pk:high_unique` (0.15), `cardinality:fk_subset` (0.05) |
| Value overlap | 0.15 | `overlap:high` (>=80%, 0.15), `overlap:medium` (50-80%, 0.10) |

Confidence is the sum of matched signals, capped at 1.0. Minimum threshold: 0.30 (configurable).

### Layer 10 — Output Writers (`file_profiler/output/`)

| Writer | Output | Format |
|--------|--------|--------|
| `profile_writer.py` | `{table_name}_profile.json` | Unified JSON schema |
| `relationship_writer.py` | `relationships.json` | FK candidates with evidence |
| `er_diagram_writer.py` | `er_diagram.md` | Mermaid erDiagram |

---

## LLM Enrichment Layer (`file_profiler/agent/enrichment.py`)

RAG-based "second opinion" that uses an LLM to enrich the deterministic pipeline's output. This layer is **optional** — the deterministic pipeline produces complete results on its own.

### Architecture

```
Deterministic Pipeline Output
        │
        ▼
┌──────────────────────────┐
│   Document Builder        │
│                           │
│  Per-table documents:     │
│  - Column schemas + types │
│  - Quality flags          │
│  - Low-cardinality cols   │
│    with 15 sample values  │
│  - 10 actual sample rows  │
│    read from source file  │
│                           │
│  Relationship document:   │
│  - All FK candidates +    │
│    evidence + confidence  │
│                           │
│  Quality overview:        │
│  - Aggregate metrics per  │
│    table                  │
└────────────┬─────────────┘
             │
             ▼
┌──────────────────────────┐
│   ChromaDB Vector Store   │
│                           │
│  Embeddings:              │
│  - HuggingFace            │
│    all-MiniLM-L6-v2       │
│    (local, free, fast)    │
│                           │
│  Transient collection —   │
│  created per enrichment,  │
│  deleted after analysis   │
└────────────┬─────────────┘
             │  full context retrieval
             │  (or similarity search
             │   for >50 tables)
             ▼
┌──────────────────────────┐
│   LLM Analysis            │
│   (Gemini 2.5 Flash)      │
│                           │
│  Produces:                │
│  1. Table descriptions    │
│     (semantic meaning)    │
│  2. Column descriptions   │
│     (key columns)         │
│  3. PK assessment         │
│     (confirm/revise)      │
│  4. FK reassessment +     │
│     new FK suggestions    │
│  5. JOIN type recs         │
│     (INNER/LEFT/etc)      │
│  6. Join path recs        │
│     (analytical queries)  │
│  7. Enriched ER diagram   │
│     (Mermaid + labels)    │
│  8. Quality remediations  │
└──────────────────────────┘
```

### What Gets Embedded

| Data | Source | Purpose |
|------|--------|---------|
| Column schemas | `ColumnProfile` fields | Types, cardinality, key candidates, flags |
| Low-cardinality values | `top_values` (up to 15 per column) | Understand categorical columns (e.g. gender codes, status values) |
| Sample rows | Source file via PyArrow/CSV (10 rows) | Real data context — lets the LLM see actual values across columns together |
| Relationships | `ForeignKeyCandidate` objects | FK/PK pairs with confidence scores and evidence codes |
| Quality summary | `QualitySummary` per table | Aggregate quality metrics for recommendations |

### Why Sample Rows Matter

The deterministic pipeline stores `sample_values` per column (5 values each), but these are **column-isolated** — you can't see which values co-occur in the same row. The enrichment layer reads 10 actual rows from the source file, giving the LLM **row-level context**. This enables:

- Better understanding of what each table represents (e.g. seeing `person_id=1, gender_concept_id=8507, year_of_birth=1963` together)
- More accurate relationship detection (seeing how FK values correspond across tables)
- Richer semantic descriptions

### Why a Vector Store

- For small datasets (< 50 tables), all documents are retrieved — the vector store acts as an embedding cache for the structured context.
- For larger datasets (50+ tables), similarity search retrieves the most relevant table contexts, keeping the LLM prompt within token limits.
- The collection is **transient** — created per enrichment run, deleted afterward. No persistent state to manage.

---

## Data Flow

### Standard Flow (Deterministic Only)

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

### Enriched Flow (Deterministic + LLM RAG)

```
User: "Profile my data in ./data/files"
  │
  ├─ list_supported_files(./data/files)
  │    → [{file_name, format, size}, ...]
  │
  ├─ enrich_relationships(./data/files)
  │    │
  │    ├─ Layers 1-8: profile all files
  │    ├─ Layer 9: detect relationships (deterministic)
  │    ├─ Extract 10 sample rows per table from source files
  │    ├─ Build documents (schemas + samples + relationships + quality)
  │    ├─ Embed into ChromaDB (text-embedding-004)
  │    ├─ Retrieve context (full or similarity-based)
  │    ├─ LLM analysis (Gemini 2.5 Flash)
  │    │    → Descriptions, PK/FK reassessment, join paths, enriched ER diagram
  │    └─ Cleanup transient vector store
  │
  └─ Agent presents enriched analysis + ER diagram
```

---

## Key Data Models

### FileProfile (`file_profiler/models/file_profile.py`)

```
FileProfile
├── source_type: "file"
├── file_format: CSV | Parquet | JSON | Excel
├── file_path: str
├── table_name: str  (derived from filename stem)
├── row_count: int
├── is_row_count_exact: bool
├── encoding: str
├── size_bytes: int
├── size_strategy: MEMORY_SAFE | LAZY_SCAN | STREAM_ONLY
├── corrupt_row_count: int
├── columns: [ColumnProfile]
│   ├── name, declared_type, inferred_type, confidence_score
│   ├── null_count, distinct_count, unique_ratio, cardinality
│   ├── is_key_candidate, is_low_cardinality, is_nullable, is_constant, is_sparse
│   ├── min, max, skewness (numeric)
│   ├── avg_length, length_p10/p50/p90/max (string)
│   ├── top_values: [{value, count}]  (top 10)
│   ├── sample_values: [str]  (5 raw values)
│   ├── quality_flags: [QualityFlag]
│   └── semantic_type: str | None  (reserved for intelligence layer)
├── structural_issues: [str]
├── standardization_applied: bool
└── quality_summary: QualitySummary
    ├── columns_profiled, columns_with_issues
    ├── null_heavy_columns, type_conflict_columns
    └── corrupt_rows_detected
```

### RelationshipReport (`file_profiler/models/relationships.py`)

```
RelationshipReport
├── tables_analyzed: int
├── columns_analyzed: int
└── candidates: [ForeignKeyCandidate]  (sorted by confidence desc)
    ├── fk: ColumnRef(table_name, column_name)
    ├── pk: ColumnRef(table_name, column_name)
    ├── confidence: float  (0.0–1.0, additive scoring)
    ├── evidence: [str]    ("name:exact", "type:exact", "pk:key_candidate", ...)
    ├── fk_null_ratio: float
    ├── fk_distinct_count: int
    ├── pk_distinct_count: int
    └── top_value_overlap_pct: float | None
```

---

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PROFILER_DATA_DIR` | `/data` | Root data directory |
| `PROFILER_UPLOAD_DIR` | `{DATA_DIR}/uploads` | Upload staging area |
| `PROFILER_OUTPUT_DIR` | `{DATA_DIR}/output` | Profile output directory |
| `MCP_TRANSPORT` | `stdio` | Transport: `stdio`, `sse`, `streamable-http` |
| `MCP_HOST` | `0.0.0.0` | Server bind address |
| `MCP_PORT` | `8080` | Server port |
| `MAX_PARALLEL_WORKERS` | `4` | Parallel profiling workers |
| `LLM_PROVIDER` | `anthropic` | LLM provider: `anthropic`, `openai`, `google`, `groq` |
| `LLM_MODEL` | (per provider) | Model override |
| `GOOGLE_API_KEY` | — | Required for Google/Gemini provider |
| `GROQ_API_KEY` | — | Required for Groq provider (automatic fallback from Google) |
| `GROQ_MODEL` | — | Groq model override (default: `llama-3.3-70b-versatile`) |

### Tuning Constants (`file_profiler/config/settings.py`)

| Constant | Value | Description |
|----------|-------|-------------|
| `MEMORY_SAFE_MAX_BYTES` | 100 MB | In-memory processing threshold |
| `LAZY_SCAN_MAX_BYTES` | 2 GB | Lazy scanning threshold |
| `SAMPLE_ROW_COUNT` | 10,000 | Rows sampled for profiling |
| `TOP_N_VALUES` | 10 | Top frequent values per column |
| `SAMPLE_VALUES_COUNT` | 5 | Raw sample values per column |
| `NULL_HEAVY_THRESHOLD` | 0.70 | Null ratio flagging threshold |
| `CATEGORICAL_MAX_DISTINCT` | 50 | Max distinct for CATEGORICAL |
| `CARDINALITY_HIGH_THRESHOLD` | 0.90 | unique_ratio > this → HIGH |
| `CARDINALITY_LOW_THRESHOLD` | 0.10 | unique_ratio <= this → LOW |

---

## Running the System

### MCP Server

```bash
conda activate gen_ai

# stdio (local, for Claude Desktop / Claude Code)
python -m file_profiler --transport stdio

# SSE (for chatbot / remote agents)
set PROFILER_DATA_DIR=C:\path\to\data
python -m file_profiler --transport sse --port 8080
```

### Interactive Chatbot

```bash
# Start MCP server first (Terminal 1), then:
python -m file_profiler.agent --chat --provider google
```

### Autonomous Agent

```bash
python -m file_profiler.agent --data-path ./data/files --provider google
```

### Direct Python API

```python
from file_profiler.main import profile_file, profile_directory, analyze_relationships

# Profile a single file
profile = profile_file("data/files/person.parquet", output_dir="data/output")

# Profile a directory
profiles = profile_directory("data/files", output_dir="data/output")

# Detect relationships
report = analyze_relationships(profiles, output_path="data/output/relationships.json")
```

---

## Project Structure

```
file_profiler/
├── __init__.py
├── __main__.py              # Entry point → mcp_server.main()
├── main.py                  # Pipeline orchestrator
├── mcp_server.py            # MCP server (7 tools, 2 resources, 3 prompts)
│
├── agent/                   # LangGraph agent + chatbot
│   ├── chatbot.py           # Interactive multi-turn chatbot
│   ├── graph.py             # ReAct StateGraph
│   ├── cli.py               # Autonomous / interactive CLI
│   ├── state.py             # AgentState TypedDict
│   ├── llm_factory.py       # Multi-provider LLM factory
│   └── enrichment.py        # RAG enrichment (ChromaDB + LLM)
│
├── config/
│   ├── env.py               # Environment-based config
│   └── settings.py          # Tuning constants
│
├── intake/
│   └── validator.py          # Layer 1: file validation
│
├── classification/
│   └── classifier.py         # Layer 2: format detection
│
├── strategy/
│   └── size_strategy.py      # Layer 3: size strategy selection
│
├── engines/                  # Layer 4: format-specific engines
│   ├── csv_engine.py
│   ├── parquet_engine.py
│   ├── json_engine.py
│   ├── excel_engine.py
│   └── duckdb_sampler.py
│
├── standardization/          # Layer 5: normalisation
│   └── normalizer.py
│
├── profiling/                # Layers 6-7: profiling + type inference
│   ├── column_profiler.py
│   └── type_inference.py
│
├── quality/                  # Layer 8: quality checks
│   └── structural_checker.py
│
├── analysis/                 # Layer 9: relationship detection
│   └── relationship_detector.py
│
├── output/                   # Layer 10: serialisation
│   ├── profile_writer.py
│   ├── relationship_writer.py
│   └── er_diagram_writer.py
│
├── models/                   # Data models
│   ├── file_profile.py
│   ├── relationships.py
│   └── enums.py
│
└── utils/
    ├── file_resolver.py
    └── logging_setup.py
```

---

## Dependencies

```toml
# Core pipeline
pyarrow >= 21.0.0           # Parquet engine
chardet >= 5.2.0             # Encoding detection
mcp[cli] >= 1.0.0            # MCP server framework

# Agent + chatbot
langgraph >= 1.0.0
langchain-core >= 1.2.0
langchain-mcp-adapters >= 0.2.0

# LLM providers (pick one or more)
langchain-google-genai >= 4.2.0   # Gemini (default for chatbot)
langchain-groq >= 1.0.0           # Groq (fallback from Google)
langchain-anthropic >= 1.0.0      # Claude
langchain-openai >= 0.3.0         # OpenAI

# RAG enrichment
chromadb >= 1.5.0
langchain-chroma >= 1.1.0
langchain-huggingface >= 0.1.0    # HuggingFace embeddings (all-MiniLM-L6-v2)
sentence-transformers >= 3.0.0    # Local embedding model

# Dev
pytest >= 8.4.0
pytest-asyncio >= 0.24.0
duckdb >= 1.4.0
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
| DuckDB Sampler (Layer 4) | Built |
| Standardization (Layer 5) | Built |
| Column Profiler (Layer 6) | Built |
| Type Inference (Layer 7) | Built |
| Structural Quality Checker (Layer 8) | Built |
| Relationship Detector (Layer 9) | Built |
| Profile Writer (Layer 10) | Built |
| ER Diagram Writer (Layer 10) | Built |
| MCP Server (Layer 11) | Built |
| LangGraph Agent + Chatbot | Built |
| LLM Enrichment / RAG Layer | Built |
| Multi-file partition support | Not built |
| Schema drift detection | Not built |
| Legacy flat file handler | Not built |
| Docker packaging | Not built |
