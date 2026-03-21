# Data Profiler — Current System Design

## 1. Executive Summary

The Data Profiler is a robust, Python-based engine designed to analyze raw file-based data sources (CSV, Parquet, JSON, Excel, DuckDB/SQLite) and remote data sources (S3, ADLS Gen2, GCS, Snowflake, PostgreSQL). It generates standardized, format-agnostic statistical profiles. Unlike traditional database profilers, this system emphasizes defensive coding, content-aware format detection, memory-safe processing strategies, and secure multi-source connectivity to handle the chaotic nature of file exports (corruption, missing headers, mixed types).

The system outputs a unified JSON schema for every source, enabling downstream systems to consume metadata without needing to understand the underlying source format or location.

> **Note:** This document covers the deterministic pipeline design and multi-source connector architecture. For the full system architecture including MCP server, LangGraph agent, Map-Reduce enrichment, web UI, and deployment, see [FILE_PROFILING_ARCHITECTURE.md](FILE_PROFILING_ARCHITECTURE.md) and [README.md](README.md).

---

## 2. Architectural Patterns

The system follows a **Layered Pipeline Architecture**. Each file passes through a strict sequence of processing layers, where each layer is responsible for a specific transformation or decision. Remote sources enter the pipeline at the RawColumnData level, bypassing intake/classify/strategy layers.

### Pipeline Flow (Local Files)

1.  **Intake Layer**: Validates file existence and readability.
2.  **Classification Layer**: Determines the true file format (ignoring extensions).
3.  **Strategy Layer**: Selects the I/O strategy (Memory vs. Stream) based on size.
4.  **Engine Layer**: Format-specific parsing (CSV/Parquet/JSON/Excel/DB) to extract raw columns.
5.  **Standardization Layer**: Normalizes column names, cleans nulls, and standardizes booleans.
6.  **Profiling Layer**: Computes statistics and infers data types.
7.  **Quality Layer**: Checks for structural corruption and integrity issues.
8.  **Output Layer**: Serializes the `FileProfile` object to JSON.

### Pipeline Flow (Remote Sources)

1.  **URI Parsing**: Parse scheme, bucket/host, path from URI.
2.  **Credential Resolution**: Connection ID → env vars → SDK defaults.
3.  **DuckDB Remote Layer**: Configure DuckDB extensions, scan remote data.
4.  **RawColumnData Entry**: Results enter the standard pipeline at Layer 5+.
5.  **Profiling → Quality → Output**: Same layers as local files.

---

## 3. Component Design Details

### 3.1. Orchestration (`main.py`)

The orchestrator serves as the entry point (`run`, `profile_file`, `profile_directory`, `profile_remote`). It manages the lifecycle of a profiling job and handles high-level error boundaries.

*   **Directory Scanning**: Non-recursive scanning of supported extensions (`.csv`, `.parquet`, `.json`, `.xlsx`, `.xls`, `.gz`, `.zip`, `.duckdb`, `.db`, `.sqlite`).
*   **Fault Tolerance**: Individual file failures (corruption, empty files) are logged and skipped, preventing batch job failures.
*   **Pipeline Wiring**: Connects the output of the Intake layer to the input of the Classifier, and so on.
*   **Remote Profiling**: `profile_remote()` handles cloud storage and database sources, routing through the connector framework.

### 3.2. File Type Classification (`classifier.py`)

We do not trust file extensions. The classifier uses **Content Sniffing** (Magic Bytes) to determine the actual format.

*   **Parquet**: Checks for `PAR1` magic bytes at the start (and end, if uncompressed) of the file.
*   **Excel**:
    *   **XLS**: Checks for OLE2 compound document magic (`\xd0\xcf\x11...`).
    *   **XLSX**: Inspects ZIP structure for `xl/workbook.xml` and `[Content_Types].xml`.
*   **JSON**: Checks if stripped content starts with `{` (Object) or `[` (Array), or validates NDJSON structure.
*   **CSV**: Fallback for clean text content that doesn't match the above.
*   **Compression Handling**: Transparently peeks inside `.gz` and `.zip` files to classify the payload.

### 3.3. Read Strategy Selection (`size_strategy.py`)

To prevent Out-Of-Memory (OOM) errors, the system selects a read strategy before loading data.

*   **Effective Size Calculation**:
    *   **Uncompressed**: Uses file size.
    *   **Zip**: Sums uncompressed size of entries in the central directory.
    *   **Gzip**: Reads the `ISIZE` field (last 4 bytes). If `ISIZE` is 0 (wrapped >4GB), applies a **5x safety expansion factor**.
*   **Strategies**:
    *   `MEMORY_SAFE`: Load fully into memory (Small files).
    *   `LAZY_SCAN`: Use chunked readers or lazy frames (Medium files).
    *   `STREAM_ONLY`: Single-pass streaming (Large files).

### 3.4. Type Inference Engine (`type_inference.py`)

The system infers the semantic type of a column based on sampled values. It uses a **Confidence Threshold (90%)**—if 90% of non-null values match a pattern, the type is accepted.

**Detection Order (Specific → Generic):**
1.  **Null Only**: 100% nulls.
2.  **Boolean**: Matches tokens `{true, false, yes, no, 1, 0, t, f}` (case-insensitive).
3.  **Date**: Matches ISO and common formats (e.g., `YYYY-MM-DD`, `MM/DD/YYYY`).
4.  **Timestamp**: Matches datetime patterns, detecting Timezone-Aware vs. Naive.
5.  **Integer**: Whole numbers (excludes leading zeros to protect Zip Codes/IDs).
6.  **Float**: Decimals and scientific notation.
7.  **UUID**: Standard 8-4-4-4-12 hex format.
8.  **Categorical**: Low cardinality distinct count.
9.  **Free Text**: High average string length.
10. **String**: Fallback.

**Quality Flags**:
*   `MIXED_DATE_FORMATS`: Column contains multiple date patterns (e.g., `YYYY-MM-DD` and `MM/DD/YYYY`).
*   `MIXED_TIMEZONES`: Column contains both TZ-aware and TZ-naive timestamps.

### 3.5. Standardization Layer (`normalizer.py`)

Data is cleaned *before* final profiling to ensure accurate statistics.

*   **Name Normalization**: Converts headers to `snake_case`, handles special characters, and deduplicates colliding names (e.g., `Name` and `name` become `name` and `name_2`).
*   **Null Standardization**: Converts sentinels like `"NULL"`, `"N/A"`, `"nan"`, `"-"`, `""` into native Python `None`.
*   **Boolean Normalization**: Unifies diverse boolean tokens (`Yes`/`No`, `1`/`0`) into standard `"true"`/`"false"` strings.
*   **Numeric Cleaning**: Strips currency symbols (`$`, `€`, `£`, `¥`), percentage signs, and grouping commas from numeric strings.

### 3.6. Relationship Analysis (`relationship_detector.py`)

Post-profiling analysis to detect relationships between tables.

*   **Foreign Key Detection**: Compares column profiles across files using four additive signals:
    *   **Name match** (max 0.50): `name:direct_prefix`, `name:singular_prefix`, `name:exact`, `name:embedded`
    *   **Type compatibility** (max 0.20): `type:exact`, `type:numeric_compat`, `type:string_compat`
    *   **Cardinality** (max 0.25): `pk:key_candidate`, `pk:high_unique`, `pk:soft_id`, `cardinality:fk_subset`
    *   **Value overlap** (max 0.15): `overlap:high` (>=80%), `overlap:medium` (50-80%)
*   **Confidence Scoring**: Sum of matched signals, capped at 1.0. Minimum threshold: 0.50 (configurable).

### 3.7. Multi-Source Connector Framework (`connectors/`)

URI-based routing to remote data sources with DuckDB as the universal connectivity layer.

*   **URI Parser** (`uri_parser.py`): Parses `s3://`, `abfss://`, `gs://`, `snowflake://`, `postgresql://` URIs into `SourceDescriptor` objects.
*   **Connector Registry** (`registry.py`): Lazy-loaded connector map. Avoids importing heavy SDKs (boto3, azure, snowflake) until first use.
*   **BaseConnector ABC** (`base.py`): Defines `test_connection()`, `configure_duckdb()`, `list_objects()`, `duckdb_scan_expression()` interface.
*   **CloudStorageConnector** (`cloud_storage.py`): Handles S3, ADLS Gen2, GCS. Uses DuckDB httpfs/azure extensions for data reading, native SDKs for object listing.
*   **DatabaseConnector** (`database.py`): Handles PostgreSQL (via DuckDB postgres_scanner) and Snowflake (native SDK only).
*   **DuckDB Remote Layer** (`duckdb_remote.py`): Creates in-memory DuckDB connections with auto-loaded extensions, provides `remote_count()`, `remote_sample()`, `remote_schema()` helpers.

### 3.8. Secure Credential Management

Credentials flow directly from the UI to REST endpoints — they **never** pass through the LLM, chat history, or LangGraph checkpoints.

*   **ConnectionManager** (`connection_manager.py`): Credential store with `register()`, `get()`, `remove()`, `test()`, `resolve_credentials()`. Resolution priority: stored connection → env vars → SDK defaults.
*   **CredentialStore** (`credential_store.py`): Fernet symmetric encryption using SHA-256 of `PROFILER_SECRET_KEY`. Double encryption: individual credentials encrypted, then entire file encrypted. Falls back to in-memory-only when no secret key configured.
*   **REST API**: `/api/connections` endpoints on the web server handle CRUD + test. No credential data in list responses.

---

## 4. Data Models

### 4.1. FileProfile (Output JSON)

The unified output object containing all metadata for a single source.

```json
{
  "source_type": "file",
  "file_format": "CSV",
  "file_path": "/data/customers.csv",
  "table_name": "customers",
  "row_count": 10500,
  "is_row_count_exact": true,
  "size_strategy": "MEMORY_SAFE",
  "standardization_applied": true,
  "source_uri": null,
  "connection_id": null,
  "columns": [
    {
      "name": "customer_id",
      "original_name": "Customer ID",
      "inferred_type": "INTEGER",
      "confidence_score": 1.0,
      "null_count": 0,
      "distinct_count": 10500,
      "quality_flags": []
    }
  ],
  "structural_issues": []
}
```

For remote sources, `source_uri` and `connection_id` are populated:
```json
{
  "source_type": "remote_storage",
  "source_uri": "s3://my-bucket/data/customers.parquet",
  "connection_id": "prod-s3",
  ...
}
```

### 4.2. SourceDescriptor (Remote URI)

```
SourceDescriptor
├── scheme: str             # "s3", "abfss", "gs", "snowflake", "postgresql"
├── bucket_or_host: str     # "my-bucket", "db.example.com"
├── path: str               # "/data/file.csv", "/dbname"
├── raw_uri: str            # Original URI string
├── connection_id: str      # Optional — links to stored credentials
├── is_remote: bool         # Always True
├── is_object_storage: bool # True for s3/abfss/gs
├── is_database: bool       # True for snowflake/postgresql
└── is_directory_like: bool # True when path ends with /
```

### 4.3. RelationshipReport

```json
{
  "tables_analyzed": 5,
  "candidates": [
    {
      "fk": {"table": "orders", "column": "cust_id"},
      "pk": {"table": "customers", "column": "id"},
      "confidence": 0.95,
      "evidence": ["name:direct_prefix", "type:exact", "pk:key_candidate", "overlap:high"],
      "signal_source": "deterministic"
    }
  ]
}
```

---
