# Data Profiler — Current System Design

## 1. Executive Summary

The Data Profiler is a robust, Python-based engine designed to analyze raw file-based data sources (CSV, Parquet, JSON, Excel) and generate standardized, format-agnostic statistical profiles. Unlike traditional database profilers, this system emphasizes defensive coding, content-aware format detection, and memory-safe processing strategies to handle the chaotic nature of file exports (corruption, missing headers, mixed types).

The system outputs a unified JSON schema for every file, enabling downstream systems to consume metadata without needing to understand the underlying source format.

---

## 2. Architectural Patterns

The system follows a **Layered Pipeline Architecture**. Each file passes through a strict sequence of processing layers, where each layer is responsible for a specific transformation or decision.

### Pipeline Flow

1.  **Intake Layer**: Validates file existence and readability.
2.  **Classification Layer**: Determines the true file format (ignoring extensions).
3.  **Strategy Layer**: Selects the I/O strategy (Memory vs. Stream) based on size.
4.  **Engine Layer**: Format-specific parsing (CSV/Parquet) to extract raw columns.
5.  **Standardization Layer**: Normalizes column names, cleans nulls, and standardizes booleans.
6.  **Profiling Layer**: Computes statistics and infers data types.
7.  **Quality Layer**: Checks for structural corruption and integrity issues.
8.  **Output Layer**: Serializes the `FileProfile` object to JSON.

---

## 3. Component Design Details

### 3.1. Orchestration (`main.py`)

The orchestrator serves as the entry point (`run`, `profile_file`, `profile_directory`). It manages the lifecycle of a profiling job and handles high-level error boundaries.

*   **Directory Scanning**: Non-recursive scanning of supported extensions (`.csv`, `.parquet`, `.gz`, `.zip`).
*   **Fault Tolerance**: Individual file failures (corruption, empty files) are logged and skipped, preventing batch job failures.
*   **Pipeline Wiring**: Connects the output of the Intake layer to the input of the Classifier, and so on.

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

### 3.6. Relationship Analysis (`relationships.py`)

Post-profiling analysis to detect relationships between tables.

*   **Foreign Key Detection**: Compares column profiles across files.
*   **Confidence Scoring**: Calculates a score (0.0 - 1.0) based on:
    *   Column Name similarity.
    *   Data Type compatibility.
    *   Cardinality ratios (FK should have lower/equal cardinality to PK).
    *   Value Overlap (Intersection of top frequent values).

---

## 4. Data Models

### 4.1. FileProfile (Output JSON)

The unified output object containing all metadata for a single file.

```json
{
  "source_type": "file",
  "file_format": "CSV",
  "file_path": "/data/customers.csv",
  "row_count": 10500,
  "is_row_count_exact": true,
  "size_strategy": "MEMORY_SAFE",
  "standardization_applied": true,
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

### 4.2. RelationshipReport

The output of the cross-file analysis.

```json
{
  "tables_analyzed": 5,
  "candidates": [
    {
      "fk": {"table": "orders", "column": "cust_id"},
      "pk": {"table": "customers", "column": "id"},
      "confidence": 0.95,
      "evidence": ["name:partial_match", "type:exact", "overlap:high"]
    }
  ]
}
```

---
