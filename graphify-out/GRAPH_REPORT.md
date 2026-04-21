# Graph Report - .  (2026-04-08)

## Corpus Check
- 110 files · ~114,778 words
- Verdict: corpus is large enough that graph structure adds value.

## Summary
- 2059 nodes · 5250 edges · 60 communities detected
- Extraction: 38% EXTRACTED · 62% INFERRED · 0% AMBIGUOUS · INFERRED: 3259 edges (avg confidence: 0.5)
- Token cost: 0 input · 0 output

## God Nodes (most connected - your core abstractions)
1. `SizeStrategy` - 215 edges
2. `FileProfile` - 207 edges
3. `RelationshipReport` - 182 edges
4. `RawColumnData` - 161 edges
5. `InferredType` - 154 edges
6. `QualityFlag` - 147 edges
7. `CorruptFileError` - 135 edges
8. `IntakeResult` - 133 edges
9. `FileFormat` - 113 edges
10. `ColumnProfile` - 90 edges

## Surprising Connections (you probably didn't know these)
- `Standardization Layer — Column Name & Value Normalization  Entry point:  standar` --uses--> `RawColumnData`  [INFERRED]
  file_profiler\standardization\normalizer.py → file_profiler\models\file_profile.py
- `Per-column record of what the standardizer changed.` --uses--> `RawColumnData`  [INFERRED]
  file_profiler\standardization\normalizer.py → file_profiler\models\file_profile.py
- `Summary of all standardization actions for quality reporting.` --uses--> `RawColumnData`  [INFERRED]
  file_profiler\standardization\normalizer.py → file_profiler\models\file_profile.py
- `Standardize column names and values across all columns.      Returns a NEW list` --uses--> `RawColumnData`  [INFERRED]
  file_profiler\standardization\normalizer.py → file_profiler\models\file_profile.py
- `Convert a column name to lowercase snake_case.      Rules:       1. Strip leadin` --uses--> `RawColumnData`  [INFERRED]
  file_profiler\standardization\normalizer.py → file_profiler\models\file_profile.py

## Hyperedges (group relationships)
- **LangGraph Agent bridges File Profiler + Data Connector MCP Servers** — mcparch_langgraph_agent, mcparch_file_profiler_server, mcparch_data_connector_server [EXTRACTED 1.00]
- **MAP-APPLY-EMBED-CLUSTER-REDUCE Enrichment** — info_enrichment_mapreduce, readme_chromadb, currentsystemdesign_relationship_detector [EXTRACTED 0.90]
- **Secure Remote Profiling Stack** — currentsystemdesign_connector_framework, currentsystemdesign_credential_mgmt, mcparch_data_connector_server [EXTRACTED 1.00]

## Communities

### Community 0 - "Column Profiling Engine"
Cohesion: 0.02
Nodes (302): Layer 7 — Column Profiling Engine  Entry point:  profile(raw: RawColumnData) ->, Compute min, max, and skewness., Count distinct non-null values from the sample.      Returns (distinct_count, is, Compute string length distribution., A column is a key candidate if it has near-perfect uniqueness, no nulls,     and, Min and max stored as strings (format-agnostic per architecture spec).     - Num, Pearson's moment coefficient of skewness — numeric columns only.     Returns Non, Compute comprehensive descriptive statistics for numeric columns.      Returns a (+294 more)

### Community 1 - "CSV Profiling Engine"
Cohesion: 0.03
Nodes (209): _CsvStructure, Layer 4 — CSV Profiling Engine  Steps (all gated by SizeStrategy):   A — Structu, Return a text-mode file handle, transparently decompressing gz/zip.     For zip,, Context manager: opens the first CSV-like entry of a zip archive as text.      U, Profile a CSV via DuckDB (plain or gzip-compressed).      DuckDB handles structu, Detected structural properties of a CSV file., Read the probe lines then delegate to the pure-logic helper., Detect structure from already-read probe lines.      Extracted as a pure functio (+201 more)

### Community 2 - "Connector Framework"
Cohesion: 0.02
Nodes (128): ABC, BaseConnector, ConnectorError, Core abstractions for remote data source connectors.  SourceDescriptor  — parsed, Interface that all source connectors implement.      Each connector knows how to, List available schemas in a database.          Only applicable to database conne, Whether DuckDB can handle this source directly.          Override to return Fals, Raised when a connector operation fails (auth, network, config). (+120 more)

### Community 3 - "Chatbot CLI"
Cohesion: 0.02
Nodes (99): _load_dotenv(), main(), _print_banner(), _print_help(), Interactive chatbot for the data profiling agent.  Multi-turn conversational int, Run the interactive chatbot loop., Execute one conversational turn with progress tracking., Truncate oversized ToolMessage content to avoid context overflow. (+91 more)

### Community 4 - "Standardization Tests"
Cohesion: 0.03
Nodes (12): Tests for the standardization layer (file_profiler/standardization/normalizer.py, _raw(), TestBooleanNormalization, TestBooleanPreScanGuard, TestEdgeCases, TestIdempotency, TestMainIntegration, TestNameDeduplication (+4 more)

### Community 5 - "Frontend App"
Cohesion: 0.06
Nodes (67): addAssistantMessage(), addChartPreviewCard(), addDirectoryPreviewCard(), addERDiagramMessage(), addErrorMessage(), addHistoryToolMessage(), addPreviewCard(), addRelationshipPreviewCard() (+59 more)

### Community 6 - "JSON Engine Tests"
Cohesion: 0.06
Nodes (14): _make_intake(), TestEdgeCases, TestFlattenRecord, TestHasDeepNesting, TestProfileArrayOfObjects, TestProfileNDJSON, TestProfileSingleObject, TestSamplingStrategies (+6 more)

### Community 7 - "Relationship Detector Tests"
Cohesion: 0.07
Nodes (9): _col(), _fp(), TestCardinalityScoring, TestFkEligibility, TestPkEligibility, TestRelationshipWriter, TestReport, TestValueOverlap (+1 more)

### Community 8 - "Vector Store"
Cohesion: 0.05
Nodes (51): batch_upsert_column_descriptions(), batch_upsert_table_summaries(), _batched_add_documents(), build_table_affinity_matrix(), clear_store(), cluster_by_column_affinity(), cluster_columns_dbscan(), derive_relationships_from_clusters() (+43 more)

### Community 9 - "Chart Generator"
Cohesion: 0.08
Nodes (47): _apply_theme(), chart_cardinality(), chart_column_detail(), chart_completeness(), chart_correlation_matrix(), chart_data_quality_scorecard(), _chart_dir(), chart_distribution() (+39 more)

### Community 10 - "Parquet Engine Tests"
Cohesion: 0.11
Nodes (13): _col(), _large_table(), Tests for file_profiler/engines/parquet_engine.py, _simple_table(), _struct_table(), TestDeclaredTypes, TestMainIntegration, TestNestedFields (+5 more)

### Community 11 - "System Architecture Concepts"
Cohesion: 0.05
Nodes (45): Classification Layer (Content Sniffing), Multi-Source Connector Framework, Secure Credential Management, Data Profiler Engine, FileProfile Data Model, Intake Layer, Layered Pipeline Architecture, Standardization/Normalizer (+37 more)

### Community 12 - "Excel Engine Tests"
Cohesion: 0.06
Nodes (7): _create_xlsx(), TestCellToStr, TestHeaderDetection, TestLooksNumeric, TestMultiSheet, TestProfileXLSX, TestSamplingStrategies

### Community 13 - "CSV Engine Internals"
Cohesion: 0.13
Nodes (37): _add_to_sample(), _build_raw_columns(), _deduplicate_headers(), _detect_headers(), _detect_headers_from_rows(), _detect_structure(), _detect_structure_from_lines(), _determine_delimiter() (+29 more)

### Community 14 - "CSV Engine Tests"
Cohesion: 0.09
Nodes (12): _intake(), _make_zip_partition(), TestCompression, TestHeaderDetection, TestIntegration, TestMultiFileZip, TestRawColumnData, TestRowCount (+4 more)

### Community 15 - "Directory Profiling Tests"
Cohesion: 0.07
Nodes (9): _orders_csv(), TestProfileDirectory, TestProfileFileCsv, TestProfileFileGzip, TestProfileFileOutput, TestProfileFileZipPartition, TestProfileFileZipSingle, TestRun (+1 more)

### Community 16 - "Enrichment MapReduce"
Cohesion: 0.11
Nodes (30): _apply_descriptions_to_profiles(), batch_enrich(), _build_cluster_context(), _build_cluster_derived_relationships_context(), _build_column_descriptions_context(), _build_discovered_relationships_context(), _build_relationships_context(), _build_table_context() (+22 more)

### Community 17 - "Quality Checker Tests"
Cohesion: 0.1
Nodes (10): _col(), Tests for file_profiler/quality/structural_checker.py, TestColumnShiftErrors, TestConstantColumn, TestDuplicateColumnNames, TestEncodingInconsistency, TestFullyNull, TestHighNullRatio (+2 more)

### Community 18 - "File Resolver Security"
Cohesion: 0.1
Nodes (20): Exception, cleanup_expired_uploads(), _is_subpath(), PathSecurityError, File resolver — validates and resolves paths for MCP tool handlers.  All tool ha, Resolve a user input to either a local Path or a SourceDescriptor.      If the i, Check if child is equal to or a subpath of parent., Raised when a resolved path falls outside allowed directories. (+12 more)

### Community 19 - "DB Engine"
Cohesion: 0.12
Nodes (25): _duckdb_connect(), _duckdb_list_tables(), _duckdb_profile_one(), _duckdb_profile_tables(), list_tables(), profile(), Database engine — profiles tables inside DuckDB (.duckdb) and SQLite (.db) files, Profile each table in a DuckDB file. (+17 more)

### Community 20 - "Output Writer Tests"
Cohesion: 0.14
Nodes (5): _minimal_profile(), Tests for file_profiler/output/profile_writer.py, TestQualitySummary, TestSchema, TestWrite

### Community 21 - "DuckDB Sampler"
Cohesion: 0.12
Nodes (23): _cleanup_connections(), _connect(), duckdb_connection(), duckdb_count(), duckdb_count_json(), duckdb_count_parquet(), duckdb_sample(), duckdb_sample_json() (+15 more)

### Community 22 - "File Classifier"
Cohesion: 0.13
Nodes (21): classify(), _decode_sniff(), _is_csv(), _is_duckdb(), _is_excel(), _is_json(), _is_parquet(), _is_sqlite() (+13 more)

### Community 23 - "Null Sentinel Tests"
Cohesion: 0.19
Nodes (1): TestNullSentinels

### Community 24 - "Enrichment Progress IPC"
Cohesion: 0.15
Nodes (17): check_enrichment_complete(), clear_progress(), manifest_path(), progress_file_path(), Enrichment progress file IPC.  The MCP server writes a progress JSON file at eac, Write a completion manifest after a successful enrichment run.      Args:, Read the enrichment completion manifest.      Returns None if the manifest doesn, Check if a previous enrichment run is still valid.      Compares the current tab (+9 more)

### Community 25 - "Community 25"
Cohesion: 0.2
Nodes (17): File encoding cannot be determined and the UTF-8 fallback also failed.     Raise, UnsupportedEncodingError, _check_exists(), _check_size(), _detect_bom(), _detect_compression(), _detect_encoding(), _get_sniff_bytes() (+9 more)

### Community 26 - "Community 26"
Cohesion: 0.16
Nodes (17): _build_name_map(), _clean_numeric(), ColumnStandardizationDetail, _is_boolean_column(), _normalize_name(), Standardization Layer — Column Name & Value Normalization  Entry point:  standar, Standardize column names and values across all columns.      Returns a NEW list, Convert a column name to lowercase snake_case.      Rules:       1. Strip leadin (+9 more)

### Community 27 - "Community 27"
Cohesion: 0.24
Nodes (17): _make_ctx(), _patch_dirs(), Tests for the MCP server tool handlers.  Tests call the tool functions directly, Point DATA_DIR, UPLOAD_DIR, OUTPUT_DIR at tmp_path subdirectories., Create a mock MCP Context with report_progress., test_caches_profile(), test_lists_csv_files(), test_reports_progress() (+9 more)

### Community 28 - "Community 28"
Cohesion: 0.23
Nodes (15): _api_key_env(), get_llm(), get_llm_with_fallback(), get_reduce_llm(), _get_timeout(), _make_anthropic(), _make_google(), _make_groq() (+7 more)

### Community 29 - "Community 29"
Cohesion: 0.24
Nodes (15): _bucket_cardinality(), _check_key_candidate(), _compute_avg_length(), _compute_distinct(), _compute_length_distribution(), _compute_length_stats(), _compute_min_max(), _compute_numeric_stats() (+7 more)

### Community 30 - "Community 30"
Cohesion: 0.19
Nodes (14): delete_session(), list_sessions(), _memory_list(), _memory_touch(), _memory_update(), Session persistence for the Data Profiler chat UI.  CRUD operations on the ``ses, Return the most recent sessions, newest first., Delete a session. Returns True if it existed. (+6 more)

### Community 31 - "Community 31"
Cohesion: 0.21
Nodes (13): _add_flag(), check(), _flag_column_nullness(), _flag_duplicate_names(), Layer 8 — Structural Quality Checker  Runs after column profiling. Examines the, Report column shift errors at the file level.      A shift error means a row had, Report a suspected encoding inconsistency.      In this system latin-1 is the fi, Append flag only if not already present (idempotent). (+5 more)

### Community 32 - "Community 32"
Cohesion: 0.31
Nodes (12): _cardinality_score(), detect(), _has_disqualifying_flag(), _is_fk_eligible(), _is_pk_eligible(), _looks_like_id_column(), _name_score(), _overlap_score_from_pct() (+4 more)

### Community 33 - "Community 33"
Cohesion: 0.17
Nodes (11): Test the progress tracking module — smart summaries and rendering.  Usage:, Verify all tools have weights defined., Test progress bar rendering at various percentages., Test time formatting., Test stage hints for each tool., Test smart result summaries for each tool type., test_extract_summary(), test_fmt_time() (+3 more)

### Community 34 - "Community 34"
Cohesion: 0.2
Nodes (5): large_csv(), Smoke tests for the DuckDB sampler used by STREAM_ONLY CSV profiling., Generate a 20k-row CSV to test DuckDB sampling., Verify DuckDB output feeds cleanly into _build_raw_columns., test_duckdb_integration_with_build_raw_columns()

### Community 35 - "Community 35"
Cohesion: 0.31
Nodes (8): main(), CLI entry point for the LangGraph profiling agent.  Usage:   # Start MCP server, Run the profiling agent and return the final report.      Args:         data_pat, Run the agent without interruptions., Run the agent with human-in-the-loop approval for tool calls., run_agent(), _run_autonomous(), _run_interactive()

### Community 36 - "Community 36"
Cohesion: 0.25
Nodes (7): _auto_duckdb_memory(), get_postgres_dsn(), Environment-based configuration for the MCP server layer.  Pipeline-internal set, Build a PostgreSQL connection string from env vars. Empty if unconfigured., Validate configuration values at import time.      Raises ValueError with a clea, Auto-scale DuckDB memory to min(system_ram / 4, 4GB), floor 512MB., _validate_config()

### Community 37 - "Community 37"
Cohesion: 0.43
Nodes (6): build_documents(), create_vector_store(), enrich(), extract_sample_rows(), _read_csv_rows(), _read_parquet_rows()

### Community 38 - "Community 38"
Cohesion: 0.5
Nodes (3): Relationship Report Writer  Serialises a RelationshipReport to JSON. Reuses seri, Serialise a RelationshipReport to JSON and write atomically to output_path., write()

### Community 39 - "Community 39"
Cohesion: 0.5
Nodes (3): configure_logging(), One-call logging configuration for the MCP server process., Configure root logger.  Call once at process startup.      Logs go to stderr — s

### Community 40 - "Community 40"
Cohesion: 0.67
Nodes (1): Load all WWI CSV files into PostgreSQL under the 'wwi' schema. Each CSV becomes

### Community 41 - "Community 41"
Cohesion: 0.67
Nodes (1): Simulate what the web server does: init checkpointer, build graph, send a messag

### Community 42 - "Community 42"
Cohesion: 0.67
Nodes (1): E2E test for the enrichment pipeline — standalone (no MCP server needed).  Run

### Community 43 - "Community 43"
Cohesion: 0.67
Nodes (1): Test MCP connection directly.

### Community 44 - "Community 44"
Cohesion: 0.67
Nodes (1): Quick WebSocket test: profile WWI CSVs via the web UI.

### Community 45 - "Community 45"
Cohesion: 1.0
Nodes (1): Test ER diagram improvements: audit column separation + deduplication.

### Community 46 - "Community 46"
Cohesion: 1.0
Nodes (1): Test LLM factory with Groq provider and fallback logic.

### Community 47 - "Community 47"
Cohesion: 1.0
Nodes (0): 

### Community 48 - "Community 48"
Cohesion: 1.0
Nodes (1): Total elapsed time for the current turn.

### Community 49 - "Community 49"
Cohesion: 1.0
Nodes (0): 

### Community 50 - "Community 50"
Cohesion: 1.0
Nodes (1): True if the path is a prefix / directory (not a single file).

### Community 51 - "Community 51"
Cohesion: 1.0
Nodes (1): Short human-readable label for UI display.

### Community 52 - "Community 52"
Cohesion: 1.0
Nodes (1): Validate credentials and reachability.          Returns True on success, raises

### Community 53 - "Community 53"
Cohesion: 1.0
Nodes (1): Install/load DuckDB extensions and SET credential parameters.          Called on

### Community 54 - "Community 54"
Cohesion: 1.0
Nodes (1): List files (object storage) or tables (database) at the path.          For objec

### Community 55 - "Community 55"
Cohesion: 1.0
Nodes (1): Return the DuckDB SQL expression to read from this source.          Examples:

### Community 56 - "Community 56"
Cohesion: 1.0
Nodes (0): 

### Community 57 - "Community 57"
Cohesion: 1.0
Nodes (0): 

### Community 58 - "Community 58"
Cohesion: 1.0
Nodes (0): 

### Community 59 - "Community 59"
Cohesion: 1.0
Nodes (0): 

## Knowledge Gaps
- **197 isolated node(s):** `CLI entry point for the LangGraph profiling agent.  Usage:   # Start MCP server`, `Run the profiling agent and return the final report.      Args:         data_pat`, `Run the agent without interruptions.`, `Run the agent with human-in-the-loop approval for tool calls.`, `Enrichment progress file IPC.  The MCP server writes a progress JSON file at eac` (+192 more)
  These have ≤1 connection - possible missing edges or undocumented components.
- **Thin community `Community 45`** (2 nodes): `test_er_improvements.py`, `Test ER diagram improvements: audit column separation + deduplication.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 46`** (2 nodes): `test_llm_factory.py`, `Test LLM factory with Groq provider and fallback logic.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 47`** (2 nodes): `test_pg.py`, `test()`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 48`** (1 nodes): `Total elapsed time for the current turn.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 49`** (1 nodes): `settings.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 50`** (1 nodes): `True if the path is a prefix / directory (not a single file).`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 51`** (1 nodes): `Short human-readable label for UI display.`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 52`** (1 nodes): `Validate credentials and reachability.          Returns True on success, raises`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 53`** (1 nodes): `Install/load DuckDB extensions and SET credential parameters.          Called on`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 54`** (1 nodes): `List files (object storage) or tables (database) at the path.          For objec`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 55`** (1 nodes): `Return the DuckDB SQL expression to read from this source.          Examples:`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 56`** (1 nodes): `legacy_engine.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 57`** (1 nodes): `test_classification.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 58`** (1 nodes): `test_column_profiler.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.
- **Thin community `Community 59`** (1 nodes): `test_intake.py`
  Too small to be a meaningful cluster - may be noise or needs more connections extracted.

## Suggested Questions
_Questions this graph is uniquely positioned to answer:_

- **Why does `SizeStrategy` connect `CSV Profiling Engine` to `Column Profiling Engine`, `JSON Engine Tests`, `Relationship Detector Tests`, `Parquet Engine Tests`, `Excel Engine Tests`, `CSV Engine Internals`, `CSV Engine Tests`, `Directory Profiling Tests`, `Output Writer Tests`?**
  _High betweenness centrality (0.144) - this node is a cross-community bridge._
- **Why does `FileProfile` connect `Column Profiling Engine` to `CSV Profiling Engine`, `Connector Framework`, `Relationship Detector Tests`, `Parquet Engine Tests`, `Directory Profiling Tests`, `Enrichment MapReduce`, `Output Writer Tests`?**
  _High betweenness centrality (0.140) - this node is a cross-community bridge._
- **Why does `RemoteObject` connect `Connector Framework` to `CSV Profiling Engine`?**
  _High betweenness centrality (0.068) - this node is a cross-community bridge._
- **Are the 211 inferred relationships involving `SizeStrategy` (e.g. with `Allow ``python -m file_profiler.connectors`` to start the Connector MCP server.` and `Profile columns using a thread pool when the column count is large enough.`) actually correct?**
  _`SizeStrategy` has 211 INFERRED edges - model-reasoned connections that need verification._
- **Are the 205 inferred relationships involving `FileProfile` (e.g. with `_LRUCache` and `MCP Server for Remote Data Connectors.  Exposes the full profiling pipeline for`) actually correct?**
  _`FileProfile` has 205 INFERRED edges - model-reasoned connections that need verification._
- **Are the 180 inferred relationships involving `RelationshipReport` (e.g. with `_LRUCache` and `MCP Server for Remote Data Connectors.  Exposes the full profiling pipeline for`) actually correct?**
  _`RelationshipReport` has 180 INFERRED edges - model-reasoned connections that need verification._
- **Are the 159 inferred relationships involving `RawColumnData` (e.g. with `Allow ``python -m file_profiler.connectors`` to start the Connector MCP server.` and `Profile columns using a thread pool when the column count is large enough.`) actually correct?**
  _`RawColumnData` has 159 INFERRED edges - model-reasoned connections that need verification._