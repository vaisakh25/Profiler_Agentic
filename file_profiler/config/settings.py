# All magic numbers live here.
# No threshold, limit, or tuning constant should be hardcoded inline anywhere else.


# ---------------------------------------------------------------------------
# Layer 3 — Size Strategy Thresholds
#
# Used by strategy/size_strategy.py to select the read strategy.
# Logic: size < MEMORY_SAFE_BYTES → MEMORY_SAFE
#        size < LAZY_SCAN_BYTES   → LAZY_SCAN
#        else                     → STREAM_ONLY
# ---------------------------------------------------------------------------

MEMORY_SAFE_MAX_BYTES: int = 100 * 1024 * 1024           # 100 MB
LAZY_SCAN_MAX_BYTES:   int = 2   * 1024 * 1024 * 1024    # 2 GB


# ---------------------------------------------------------------------------
# Layer 4 — CSV Engine
# ---------------------------------------------------------------------------

# Step A — Structure Detection
# Number of rows sampled to detect structural corruption (inconsistent field counts).
CSV_STRUCTURE_PROBE_ROWS: int = 100

# Candidate delimiters for frequency analysis.
CSV_CANDIDATE_DELIMITERS: list[str] = [",", "\t", "|", ";"]

# If the ratio of rows with inconsistent field counts exceeds this, flag the file
# as structurally corrupt before profiling continues.
STRUCTURAL_CORRUPTION_THRESHOLD: float = 0.05    # 5 %

# Step B — Header Detection
# Number of rows read to determine whether row 0 is a header.
HEADER_DETECTION_ROWS: int = 5

# Step C — Row Count Estimation
# Number of chunks sampled to extrapolate total row count for large files.
# (rows_per_byte_avg  ×  file_size_bytes → estimated total)
ROW_COUNT_ESTIMATION_CHUNKS: int = 5

# Step D — Sampling
# Rows per read chunk (chunked reads and reservoir sampling).
CHUNK_SIZE: int = 50_000

# Reservoir sample target — rows kept in memory for type inference.
SAMPLE_ROW_COUNT: int = 10_000

# For STREAM_ONLY: read every Kth row (skip-interval sampling).
STREAM_SKIP_INTERVAL: int = 100


# ---------------------------------------------------------------------------
# Layer 4 / 6 — Type Inference (CSV and JSON share this)
# ---------------------------------------------------------------------------

# A column is CATEGORICAL if its distinct value count is below this threshold.
CATEGORICAL_MAX_DISTINCT: int = 50

# A column is FREE_TEXT if its average string length exceeds this threshold.
FREE_TEXT_MIN_AVG_LENGTH: float = 100.0


# ---------------------------------------------------------------------------
# Layer 6 — JSON Engine
# ---------------------------------------------------------------------------

# Number of records streamed to build the union schema.
JSON_SCHEMA_DISCOVERY_SAMPLE: int = 1_000

# Maximum array length before the EXPLODE flatten strategy is refused.
# Arrays larger than this must use STRINGIFY or HYBRID to avoid row count explosion.
JSON_MAX_ARRAY_EXPLODE_SIZE: int = 100


# ---------------------------------------------------------------------------
# Layer 7 — Column Profiling Engine
# ---------------------------------------------------------------------------

# Number of top most-frequent values stored per column.
TOP_N_VALUES: int = 10

# Number of raw sample values stored per column (for inspection).
SAMPLE_VALUES_COUNT: int = 5

# Row count above which distinct_count switches from exact to approximate
# (hash-modulo sampling) to avoid materializing all unique values.
APPROX_DISTINCT_ROW_THRESHOLD: int = 1_000_000


# ---------------------------------------------------------------------------
# Layer 8 — Structural Quality Checks
# ---------------------------------------------------------------------------

# Columns where null_count / total_count exceeds this threshold are flagged
# HIGH_NULL_RATIO and marked is_sparse = True.
NULL_HEAVY_THRESHOLD: float = 0.70    # 70 %


# ---------------------------------------------------------------------------
# Cardinality bucketing
# Thresholds used when assigning Cardinality.HIGH / MEDIUM / LOW to a column.
# ---------------------------------------------------------------------------

# unique_ratio > HIGH  → Cardinality.HIGH
# unique_ratio > LOW   → Cardinality.MEDIUM
# unique_ratio <= LOW  → Cardinality.LOW
CARDINALITY_HIGH_THRESHOLD: float = 0.90
CARDINALITY_LOW_THRESHOLD:  float = 0.10


# ---------------------------------------------------------------------------
# Layer 6.5 — Standardization
# ---------------------------------------------------------------------------

# Master toggle: set to False to skip all standardization.
STANDARDIZATION_ENABLED: bool = True

# Null sentinel values.  Compared after stripping whitespace and lowercasing.
# Empty string "" is handled separately (after whitespace trim, empty → None).
NULL_SENTINEL_VALUES: list[str] = [
    "null", "none", "na", "n/a", "nan", "nil",
    "-", "--", ".", "missing", "undefined",
]


# ---------------------------------------------------------------------------
# Parallelism — profile_directory worker pool
# ---------------------------------------------------------------------------

# Max workers for parallel file profiling.
# Each worker may hold up to MEMORY_SAFE_MAX_BYTES in memory, so keep this
# conservative.  Set to 1 to disable parallelism.
# Reads from env var MAX_PARALLEL_WORKERS (set in docker-compose / .env).
from file_profiler.config.env import MAX_PARALLEL_WORKERS  # noqa: E402
