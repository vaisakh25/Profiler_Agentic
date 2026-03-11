from enum import Enum


class FileFormat(str, Enum):
    """Detected file format — determined by content sniffing, not extension."""
    CSV     = "csv"
    PARQUET = "parquet"
    JSON    = "json"
    EXCEL   = "excel"
    LEGACY  = "legacy"   # fixed-width / positional flat files
    UNKNOWN = "unknown"  # no format matched; file is skipped


class SizeStrategy(str, Enum):
    """
    Read strategy selected before any data is touched.

    MEMORY_SAFE  — < 100 MB   : full load into memory
    LAZY_SCAN    — 100 MB–2 GB: chunked reads, Polars lazy, DuckDB scan
    STREAM_ONLY  — > 2 GB     : line-by-line stream, never materialize full set
    """
    MEMORY_SAFE = "MEMORY_SAFE"
    LAZY_SCAN   = "LAZY_SCAN"
    STREAM_ONLY = "STREAM_ONLY"


class InferredType(str, Enum):
    """
    Type inference result for a column.
    Detection order (most specific → least): see profiling/type_inference.py.
    """
    NULL_ONLY   = "NULL_ONLY"    # every sampled value is null
    INTEGER     = "INTEGER"
    FLOAT       = "FLOAT"
    BOOLEAN     = "BOOLEAN"
    DATE        = "DATE"
    TIMESTAMP   = "TIMESTAMP"
    UUID        = "UUID"
    CATEGORICAL = "CATEGORICAL"  # low cardinality string
    FREE_TEXT   = "FREE_TEXT"    # long average length
    STRING      = "STRING"       # default fallback
    MIXED_DATE  = "MIXED_DATE"   # multiple date formats detected in the same column


class Cardinality(str, Enum):
    """
    Coarse cardinality bucket derived from unique_ratio.
    Thresholds are defined in config/settings.py.
    """
    HIGH   = "HIGH"    # unique_ratio > 0.9
    MEDIUM = "MEDIUM"  # 0.1 < unique_ratio <= 0.9
    LOW    = "LOW"     # unique_ratio <= 0.1


class QualityFlag(str, Enum):
    """
    Structural and type-level issue codes.
    Emitted by StructuralChecker (Layer 8) and format engines (Layers 4–6).
    """
    # Structural issues (Layer 8)
    DUPLICATE_COLUMN_NAME  = "DUPLICATE_COLUMN_NAME"   # two columns share the same header
    FULLY_NULL             = "FULLY_NULL"               # every value in the column is null
    CONSTANT_COLUMN        = "CONSTANT_COLUMN"          # only one distinct non-null value
    HIGH_NULL_RATIO        = "HIGH_NULL_RATIO"          # null_count / total > NULL_HEAVY_THRESHOLD
    COLUMN_SHIFT_ERROR     = "COLUMN_SHIFT_ERROR"       # row field count != header field count
    ENCODING_INCONSISTENCY = "ENCODING_INCONSISTENCY"   # mixed UTF-8 and Latin-1 in same file

    # Type-level issues (Layers 4 & 6)
    TYPE_CONFLICT       = "TYPE_CONFLICT"       # same key holds different types across records (JSON)
    MIXED_DATE_FORMATS  = "MIXED_DATE_FORMATS"  # multiple date format patterns in one column
    MIXED_TIMEZONES     = "MIXED_TIMEZONES"     # inconsistent timezone in timestamp column
    NULL_VARIANT_NORMALIZED = "NULL_VARIANT_NORMALIZED" # null sentinels (e.g. "NULL", "n/a") were converted to None

    # Row / file integrity
    STRUCTURAL_CORRUPTION = "STRUCTURAL_CORRUPTION"  # corrupt row ratio > STRUCTURAL_CORRUPTION_THRESHOLD
    TRUNCATED_VALUES      = "TRUNCATED_VALUES"       # max value length == declared field width (legacy)

    # Intake issues
    BINARY_MASQUERADE = "BINARY_MASQUERADE"  # binary content behind a text extension
    UNKNOWN_FORMAT    = "UNKNOWN_FORMAT"     # no format signature matched


class FlattenStrategy(str, Enum):
    """
    Strategy for handling nested structures in JSON files (Layer 6, Step C).

    EXPLODE   — arrays expand into multiple rows (use for small, uniform arrays)
    STRINGIFY — nested objects/arrays kept as a JSON string column
    HYBRID    — flatten known shallow fields, stringify deep arrays (recommended default)
    """
    EXPLODE   = "EXPLODE"
    STRINGIFY = "STRINGIFY"
    HYBRID    = "HYBRID"


class JSONShape(str, Enum):
    """Detected top-level shape of a JSON file (Layer 6, Step A)."""
    SINGLE_OBJECT    = "SINGLE_OBJECT"     # file contains one top-level {}
    ARRAY_OF_OBJECTS = "ARRAY_OF_OBJECTS"  # file contains [{}, {}, ...]
    NDJSON           = "NDJSON"            # each line is a valid {} (newline-delimited)
    DEEP_NESTED      = "DEEP_NESTED"       # any of the above with objects nested inside
