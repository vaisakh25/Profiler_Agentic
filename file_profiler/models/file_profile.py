from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

from file_profiler.models.enums import (
    Cardinality,
    FileFormat,
    InferredType,
    QualityFlag,
    SizeStrategy,
)


# ---------------------------------------------------------------------------
# Small value types
# ---------------------------------------------------------------------------

@dataclass
class TopValue:
    """A single entry in the top-N most frequent values list."""
    value: str
    count: int


@dataclass
class TypeInferenceResult:
    """
    Output of the type inference engine for a single column.
    Produced by profiling/type_inference.py; consumed by the column profiler.
    """
    inferred_type:    InferredType
    confidence_score: float               # 0.0–1.0
    format_variants:  list[str] = field(default_factory=list)
    # ^ populated for MIXED_DATE: stores each distinct date format pattern observed
    quality_flags:    list[QualityFlag] = field(default_factory=list)
    # ^ MIXED_DATE_FORMATS, MIXED_TIMEZONES — raised during inference, not structural checks


# ---------------------------------------------------------------------------
# Intermediate type — passed from a format engine to the column profiler
# ---------------------------------------------------------------------------

@dataclass
class RawColumnData:
    """
    Intermediate payload produced by a format engine (csv/parquet/json/excel/legacy)
    and consumed by the ColumnProfiler.

    Raw values are NEVER transformed — type inference works on the original strings.
    """
    name:          str
    declared_type: Optional[str]   # None for CSV/JSON; populated for Parquet/Excel
    values:        list[str]       # sampled raw string values
    total_count:   int             # total rows in the file (used for ratio calculations)
    null_count:    int             # null count from the raw scan

    # Pre-computed by engines that already have schema info (e.g. Parquet metadata read)
    type_inference: Optional[TypeInferenceResult] = None


# ---------------------------------------------------------------------------
# Output types — the unified profile schema
# ---------------------------------------------------------------------------

@dataclass
class QualitySummary:
    """Aggregate quality metrics attached to the top-level FileProfile."""
    columns_profiled:      int = 0
    columns_with_issues:   int = 0
    null_heavy_columns:    int = 0
    type_conflict_columns: int = 0
    corrupt_rows_detected: int = 0


@dataclass
class ColumnProfile:
    """
    Per-column profile.

    Structure is identical regardless of source file format.
    Fields that cannot be computed for a column type are set to None, never omitted.
    The semantic_type field is populated later by the Column Intelligence Layer.
    """
    # Identity
    name:          str
    declared_type: Optional[str]   # None for CSV/JSON; e.g. "INT64" for Parquet

    # Type inference
    inferred_type:    InferredType
    confidence_score: float        # 0.0–1.0

    # Counts
    null_count:             int
    distinct_count:         int
    is_distinct_count_exact: bool = True   # False when approximated via hash sampling

    # Ratios and cardinality
    unique_ratio:       float       = 0.0
    cardinality:        Cardinality = Cardinality.MEDIUM
    is_nullable:        bool        = False
    is_constant:        bool        = False
    is_sparse:          bool        = False   # True when null_count/total > NULL_HEAVY_THRESHOLD
    is_key_candidate:   bool        = False
    is_low_cardinality: bool        = False

    # Range stats — None for free-text and purely categorical columns
    min:      Optional[str]   = None   # stored as string to be format-agnostic
    max:      Optional[str]   = None
    skewness: Optional[float] = None   # numeric columns only

    # String length distribution — None for numeric/date/boolean columns
    avg_length:  Optional[float] = None
    length_p10:  Optional[float] = None
    length_p50:  Optional[float] = None
    length_p90:  Optional[float] = None
    length_max:  Optional[int]   = None

    # Intelligence layer — populated downstream, not by this branch
    semantic_type: Optional[str] = None

    # Standardization — pre-normalization column name; None if unchanged
    original_name: Optional[str] = None

    # Top-N most frequent values
    top_values: list[TopValue] = field(default_factory=list)

    # Sample of raw (untransformed) values for inspection
    sample_values: list[str] = field(default_factory=list)

    # Quality flags raised by engines or StructuralChecker
    quality_flags: list[QualityFlag] = field(default_factory=list)


@dataclass
class FileProfile:
    """
    Top-level profile for a single file.

    Mirrors the database branch output schema exactly so that downstream
    Silver/Gold layer logic is completely source-agnostic.
    """
    # Source identity
    source_type: str        = "file"             # always "file" for this branch
    file_format: FileFormat = FileFormat.UNKNOWN
    file_path:   str        = ""
    table_name:  str        = ""                 # derived from filename stem

    # Row count
    row_count:          int  = 0
    is_row_count_exact: bool = True

    # File-level metadata
    encoding:          str          = "utf-8"
    size_bytes:        int          = 0
    size_strategy:     SizeStrategy = SizeStrategy.MEMORY_SAFE
    corrupt_row_count: int          = 0          # rows skipped due to parse errors

    # Columns
    columns: list[ColumnProfile] = field(default_factory=list)

    # File-level structural issues (not tied to a specific column)
    structural_issues: list[str] = field(default_factory=list)

    # Standardization
    standardization_applied: bool = False

    # Aggregate quality summary
    quality_summary: QualitySummary = field(default_factory=QualitySummary)
