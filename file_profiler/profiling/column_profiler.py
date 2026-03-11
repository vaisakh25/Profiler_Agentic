"""
Layer 7 — Column Profiling Engine

Entry point:  profile(raw: RawColumnData) -> ColumnProfile

Shared across all format engines. Receives a RawColumnData payload from any
engine (csv / parquet / json / excel / legacy) and computes the standard set
of column metrics that populate the unified output schema.

Metrics produced:
  - null_count / distinct_count (exact or approximate) / unique_ratio
  - cardinality bucket, boolean flags (nullable, constant, sparse, key candidate)
  - min / max  (numeric sort for INTEGER/FLOAT; lexicographic for all others)
  - skewness   (numeric columns only)
  - avg_length (all columns with non-null values)
  - length_p10 / p50 / p90 / max  (string-class columns only)
  - top_N_values (Counter-based, TOP_N_VALUES entries)
  - sample_values (first SAMPLE_VALUES_COUNT non-null values)
  - quality_flags inherited from TypeInferenceResult (MIXED_DATE_FORMATS,
    MIXED_TIMEZONES) plus any passed in via raw.type_inference
"""

from __future__ import annotations

import logging
from collections import Counter
from typing import Optional

from file_profiler.config import settings
from file_profiler.models.enums import Cardinality, InferredType, QualityFlag
from file_profiler.models.file_profile import (
    ColumnProfile,
    RawColumnData,
    TopValue,
    TypeInferenceResult,
)
from file_profiler.profiling.type_inference import infer

log = logging.getLogger(__name__)

# Types where numeric sort applies for min/max.
_NUMERIC_TYPES = frozenset({InferredType.INTEGER, InferredType.FLOAT})

# Types where min/max is meaningless — set to None.
_NO_RANGE_TYPES = frozenset({InferredType.NULL_ONLY, InferredType.BOOLEAN, InferredType.FREE_TEXT})

# Types where the full length distribution (P10/P50/P90/max) is computed.
# avg_length is computed for ALL types with non-null values.
_DISTRIBUTION_TYPES = frozenset({
    InferredType.STRING,
    InferredType.CATEGORICAL,
    InferredType.UUID,
    InferredType.FREE_TEXT,
    InferredType.MIXED_DATE,
})

# Types that disqualify a column from being a key candidate.
_NON_KEY_TYPES = frozenset({
    InferredType.FREE_TEXT,
    InferredType.BOOLEAN,
    InferredType.NULL_ONLY,
    InferredType.MIXED_DATE,
})


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def profile(raw: RawColumnData) -> ColumnProfile:
    """
    Compute a full ColumnProfile from a RawColumnData payload.

    Type inference is skipped if raw.type_inference is already populated
    (Parquet and Excel engines pre-compute this from declared schema).

    Args:
        raw: Intermediate payload from a format engine.

    Returns:
        ColumnProfile ready to be written into FileProfile.columns.
    """
    type_result: TypeInferenceResult = raw.type_inference or infer(raw.values)

    non_null = [
        v for v in raw.values
        if v is not None and str(v).strip() != ""
    ]

    # ── Counts ────────────────────────────────────────────────────────────
    null_count              = raw.null_count
    distinct_count, is_exact = _compute_distinct(non_null, raw.total_count)

    # ── Ratios and flags ──────────────────────────────────────────────────
    total                   = raw.total_count
    unique_ratio            = distinct_count / total if total > 0 else 0.0
    cardinality             = _bucket_cardinality(unique_ratio)
    is_nullable             = null_count > 0
    is_constant             = distinct_count == 1 and len(non_null) > 0
    is_sparse               = (null_count / total) > settings.NULL_HEAVY_THRESHOLD if total > 0 else False
    is_key_candidate        = _check_key_candidate(unique_ratio, null_count, type_result.inferred_type)
    is_low_cardinality      = cardinality == Cardinality.LOW

    # ── Range stats ───────────────────────────────────────────────────────
    min_val, max_val        = _compute_min_max(non_null, type_result.inferred_type)
    skewness                = _compute_skewness(non_null, type_result.inferred_type)

    # ── Length stats ──────────────────────────────────────────────────────
    avg_length              = _compute_avg_length(non_null)
    length_p10, length_p50, length_p90, length_max = _compute_length_distribution(
        non_null, type_result.inferred_type
    )

    # ── Frequency and samples ─────────────────────────────────────────────
    top_values              = _compute_top_values(non_null)
    sample_values           = non_null[:settings.SAMPLE_VALUES_COUNT]

    # ── Quality flags (inherit from type inference) ───────────────────────
    quality_flags           = list(type_result.quality_flags)

    return ColumnProfile(
        name=raw.name,
        declared_type=raw.declared_type,
        inferred_type=type_result.inferred_type,
        confidence_score=type_result.confidence_score,
        null_count=null_count,
        distinct_count=distinct_count,
        is_distinct_count_exact=is_exact,
        unique_ratio=round(unique_ratio, 4),
        cardinality=cardinality,
        is_nullable=is_nullable,
        is_constant=is_constant,
        is_sparse=is_sparse,
        is_key_candidate=is_key_candidate,
        is_low_cardinality=is_low_cardinality,
        min=min_val,
        max=max_val,
        skewness=skewness,
        avg_length=avg_length,
        length_p10=length_p10,
        length_p50=length_p50,
        length_p90=length_p90,
        length_max=length_max,
        top_values=top_values,
        sample_values=sample_values,
        quality_flags=quality_flags,
    )


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _compute_distinct(non_null: list[str], total: int) -> tuple[int, bool]:
    """
    Count distinct non-null values from the sample.

    Returns (distinct_count, is_exact).
    is_exact is False when total_count exceeds APPROX_DISTINCT_ROW_THRESHOLD,
    because the sample may not have seen all values in the full file.
    """
    distinct_count = len(set(non_null))
    is_exact       = total <= settings.APPROX_DISTINCT_ROW_THRESHOLD
    return distinct_count, is_exact


def _bucket_cardinality(unique_ratio: float) -> Cardinality:
    if unique_ratio > settings.CARDINALITY_HIGH_THRESHOLD:
        return Cardinality.HIGH
    if unique_ratio > settings.CARDINALITY_LOW_THRESHOLD:
        return Cardinality.MEDIUM
    return Cardinality.LOW


def _check_key_candidate(unique_ratio: float, null_count: int, inferred_type: InferredType) -> bool:
    """
    A column is a key candidate if it has near-perfect uniqueness, no nulls,
    and its type is not inherently non-unique (free text, booleans, etc.).
    """
    return (
        unique_ratio >= 0.99
        and null_count == 0
        and inferred_type not in _NON_KEY_TYPES
    )


def _compute_min_max(
    non_null: list[str], inferred_type: InferredType
) -> tuple[Optional[str], Optional[str]]:
    """
    Min and max stored as strings (format-agnostic per architecture spec).
    - Numeric columns: sorted as float after comma-stripping.
    - All others: lexicographic sort.
    - Types in _NO_RANGE_TYPES: returns (None, None).
    """
    if not non_null or inferred_type in _NO_RANGE_TYPES:
        return None, None

    if inferred_type in _NUMERIC_TYPES:
        try:
            nums = [float(v.replace(",", "")) for v in non_null]
            return str(min(nums)), str(max(nums))
        except (ValueError, AttributeError):
            pass   # fall through to lexicographic

    sorted_vals = sorted(non_null)
    return sorted_vals[0], sorted_vals[-1]


def _compute_skewness(non_null: list[str], inferred_type: InferredType) -> Optional[float]:
    """
    Pearson's moment coefficient of skewness — numeric columns only.
    Returns None for all other types or when the sample is too small.
    """
    if inferred_type not in _NUMERIC_TYPES or len(non_null) < 3:
        return None

    try:
        nums = [float(v.replace(",", "")) for v in non_null]
        n    = len(nums)
        mean = sum(nums) / n
        std  = (sum((x - mean) ** 2 for x in nums) / n) ** 0.5

        if std == 0.0:
            return 0.0

        skew = sum((x - mean) ** 3 for x in nums) / (n * std ** 3)
        return round(skew, 4)

    except (ValueError, ZeroDivisionError):
        return None


def _compute_avg_length(non_null: list[str]) -> Optional[float]:
    """Mean character length across all non-null values."""
    if not non_null:
        return None
    return round(sum(len(v) for v in non_null) / len(non_null), 2)


def _compute_length_distribution(
    non_null: list[str], inferred_type: InferredType
) -> tuple[Optional[float], Optional[float], Optional[float], Optional[int]]:
    """
    P10, P50, P90, and max character length.
    Only computed for string-class column types; returns (None, None, None, None) otherwise.
    """
    if inferred_type not in _DISTRIBUTION_TYPES or not non_null:
        return None, None, None, None

    lengths = sorted(len(v) for v in non_null)
    return (
        _percentile(lengths, 0.10),
        _percentile(lengths, 0.50),
        _percentile(lengths, 0.90),
        lengths[-1],
    )


def _percentile(sorted_values: list[int], p: float) -> float:
    """
    Linear-interpolation percentile on a pre-sorted integer list.
    p must be in [0.0, 1.0].
    """
    n = len(sorted_values)
    if n == 1:
        return float(sorted_values[0])
    idx   = p * (n - 1)
    lower = int(idx)
    upper = min(lower + 1, n - 1)
    frac  = idx - lower
    return round(sorted_values[lower] + frac * (sorted_values[upper] - sorted_values[lower]), 2)


def _compute_top_values(non_null: list[str]) -> list[TopValue]:
    """Top-N most frequent values with their counts."""
    counter = Counter(non_null)
    return [
        TopValue(value=val, count=cnt)
        for val, cnt in counter.most_common(settings.TOP_N_VALUES)
    ]
