"""
Layer 7 — Column Profiling Engine

Entry point:  profile(raw: RawColumnData) -> ColumnProfile

Computes statistics, cardinality, and distributions for a single column
based on the raw data (or sample) provided by the format engine.
"""

from __future__ import annotations

import logging
import statistics
from collections import Counter

from file_profiler.config import settings
from file_profiler.models.enums import Cardinality, InferredType
from file_profiler.models.file_profile import (
    ColumnProfile,
    RawColumnData,
    TopValue,
)
from file_profiler.profiling import type_inference

log = logging.getLogger(__name__)


def profile(raw: RawColumnData) -> ColumnProfile:
    """
    Compute the full profile for a single column.

    If type inference has not already been performed (e.g. by the engine),
    it is triggered here.
    """
    # 1. Ensure type inference is done
    if raw.type_inference:
        inference = raw.type_inference
    else:
        inference = type_inference.infer(raw.values)

    # 2. Filter valid values for statistics
    #    (None values are excluded from stats but counted in null_count)
    non_null_values = [v for v in raw.values if v is not None]
    sample_count = len(non_null_values)

    # 3. Cardinality & Uniqueness
    distinct_count = len(set(non_null_values))

    unique_ratio = 0.0
    if sample_count > 0:
        unique_ratio = distinct_count / sample_count

    cardinality = _determine_cardinality(unique_ratio)

    # 4. Distribution (Top N)
    #    Counter handles the frequency counting
    counter = Counter(non_null_values)
    top_values = [
        TopValue(value=val, count=cnt)
        for val, cnt in counter.most_common(settings.TOP_N_VALUES)
    ]

    # 5. Numeric / String Statistics
    min_val, max_val, skewness = _compute_range_stats(non_null_values, inference.inferred_type)
    len_stats = _compute_length_stats(non_null_values, inference.inferred_type)

    # 6. Boolean Flags
    is_nullable = raw.null_count > 0

    # Constant: effectively only one distinct value (ignoring nulls)
    is_constant = (distinct_count <= 1)

    is_sparse = False
    if raw.total_count > 0:
        is_sparse = (raw.null_count / raw.total_count) > settings.NULL_HEAVY_THRESHOLD

    # Key candidate: must be unique, non-null, and not a float/bool/text blob
    is_key_candidate = (
        unique_ratio == 1.0
        and raw.null_count == 0
        and inference.inferred_type not in (InferredType.FLOAT, InferredType.BOOLEAN, InferredType.FREE_TEXT)
    )

    # 7. Assemble Profile
    return ColumnProfile(
        name=raw.name,
        declared_type=raw.declared_type,
        inferred_type=inference.inferred_type,
        confidence_score=inference.confidence_score,

        null_count=raw.null_count,
        distinct_count=distinct_count,
        is_distinct_count_exact=(len(raw.values) == raw.total_count),

        unique_ratio=unique_ratio,
        cardinality=cardinality,

        is_nullable=is_nullable,
        is_constant=is_constant,
        is_sparse=is_sparse,
        is_key_candidate=is_key_candidate,
        is_low_cardinality=(cardinality == Cardinality.LOW),

        min=min_val,
        max=max_val,
        skewness=skewness,

        avg_length=len_stats.get("avg"),
        length_p10=len_stats.get("p10"),
        length_p50=len_stats.get("p50"),
        length_p90=len_stats.get("p90"),
        length_max=len_stats.get("max"),

        top_values=top_values,
        sample_values=raw.values[:settings.SAMPLE_VALUES_COUNT],
        quality_flags=inference.quality_flags,
    )


def _determine_cardinality(unique_ratio: float) -> Cardinality:
    if unique_ratio >= settings.CARDINALITY_HIGH_THRESHOLD:
        return Cardinality.HIGH
    if unique_ratio <= settings.CARDINALITY_LOW_THRESHOLD:
        return Cardinality.LOW
    return Cardinality.MEDIUM


def _compute_range_stats(
    values: list[str], inferred_type: InferredType
) -> tuple[str | None, str | None, float | None]:
    """Compute min, max, and skewness."""
    if not values:
        return None, None, None

    # Numeric stats
    if inferred_type in (InferredType.INTEGER, InferredType.FLOAT):
        try:
            # Convert to float for calculation
            # Remove commas for safety (though standardization might have done it)
            nums = [float(v.replace(",", "")) for v in values]

            min_v = min(nums)
            max_v = max(nums)

            skew = None
            if len(nums) > 1:
                try:
                    skew = statistics.skew(nums)
                except Exception:
                    pass

            # Return as strings to match dataclass
            if inferred_type == InferredType.INTEGER:
                return str(int(min_v)), str(int(max_v)), skew
            return str(min_v), str(max_v), skew

        except ValueError:
            # Fallback if conversion fails
            pass

    # String/Date stats (Lexicographical)
    # Skip for Boolean, Categorical?
    # Dates should be comparable as strings if ISO.
    if inferred_type not in (InferredType.BOOLEAN, InferredType.CATEGORICAL):
        return min(values), max(values), None

    return None, None, None


def _compute_length_stats(
    values: list[str], inferred_type: InferredType
) -> dict[str, float | int]:
    """Compute string length distribution."""
    # Skip for types where length is irrelevant
    if inferred_type in (InferredType.INTEGER, InferredType.FLOAT, InferredType.BOOLEAN, InferredType.DATE, InferredType.TIMESTAMP):
        return {}

    if not values:
        return {}

    lengths = [len(v) for v in values]
    lengths.sort()
    n = len(lengths)

    return {
        "avg": statistics.mean(lengths),
        "max": lengths[-1],
        "p10": lengths[int(n * 0.10)],
        "p50": lengths[int(n * 0.50)],
        "p90": lengths[int(n * 0.90)],
    }