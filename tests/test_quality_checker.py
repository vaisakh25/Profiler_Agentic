"""
Tests for file_profiler/quality/structural_checker.py
"""

from __future__ import annotations

import pytest

from file_profiler.models.enums import Cardinality, InferredType, QualityFlag
from file_profiler.models.file_profile import ColumnProfile
from file_profiler.quality.structural_checker import check


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _col(
    name: str,
    *,
    null_count: int = 0,
    distinct_count: int = 10,
    is_constant: bool = False,
    is_sparse: bool = False,
    inferred_type: InferredType = InferredType.STRING,
    total_count: int = 100,
) -> ColumnProfile:
    """Build a minimal ColumnProfile for testing."""
    return ColumnProfile(
        name               = name,
        declared_type      = None,
        inferred_type      = inferred_type,
        confidence_score   = 1.0,
        null_count         = null_count,
        distinct_count     = distinct_count,
        is_distinct_count_exact = True,
        unique_ratio       = distinct_count / total_count if total_count else 0.0,
        cardinality        = Cardinality.MEDIUM,
        is_nullable        = null_count > 0,
        is_constant        = is_constant,
        is_sparse          = is_sparse,
    )


# ---------------------------------------------------------------------------
# Duplicate column names
# ---------------------------------------------------------------------------

class TestDuplicateColumnNames:

    def test_flags_both_copies_of_duplicate(self):
        profiles = [_col("id"), _col("name"), _col("name")]
        result, _ = check(profiles)
        name_flags = [p.quality_flags for p in result if p.name == "name"]
        assert all(QualityFlag.DUPLICATE_COLUMN_NAME in f for f in name_flags)

    def test_does_not_flag_unique_names(self):
        profiles = [_col("id"), _col("name"), _col("amount")]
        result, _ = check(profiles)
        for p in result:
            assert QualityFlag.DUPLICATE_COLUMN_NAME not in p.quality_flags

    def test_three_copies_all_flagged(self):
        profiles = [_col("x"), _col("x"), _col("x")]
        result, _ = check(profiles)
        assert all(QualityFlag.DUPLICATE_COLUMN_NAME in p.quality_flags for p in result)

    def test_only_duplicate_flagged_not_unique(self):
        profiles = [_col("id"), _col("val"), _col("val")]
        result, _ = check(profiles)
        id_profile = next(p for p in result if p.name == "id")
        assert QualityFlag.DUPLICATE_COLUMN_NAME not in id_profile.quality_flags


# ---------------------------------------------------------------------------
# Fully null columns
# ---------------------------------------------------------------------------

class TestFullyNull:

    def test_fully_null_column_flagged(self):
        profiles = [_col("empty", null_count=100, distinct_count=0)]
        result, _ = check(profiles)
        assert QualityFlag.FULLY_NULL in result[0].quality_flags

    def test_non_null_column_not_flagged(self):
        profiles = [_col("id", null_count=0, distinct_count=10)]
        result, _ = check(profiles)
        assert QualityFlag.FULLY_NULL not in result[0].quality_flags

    def test_partially_null_not_flagged_as_fully_null(self):
        profiles = [_col("score", null_count=30, distinct_count=5)]
        result, _ = check(profiles)
        assert QualityFlag.FULLY_NULL not in result[0].quality_flags

    def test_zero_nulls_zero_distinct_not_flagged(self):
        # Empty column with no rows — should not be flagged
        profiles = [_col("empty", null_count=0, distinct_count=0)]
        result, _ = check(profiles)
        assert QualityFlag.FULLY_NULL not in result[0].quality_flags


# ---------------------------------------------------------------------------
# Constant columns
# ---------------------------------------------------------------------------

class TestConstantColumn:

    def test_constant_column_flagged(self):
        profiles = [_col("status", is_constant=True, distinct_count=1)]
        result, _ = check(profiles)
        assert QualityFlag.CONSTANT_COLUMN in result[0].quality_flags

    def test_non_constant_column_not_flagged(self):
        profiles = [_col("status", is_constant=False, distinct_count=5)]
        result, _ = check(profiles)
        assert QualityFlag.CONSTANT_COLUMN not in result[0].quality_flags


# ---------------------------------------------------------------------------
# High null ratio
# ---------------------------------------------------------------------------

class TestHighNullRatio:

    def test_sparse_column_flagged(self):
        profiles = [_col("opt", is_sparse=True, null_count=80)]
        result, _ = check(profiles)
        assert QualityFlag.HIGH_NULL_RATIO in result[0].quality_flags

    def test_dense_column_not_flagged(self):
        profiles = [_col("name", is_sparse=False, null_count=2)]
        result, _ = check(profiles)
        assert QualityFlag.HIGH_NULL_RATIO not in result[0].quality_flags


# ---------------------------------------------------------------------------
# Column shift errors (file-level)
# ---------------------------------------------------------------------------

class TestColumnShiftErrors:

    def test_corrupt_rows_produce_structural_issue(self):
        profiles = [_col("id"), _col("val")]
        _, issues = check(profiles, corrupt_row_count=5)
        assert any("COLUMN_SHIFT_ERROR" in issue for issue in issues)
        assert any("5" in issue for issue in issues)

    def test_zero_corrupt_rows_no_issue(self):
        profiles = [_col("id"), _col("val")]
        _, issues = check(profiles, corrupt_row_count=0)
        assert not any("COLUMN_SHIFT_ERROR" in issue for issue in issues)


# ---------------------------------------------------------------------------
# Encoding inconsistency (file-level)
# ---------------------------------------------------------------------------

class TestEncodingInconsistency:

    def test_latin1_encoding_produces_structural_issue(self):
        profiles = [_col("id")]
        _, issues = check(profiles, encoding="latin-1")
        assert any("ENCODING_INCONSISTENCY" in issue for issue in issues)

    def test_iso88591_encoding_produces_structural_issue(self):
        profiles = [_col("id")]
        _, issues = check(profiles, encoding="iso-8859-1")
        assert any("ENCODING_INCONSISTENCY" in issue for issue in issues)

    def test_utf8_encoding_no_issue(self):
        profiles = [_col("id")]
        _, issues = check(profiles, encoding="utf-8")
        assert not any("ENCODING_INCONSISTENCY" in issue for issue in issues)

    def test_utf8sig_encoding_no_issue(self):
        profiles = [_col("id")]
        _, issues = check(profiles, encoding="utf-8-sig")
        assert not any("ENCODING_INCONSISTENCY" in issue for issue in issues)


# ---------------------------------------------------------------------------
# Idempotency — running check twice does not double-add flags
# ---------------------------------------------------------------------------

class TestIdempotency:

    def test_flags_not_duplicated_on_double_check(self):
        profiles = [_col("x", null_count=100, distinct_count=0)]
        check(profiles)
        check(profiles)   # second call
        assert profiles[0].quality_flags.count(QualityFlag.FULLY_NULL) == 1

    def test_duplicate_name_flag_not_duplicated(self):
        profiles = [_col("name"), _col("name")]
        check(profiles)
        check(profiles)
        for p in profiles:
            assert p.quality_flags.count(QualityFlag.DUPLICATE_COLUMN_NAME) == 1


# ---------------------------------------------------------------------------
# Combined — multiple flags on one column
# ---------------------------------------------------------------------------

class TestMultipleFlags:

    def test_column_can_have_multiple_flags(self):
        # A column that is both a duplicate AND sparse
        profiles = [
            _col("score", is_sparse=True),
            _col("score", is_sparse=True),
        ]
        result, _ = check(profiles)
        flags = result[0].quality_flags
        assert QualityFlag.DUPLICATE_COLUMN_NAME in flags
        assert QualityFlag.HIGH_NULL_RATIO in flags

    def test_empty_profiles_returns_no_issues(self):
        profiles, issues = check([])
        assert profiles == []
        assert issues == []
