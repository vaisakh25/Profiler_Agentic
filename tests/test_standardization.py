"""
Tests for the standardization layer (file_profiler/standardization/normalizer.py).

Covers:
  TestNameNormalization     — _normalize_name rules
  TestNameDeduplication     — _build_name_map collision handling
  TestNullSentinels         — null-like value → None
  TestWhitespaceTrimming    — leading/trailing whitespace
  TestBooleanNormalization  — Yes/No/Y/N/… → true/false
  TestBooleanPreScanGuard   — column-level boolean threshold
  TestNumericCleaning       — $, €, %, grouping commas
  TestStandardizeIntegration — full standardize() with report
  TestIdempotency           — standardize() twice = same result
  TestEdgeCases             — all-None, empty list, type_inference preserved
  TestMainIntegration       — profile_file() end-to-end with standardization
"""

from __future__ import annotations

import json
import textwrap
from pathlib import Path
from typing import Optional

import pytest

from file_profiler.models.enums import QualityFlag
from file_profiler.models.file_profile import RawColumnData, TopValue
from file_profiler.standardization.normalizer import (
    _BOOL_NORMALIZE,
    _build_name_map,
    _clean_numeric,
    _is_boolean_column,
    _normalize_name,
    _standardize_values,
    standardize,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _raw(
    name: str,
    values: list[Optional[str]],
    null_count: int = 0,
    total_count: int = 0,
    declared_type: Optional[str] = None,
) -> RawColumnData:
    """Shorthand for building a RawColumnData."""
    return RawColumnData(
        name=name,
        declared_type=declared_type,
        values=values,
        total_count=total_count or len(values),
        null_count=null_count,
    )


# ---------------------------------------------------------------------------
# TestNameNormalization
# ---------------------------------------------------------------------------

class TestNameNormalization:

    def test_basic_snake_case(self):
        assert _normalize_name("First Name") == "first_name"

    def test_leading_trailing_whitespace(self):
        assert _normalize_name("  Age  ") == "age"

    def test_hyphens(self):
        assert _normalize_name("first-name") == "first_name"

    def test_dots(self):
        assert _normalize_name("user.email") == "user_email"

    def test_mixed_special_chars(self):
        assert _normalize_name("Order #ID (2024)") == "order_id_2024"

    def test_consecutive_underscores(self):
        assert _normalize_name("a___b") == "a_b"

    def test_leading_digit(self):
        assert _normalize_name("1st_column") == "col_1st_column"

    def test_empty_after_normalization(self):
        assert _normalize_name("!!!") == "unnamed"

    def test_already_snake_case_idempotent(self):
        assert _normalize_name("order_id") == "order_id"

    def test_uppercase(self):
        assert _normalize_name("ORDER_ID") == "order_id"

    def test_tabs_and_newlines(self):
        assert _normalize_name("\tmy col\n") == "my_col"

    def test_auto_generated_name_unchanged(self):
        assert _normalize_name("column_1") == "column_1"

    def test_single_char(self):
        assert _normalize_name("X") == "x"

    def test_leading_trailing_underscores_stripped(self):
        assert _normalize_name("__name__") == "name"


# ---------------------------------------------------------------------------
# TestNameDeduplication
# ---------------------------------------------------------------------------

class TestNameDeduplication:

    def test_no_collision(self):
        mapping, collisions = _build_name_map(["id", "name", "age"])
        assert mapping == {"id": "id", "name": "name", "age": "age"}
        assert collisions == set()

    def test_two_names_collide(self):
        mapping, collisions = _build_name_map(["First Name", "first_name"])
        assert mapping["First Name"] == "first_name"
        assert mapping["first_name"] == "first_name_2"
        assert "first_name" in collisions

    def test_triple_collision(self):
        mapping, _ = _build_name_map(["A", "a", " A "])
        assert mapping["A"] == "a"
        assert mapping["a"] == "a_2"
        assert mapping[" A "] == "a_3"

    def test_mixed_collision_and_unique(self):
        mapping, collisions = _build_name_map(["First Name", "first_name", "age"])
        assert mapping["age"] == "age"
        assert "first_name" in collisions


# ---------------------------------------------------------------------------
# TestNullSentinels
# ---------------------------------------------------------------------------

class TestNullSentinels:

    def _std_vals(self, values):
        """Run _standardize_values and return new values."""
        new_vals, _ = _standardize_values(
            values, original_name="col", standardized_name="col"
        )
        return new_vals

    def test_null_uppercase(self):
        assert self._std_vals(["NULL"])[0] is None

    def test_null_lowercase(self):
        assert self._std_vals(["null"])[0] is None

    def test_na_slash(self):
        assert self._std_vals(["N/A"])[0] is None

    def test_na_plain(self):
        assert self._std_vals(["NA"])[0] is None

    def test_none_string(self):
        assert self._std_vals(["None"])[0] is None

    def test_dash(self):
        assert self._std_vals(["-"])[0] is None

    def test_nan(self):
        assert self._std_vals(["nan"])[0] is None

    def test_nil(self):
        assert self._std_vals(["nil"])[0] is None

    def test_missing(self):
        assert self._std_vals(["missing"])[0] is None

    def test_undefined(self):
        assert self._std_vals(["undefined"])[0] is None

    def test_dot(self):
        assert self._std_vals(["."])[0] is None

    def test_double_dash(self):
        assert self._std_vals(["--"])[0] is None

    def test_empty_after_trim(self):
        assert self._std_vals(["   "])[0] is None

    def test_empty_string(self):
        assert self._std_vals([""])[0] is None

    def test_regular_value_unchanged(self):
        assert self._std_vals(["hello"]) == ["hello"]

    def test_zero_is_not_null(self):
        assert self._std_vals(["0"]) != [None]

    def test_sentinel_with_whitespace(self):
        assert self._std_vals(["  n/a  "])[0] is None

    def test_none_input_passes_through(self):
        assert self._std_vals([None])[0] is None


# ---------------------------------------------------------------------------
# TestWhitespaceTrimming
# ---------------------------------------------------------------------------

class TestWhitespaceTrimming:

    def test_leading_spaces_trimmed(self):
        new_vals, detail = _standardize_values(
            ["  hello"], original_name="c", standardized_name="c"
        )
        assert new_vals == ["hello"]
        assert detail.whitespace_trimmed == 1

    def test_trailing_spaces_trimmed(self):
        new_vals, _ = _standardize_values(
            ["hello  "], original_name="c", standardized_name="c"
        )
        assert new_vals == ["hello"]

    def test_no_whitespace_no_count(self):
        _, detail = _standardize_values(
            ["hello"], original_name="c", standardized_name="c"
        )
        assert detail.whitespace_trimmed == 0

    def test_multiple_values_counted(self):
        _, detail = _standardize_values(
            ["  a  ", "b", "  c"], original_name="c", standardized_name="c"
        )
        assert detail.whitespace_trimmed == 2


# ---------------------------------------------------------------------------
# TestBooleanNormalization
# ---------------------------------------------------------------------------

class TestBooleanNormalization:

    def _bool_col(self, values):
        """Standardize a predominantly boolean column."""
        new_vals, detail = _standardize_values(
            values, original_name="flag", standardized_name="flag"
        )
        return new_vals, detail

    def test_yes_to_true(self):
        vals, _ = self._bool_col(["Yes", "No", "Yes", "No"])
        assert vals == ["true", "false", "true", "false"]

    def test_true_uppercase_to_lowercase(self):
        vals, _ = self._bool_col(["TRUE", "FALSE", "TRUE"])
        assert vals == ["true", "false", "true"]

    def test_y_n_to_true_false(self):
        vals, _ = self._bool_col(["Y", "N", "Y", "N"])
        assert vals == ["true", "false", "true", "false"]

    def test_one_zero_to_true_false(self):
        vals, _ = self._bool_col(["1", "0", "1", "0"])
        assert vals == ["true", "false", "true", "false"]

    def test_t_f_to_true_false(self):
        vals, _ = self._bool_col(["T", "F", "T", "F"])
        assert vals == ["true", "false", "true", "false"]

    def test_already_canonical_idempotent(self):
        vals, detail = self._bool_col(["true", "false", "true"])
        assert vals == ["true", "false", "true"]
        assert detail.booleans_normalized == 0

    def test_count_tracked(self):
        _, detail = self._bool_col(["Yes", "No", "true", "false"])
        assert detail.booleans_normalized == 2  # Yes + No changed; true + false unchanged


# ---------------------------------------------------------------------------
# TestBooleanPreScanGuard
# ---------------------------------------------------------------------------

class TestBooleanPreScanGuard:

    def test_100_percent_boolean_normalized(self):
        assert _is_boolean_column(["Yes", "No", "Yes", "No"])

    def test_50_percent_boolean_not_normalized(self):
        # Only 50% are boolean tokens
        assert not _is_boolean_column(["1", "2", "3", "0"])

    def test_89_percent_not_normalized(self):
        # 8/9 ≈ 88.9% < 90%
        vals = ["true"] * 8 + ["hello"]
        assert not _is_boolean_column(vals)

    def test_90_percent_normalized(self):
        # 9/10 = 90% → normalized
        vals = ["true"] * 9 + ["hello"]
        assert _is_boolean_column(vals)

    def test_all_none_not_boolean(self):
        assert not _is_boolean_column([None, None, None])

    def test_empty_list_not_boolean(self):
        assert not _is_boolean_column([])

    def test_mixed_column_values_unchanged(self):
        # Column of integers where "1" and "0" happen to be boolean tokens
        new_vals, detail = _standardize_values(
            ["1", "2", "3", "4", "0"],
            original_name="count",
            standardized_name="count",
        )
        # 2 out of 5 are boolean = 40% < 90% → no boolean normalization
        assert new_vals == ["1", "2", "3", "4", "0"]
        assert detail.booleans_normalized == 0


# ---------------------------------------------------------------------------
# TestNumericCleaning
# ---------------------------------------------------------------------------

class TestNumericCleaning:

    def test_currency_dollar(self):
        assert _clean_numeric("$1,234.56") == "1234.56"

    def test_currency_euro(self):
        assert _clean_numeric("\u20ac500") == "500"

    def test_currency_pound(self):
        assert _clean_numeric("\u00a3100") == "100"

    def test_currency_yen(self):
        assert _clean_numeric("\u00a51000") == "1000"

    def test_percent_suffix(self):
        assert _clean_numeric("85.5%") == "85.5"

    def test_grouping_commas(self):
        assert _clean_numeric("1,000,000") == "1000000"

    def test_non_numeric_dollar_unchanged(self):
        assert _clean_numeric("$ales") == "$ales"

    def test_already_clean(self):
        assert _clean_numeric("42.5") == "42.5"

    def test_negative_with_currency(self):
        # Only strips leading currency, not minus sign
        assert _clean_numeric("$-50") == "-50"

    def test_combined_currency_and_commas(self):
        assert _clean_numeric("$1,234") == "1234"

    def test_count_tracked_in_standardize(self):
        _, detail = _standardize_values(
            ["$100", "200", "$300"],
            original_name="amount",
            standardized_name="amount",
        )
        assert detail.numerics_cleaned == 2


# ---------------------------------------------------------------------------
# TestStandardizeIntegration
# ---------------------------------------------------------------------------

class TestStandardizeIntegration:

    def test_returns_new_raw_column_data(self):
        col = _raw("First Name", ["  alice  ", "bob", None])
        result, _ = standardize([col])
        assert result[0] is not col
        assert result[0].name == "first_name"

    def test_values_cleaned(self):
        col = _raw("Col", ["  hello  ", "NULL", "world", None], null_count=1)
        result, _ = standardize([col])
        assert result[0].values == ["hello", None, "world", None]

    def test_null_count_adjusted(self):
        col = _raw("Col", ["N/A", "value", "null"], null_count=0)
        result, _ = standardize([col])
        # 2 sentinels normalised
        assert result[0].null_count == 2

    def test_report_columns_renamed(self):
        cols = [
            _raw("First Name", ["alice"]),
            _raw("age", ["25"]),
        ]
        _, report = standardize(cols)
        assert report.columns_renamed == 1  # only First Name renamed

    def test_report_totals(self):
        cols = [
            _raw("Col A", ["  hello  ", "NULL"]),
            _raw("Col B", ["$100", "world"]),
        ]
        _, report = standardize(cols)
        assert report.total_whitespace_trimmed >= 1
        assert report.total_nulls_normalized >= 1
        assert report.total_numerics_cleaned >= 1

    def test_details_list_length(self):
        cols = [_raw("a", ["1"]), _raw("b", ["2"]), _raw("c", ["3"])]
        _, report = standardize(cols)
        assert len(report.details) == 3

    def test_declared_type_preserved(self):
        col = _raw("Amount", ["100"], declared_type="FLOAT64")
        result, _ = standardize([col])
        assert result[0].declared_type == "FLOAT64"

    def test_total_count_preserved(self):
        col = _raw("X", ["a", "b"], total_count=1000)
        result, _ = standardize([col])
        assert result[0].total_count == 1000

    def test_name_collision_tracked(self):
        cols = [
            _raw("First Name", ["alice"]),
            _raw("first_name", ["bob"]),
        ]
        result, report = standardize(cols)
        assert result[0].name == "first_name"
        assert result[1].name == "first_name_2"
        assert report.columns_renamed == 2  # both changed from original perspective

    def test_boolean_column_standardized(self):
        col = _raw("Active", ["Yes", "No", "Yes", "No"])
        result, report = standardize([col])
        assert result[0].values == ["true", "false", "true", "false"]
        assert report.total_booleans_normalized == 4


# ---------------------------------------------------------------------------
# TestIdempotency
# ---------------------------------------------------------------------------

class TestIdempotency:

    def test_second_pass_identical(self):
        cols = [
            _raw("First Name", ["  alice  ", "NULL", "Yes", "$100"]),
        ]
        result1, report1 = standardize(cols)
        result2, report2 = standardize(result1)

        # Values should be identical
        assert result1[0].values == result2[0].values
        assert result1[0].name == result2[0].name

    def test_second_pass_no_changes(self):
        cols = [_raw("First Name", ["  alice  ", "NULL", "$100"])]
        result1, _ = standardize(cols)
        _, report2 = standardize(result1)

        # Second pass should report zero changes (except possibly name re-normalization)
        assert report2.total_nulls_normalized == 0
        assert report2.total_whitespace_trimmed == 0
        assert report2.total_numerics_cleaned == 0


# ---------------------------------------------------------------------------
# TestEdgeCases
# ---------------------------------------------------------------------------

class TestEdgeCases:

    def test_all_none_column(self):
        col = _raw("x", [None, None, None], null_count=3)
        result, report = standardize([col])
        assert result[0].values == [None, None, None]
        assert result[0].null_count == 3
        assert report.total_nulls_normalized == 0

    def test_empty_column_list(self):
        result, report = standardize([])
        assert result == []
        assert report.columns_renamed == 0

    def test_type_inference_preserved(self):
        from file_profiler.models.file_profile import TypeInferenceResult
        from file_profiler.models.enums import InferredType
        ti = TypeInferenceResult(
            inferred_type=InferredType.INTEGER,
            confidence_score=0.99,
        )
        col = RawColumnData(
            name="id",
            declared_type="int64",
            values=["1", "2", "3"],
            total_count=3,
            null_count=0,
            type_inference=ti,
        )
        result, _ = standardize([col])
        assert result[0].type_inference is ti

    def test_single_value_column(self):
        col = _raw("x", ["hello"])
        result, _ = standardize([col])
        assert result[0].values == ["hello"]

    def test_all_sentinels_column(self):
        col = _raw("x", ["NULL", "N/A", "null", "-"], null_count=0)
        result, _ = standardize([col])
        assert result[0].values == [None, None, None, None]
        assert result[0].null_count == 4


# ---------------------------------------------------------------------------
# TestMainIntegration
# ---------------------------------------------------------------------------

class TestMainIntegration:

    def _write_csv(self, path: Path, content: str) -> None:
        path.write_text(textwrap.dedent(content).lstrip(), encoding="utf-8")

    def test_standardization_applied_flag_set(self, tmp_path):
        from file_profiler.main import profile_file
        f = tmp_path / "test.csv"
        self._write_csv(f, """
            First Name,Last Name,Age
            Alice,Smith,30
            Bob,Jones,25
        """)
        fp = profile_file(f)
        assert fp.standardization_applied is True

    def test_original_name_set_for_renamed_columns(self, tmp_path):
        from file_profiler.main import profile_file
        f = tmp_path / "test.csv"
        self._write_csv(f, """
            First Name,Last Name,Age
            Alice,Smith,30
            Bob,Jones,25
        """)
        fp = profile_file(f)
        first_name_col = next(c for c in fp.columns if c.name == "first_name")
        assert first_name_col.original_name == "First Name"

    def test_original_name_none_for_unchanged(self, tmp_path):
        from file_profiler.main import profile_file
        f = tmp_path / "test.csv"
        self._write_csv(f, """
            id,name,age
            1,Alice,30
            2,Bob,25
        """)
        fp = profile_file(f)
        id_col = next(c for c in fp.columns if c.name == "id")
        assert id_col.original_name is None

    def test_null_variant_flag_set(self, tmp_path):
        from file_profiler.main import profile_file
        f = tmp_path / "test.csv"
        self._write_csv(f, "\n".join(
            ["id,value"] +
            [f"{i},NULL" for i in range(1, 11)] +
            [f"{i},hello" for i in range(11, 21)]
        ))
        fp = profile_file(f)
        value_col = next(c for c in fp.columns if c.name == "value")
        assert QualityFlag.NULL_VARIANT_NORMALIZED in value_col.quality_flags

    def test_json_output_contains_original_name(self, tmp_path):
        from file_profiler.main import profile_file
        f = tmp_path / "test.csv"
        self._write_csv(f, """
            First Name,Age
            Alice,30
            Bob,25
        """)
        out_dir = tmp_path / "output"
        fp = profile_file(f, output_dir=out_dir)
        json_path = out_dir / "test_profile.json"
        assert json_path.exists()
        data = json.loads(json_path.read_text(encoding="utf-8"))
        first_col = data["columns"][0]
        assert first_col["original_name"] == "First Name"
        assert data["standardization_applied"] is True
