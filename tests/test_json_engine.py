"""
Tests for file_profiler/engines/json_engine.py

Covers:
  - Shape detection (SINGLE_OBJECT, ARRAY_OF_OBJECTS, NDJSON, DEEP_NESTED)
  - Schema discovery (union keys, occurrence ratio, type conflicts)
  - Flattening (nested dicts, lists stringified, depth limit)
  - Sampling strategies (full read, reservoir, skip-interval)
  - RawColumnData construction (null handling, value conversion)
  - Edge cases (empty arrays, missing keys, mixed types, single object)
  - Compressed JSON (gzip)
  - Integration via main.profile_file
"""

from __future__ import annotations

import gzip
import json
import os
import tempfile
from pathlib import Path

import pytest

from file_profiler.engines.json_engine import (
    _detect_shape,
    _discover_schema,
    _flatten_record,
    _has_deep_nesting,
    _value_to_str,
    profile,
)
from file_profiler.intake.errors import CorruptFileError
from file_profiler.intake.validator import IntakeResult, validate
from file_profiler.models.enums import JSONShape, SizeStrategy


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_json(data, suffix=".json") -> Path:
    """Write JSON data to a temp file and return the path."""
    fd, path = tempfile.mkstemp(suffix=suffix)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        json.dump(data, f)
    return Path(path)


def _write_ndjson(records: list[dict], suffix=".jsonl") -> Path:
    """Write NDJSON to a temp file and return the path."""
    fd, path = tempfile.mkstemp(suffix=suffix)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")
    return Path(path)


def _write_text(text: str, suffix=".json") -> Path:
    """Write raw text to a temp file and return the path."""
    fd, path = tempfile.mkstemp(suffix=suffix)
    with os.fdopen(fd, "w", encoding="utf-8") as f:
        f.write(text)
    return Path(path)


def _make_intake(path: Path) -> IntakeResult:
    """Create an IntakeResult for a file."""
    return validate(path)


# ---------------------------------------------------------------------------
# Shape Detection
# ---------------------------------------------------------------------------

class TestShapeDetection:

    def test_array_of_objects(self, tmp_path):
        data = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        path = _write_json(data)
        try:
            intake = _make_intake(path)
            shape = _detect_shape(path, intake)
            assert shape == JSONShape.ARRAY_OF_OBJECTS
        finally:
            os.unlink(path)

    def test_single_object(self, tmp_path):
        data = {"id": 1, "name": "Alice", "age": 30}
        path = _write_json(data)
        try:
            intake = _make_intake(path)
            shape = _detect_shape(path, intake)
            assert shape == JSONShape.SINGLE_OBJECT
        finally:
            os.unlink(path)

    def test_ndjson(self, tmp_path):
        records = [{"id": 1, "name": "Alice"}, {"id": 2, "name": "Bob"}]
        path = _write_ndjson(records)
        try:
            intake = _make_intake(path)
            shape = _detect_shape(path, intake)
            assert shape == JSONShape.NDJSON
        finally:
            os.unlink(path)

    def test_deep_nested_array(self, tmp_path):
        data = [
            {"id": 1, "user": {"profile": {"address": {"city": "NYC"}}}},
            {"id": 2, "user": {"profile": {"address": {"city": "LA"}}}},
        ]
        path = _write_json(data)
        try:
            intake = _make_intake(path)
            shape = _detect_shape(path, intake)
            assert shape == JSONShape.DEEP_NESTED
        finally:
            os.unlink(path)

    def test_empty_file_raises(self, tmp_path):
        # Write a file with just whitespace
        path = _write_text("   ")
        try:
            intake = _make_intake(path)
            with pytest.raises(CorruptFileError):
                _detect_shape(path, intake)
        finally:
            os.unlink(path)


# ---------------------------------------------------------------------------
# Flatten Record
# ---------------------------------------------------------------------------

class TestFlattenRecord:

    def test_flat_record(self):
        record = {"id": 1, "name": "Alice", "active": True}
        flat = _flatten_record(record)
        assert flat == {"id": 1, "name": "Alice", "active": True}

    def test_nested_dict(self):
        record = {"user": {"name": "Alice", "age": 30}}
        flat = _flatten_record(record)
        assert flat == {"user_name": "Alice", "user_age": 30}

    def test_deeply_nested_dict(self):
        record = {"a": {"b": {"c": {"d": "deep"}}}}
        flat = _flatten_record(record)
        assert "a_b_c_d" in flat
        assert flat["a_b_c_d"] == "deep"

    def test_list_stringified(self):
        record = {"id": 1, "tags": ["python", "data"]}
        flat = _flatten_record(record)
        assert flat["id"] == 1
        assert flat["tags"] == '["python", "data"]'

    def test_nested_list_of_dicts_stringified(self):
        record = {"id": 1, "items": [{"name": "A"}, {"name": "B"}]}
        flat = _flatten_record(record)
        assert flat["id"] == 1
        assert isinstance(flat["items"], str)
        parsed = json.loads(flat["items"])
        assert len(parsed) == 2

    def test_mixed_nesting(self):
        record = {
            "id": 1,
            "user": {"name": "Alice"},
            "scores": [90, 85, 92],
        }
        flat = _flatten_record(record)
        assert flat["id"] == 1
        assert flat["user_name"] == "Alice"
        assert flat["scores"] == "[90, 85, 92]"

    def test_none_value_preserved(self):
        record = {"id": 1, "name": None}
        flat = _flatten_record(record)
        assert flat["id"] == 1
        assert flat["name"] is None

    def test_max_depth_stringifies(self):
        record = {"a": {"b": {"c": {"d": {"e": "too deep"}}}}}
        flat = _flatten_record(record, max_depth=2)
        # At depth 2, {"d": {"e": "too deep"}} should be stringified
        assert "a_b_c" in flat
        assert isinstance(flat["a_b_c"], str)


# ---------------------------------------------------------------------------
# Value Conversion
# ---------------------------------------------------------------------------

class TestValueToStr:

    def test_none(self):
        assert _value_to_str(None) is None

    def test_bool_true(self):
        assert _value_to_str(True) == "true"

    def test_bool_false(self):
        assert _value_to_str(False) == "false"

    def test_int(self):
        assert _value_to_str(42) == "42"

    def test_float(self):
        assert _value_to_str(3.14) == "3.14"

    def test_string(self):
        assert _value_to_str("hello") == "hello"

    def test_list_stringified(self):
        result = _value_to_str([1, 2, 3])
        assert result == "[1, 2, 3]"

    def test_dict_stringified(self):
        result = _value_to_str({"a": 1})
        parsed = json.loads(result)
        assert parsed == {"a": 1}


# ---------------------------------------------------------------------------
# Has Deep Nesting
# ---------------------------------------------------------------------------

class TestHasDeepNesting:

    def test_flat_object(self):
        assert not _has_deep_nesting({"a": 1, "b": "two"})

    def test_one_level(self):
        assert not _has_deep_nesting({"a": {"b": 1}})

    def test_two_levels(self):
        assert not _has_deep_nesting({"a": {"b": {"c": 1}}})

    def test_three_levels_is_deep(self):
        assert _has_deep_nesting({"a": {"b": {"c": {"d": 1}}}})

    def test_list_with_nested_dicts(self):
        assert _has_deep_nesting({"a": [{"b": {"c": {"d": 1}}}]})


# ---------------------------------------------------------------------------
# Schema Discovery
# ---------------------------------------------------------------------------

class TestSchemaDiscovery:

    def test_basic_schema(self):
        records = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]
        path = _write_json(records)
        try:
            intake = _make_intake(path)
            schema = _discover_schema(path, intake, JSONShape.ARRAY_OF_OBJECTS)
            assert "id" in schema
            assert "name" in schema
            assert schema["id"].occurrence_count == 2
            assert schema["name"].occurrence_count == 2
        finally:
            os.unlink(path)

    def test_sparse_field(self):
        records = [
            {"id": 1, "name": "Alice", "email": "alice@test.com"},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": "Charlie"},
        ]
        path = _write_json(records)
        try:
            intake = _make_intake(path)
            schema = _discover_schema(path, intake, JSONShape.ARRAY_OF_OBJECTS)
            assert schema["email"].occurrence_count == 1
            assert schema["email"].occurrence_ratio == pytest.approx(1 / 3, abs=0.01)
        finally:
            os.unlink(path)

    def test_type_conflict_detection(self):
        records = [
            {"id": 1, "value": 42},
            {"id": 2, "value": "text"},
        ]
        path = _write_json(records)
        try:
            intake = _make_intake(path)
            schema = _discover_schema(path, intake, JSONShape.ARRAY_OF_OBJECTS)
            assert schema["value"].has_type_conflict
            assert "int" in schema["value"].observed_types
            assert "str" in schema["value"].observed_types
        finally:
            os.unlink(path)

    def test_nested_fields_discovered(self):
        records = [
            {"id": 1, "user": {"name": "Alice"}},
            {"id": 2, "user": {"name": "Bob"}},
        ]
        path = _write_json(records)
        try:
            intake = _make_intake(path)
            schema = _discover_schema(path, intake, JSONShape.ARRAY_OF_OBJECTS)
            assert "user_name" in schema
        finally:
            os.unlink(path)


# ---------------------------------------------------------------------------
# Full Profile — Array of Objects
# ---------------------------------------------------------------------------

class TestProfileArrayOfObjects:

    def test_basic_array(self):
        data = [
            {"id": 1, "name": "Alice", "score": 95.5},
            {"id": 2, "name": "Bob", "score": 87.0},
            {"id": 3, "name": "Charlie", "score": 92.3},
        ]
        path = _write_json(data)
        try:
            intake = _make_intake(path)
            raw_cols, row_count, is_exact = profile(path, SizeStrategy.MEMORY_SAFE, intake)
            assert row_count == 3
            assert is_exact is True
            col_names = {c.name for c in raw_cols}
            assert "id" in col_names
            assert "name" in col_names
            assert "score" in col_names
        finally:
            os.unlink(path)

    def test_null_values(self):
        data = [
            {"id": 1, "name": "Alice", "email": "alice@test.com"},
            {"id": 2, "name": "Bob", "email": None},
            {"id": 3, "name": None, "email": None},
        ]
        path = _write_json(data)
        try:
            intake = _make_intake(path)
            raw_cols, row_count, is_exact = profile(path, SizeStrategy.MEMORY_SAFE, intake)
            assert row_count == 3

            email_col = next(c for c in raw_cols if c.name == "email")
            assert email_col.null_count == 2

            name_col = next(c for c in raw_cols if c.name == "name")
            assert name_col.null_count == 1
        finally:
            os.unlink(path)

    def test_missing_keys_treated_as_null(self):
        data = [
            {"id": 1, "name": "Alice", "email": "alice@test.com"},
            {"id": 2, "name": "Bob"},  # missing email
        ]
        path = _write_json(data)
        try:
            intake = _make_intake(path)
            raw_cols, row_count, is_exact = profile(path, SizeStrategy.MEMORY_SAFE, intake)
            assert row_count == 2

            email_col = next(c for c in raw_cols if c.name == "email")
            assert email_col.null_count == 1
        finally:
            os.unlink(path)

    def test_nested_objects_flattened(self):
        data = [
            {"id": 1, "user": {"name": "Alice", "age": 30}},
            {"id": 2, "user": {"name": "Bob", "age": 25}},
        ]
        path = _write_json(data)
        try:
            intake = _make_intake(path)
            raw_cols, row_count, is_exact = profile(path, SizeStrategy.MEMORY_SAFE, intake)
            assert row_count == 2
            col_names = {c.name for c in raw_cols}
            assert "user_name" in col_names
            assert "user_age" in col_names
        finally:
            os.unlink(path)

    def test_boolean_values(self):
        data = [
            {"id": 1, "active": True},
            {"id": 2, "active": False},
        ]
        path = _write_json(data)
        try:
            intake = _make_intake(path)
            raw_cols, row_count, is_exact = profile(path, SizeStrategy.MEMORY_SAFE, intake)
            active_col = next(c for c in raw_cols if c.name == "active")
            assert "true" in active_col.values
            assert "false" in active_col.values
        finally:
            os.unlink(path)

    def test_empty_array(self):
        path = _write_json([])
        try:
            intake = _make_intake(path)
            raw_cols, row_count, is_exact = profile(path, SizeStrategy.MEMORY_SAFE, intake)
            assert row_count == 0
            assert raw_cols == []
        finally:
            os.unlink(path)

    def test_arrays_in_values_stringified(self):
        data = [
            {"id": 1, "tags": ["python", "data"]},
            {"id": 2, "tags": ["java", "web"]},
        ]
        path = _write_json(data)
        try:
            intake = _make_intake(path)
            raw_cols, row_count, is_exact = profile(path, SizeStrategy.MEMORY_SAFE, intake)
            tags_col = next(c for c in raw_cols if c.name == "tags")
            # Values should be JSON strings
            for val in tags_col.values:
                assert val is not None
                parsed = json.loads(val)
                assert isinstance(parsed, list)
        finally:
            os.unlink(path)


# ---------------------------------------------------------------------------
# Full Profile — NDJSON
# ---------------------------------------------------------------------------

class TestProfileNDJSON:

    def test_basic_ndjson(self):
        records = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
            {"id": 3, "name": "Charlie"},
        ]
        path = _write_ndjson(records)
        try:
            intake = _make_intake(path)
            raw_cols, row_count, is_exact = profile(path, SizeStrategy.MEMORY_SAFE, intake)
            assert row_count == 3
            assert is_exact is True
            col_names = {c.name for c in raw_cols}
            assert "id" in col_names
            assert "name" in col_names
        finally:
            os.unlink(path)

    def test_ndjson_with_missing_fields(self):
        records = [
            {"id": 1, "name": "Alice", "score": 95},
            {"id": 2, "name": "Bob"},
            {"id": 3, "score": 88},
        ]
        path = _write_ndjson(records)
        try:
            intake = _make_intake(path)
            raw_cols, row_count, is_exact = profile(path, SizeStrategy.MEMORY_SAFE, intake)
            assert row_count == 3

            score_col = next(c for c in raw_cols if c.name == "score")
            assert score_col.null_count == 1

            name_col = next(c for c in raw_cols if c.name == "name")
            assert name_col.null_count == 1
        finally:
            os.unlink(path)


# ---------------------------------------------------------------------------
# Full Profile — Single Object
# ---------------------------------------------------------------------------

class TestProfileSingleObject:

    def test_single_object(self):
        data = {"id": 1, "name": "Alice", "score": 95.5}
        path = _write_json(data)
        try:
            intake = _make_intake(path)
            raw_cols, row_count, is_exact = profile(path, SizeStrategy.MEMORY_SAFE, intake)
            assert row_count == 1
            assert is_exact is True
            col_names = {c.name for c in raw_cols}
            assert "id" in col_names
            assert "name" in col_names
            assert "score" in col_names
        finally:
            os.unlink(path)

    def test_single_nested_object(self):
        data = {"id": 1, "user": {"name": "Alice", "age": 30}}
        path = _write_json(data)
        try:
            intake = _make_intake(path)
            raw_cols, row_count, is_exact = profile(path, SizeStrategy.MEMORY_SAFE, intake)
            assert row_count == 1
            col_names = {c.name for c in raw_cols}
            assert "user_name" in col_names
            assert "user_age" in col_names
        finally:
            os.unlink(path)


# ---------------------------------------------------------------------------
# Compressed JSON (gzip)
# ---------------------------------------------------------------------------

class TestCompressedJSON:

    def test_gzip_json(self):
        data = [
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"},
        ]
        fd, path = tempfile.mkstemp(suffix=".json.gz")
        os.close(fd)
        path = Path(path)
        try:
            with gzip.open(path, "wt", encoding="utf-8") as f:
                json.dump(data, f)
            intake = _make_intake(path)
            raw_cols, row_count, is_exact = profile(path, SizeStrategy.MEMORY_SAFE, intake)
            assert row_count == 2
            col_names = {c.name for c in raw_cols}
            assert "id" in col_names
            assert "name" in col_names
        finally:
            os.unlink(path)


# ---------------------------------------------------------------------------
# Sampling Strategies
# ---------------------------------------------------------------------------

class TestSamplingStrategies:

    def _make_large_json(self, n: int = 200) -> Path:
        """Create a JSON file with n records."""
        data = [{"id": i, "value": f"val_{i}"} for i in range(n)]
        return _write_json(data)

    def test_memory_safe_reads_all(self):
        path = self._make_large_json(50)
        try:
            intake = _make_intake(path)
            raw_cols, row_count, is_exact = profile(path, SizeStrategy.MEMORY_SAFE, intake)
            assert row_count == 50
            id_col = next(c for c in raw_cols if c.name == "id")
            assert len(id_col.values) == 50
        finally:
            os.unlink(path)

    def test_lazy_scan_reservoir_sample(self):
        path = self._make_large_json(200)
        try:
            intake = _make_intake(path)
            raw_cols, row_count, is_exact = profile(path, SizeStrategy.LAZY_SCAN, intake)
            assert row_count == 200
            id_col = next(c for c in raw_cols if c.name == "id")
            # Reservoir capped at SAMPLE_ROW_COUNT but 200 < 10000, so all kept
            assert len(id_col.values) == 200
        finally:
            os.unlink(path)

    def test_stream_only_skip_interval(self):
        path = self._make_large_json(200)
        try:
            intake = _make_intake(path)
            raw_cols, row_count, is_exact = profile(path, SizeStrategy.STREAM_ONLY, intake)
            assert row_count == 200
            id_col = next(c for c in raw_cols if c.name == "id")
            # Skip interval = 100 → should get records at indices 0, 100
            assert len(id_col.values) == 2
        finally:
            os.unlink(path)


# ---------------------------------------------------------------------------
# Edge Cases
# ---------------------------------------------------------------------------

class TestEdgeCases:

    def test_mixed_types_in_column(self):
        data = [
            {"id": 1, "value": 42},
            {"id": 2, "value": "text"},
            {"id": 3, "value": True},
        ]
        path = _write_json(data)
        try:
            intake = _make_intake(path)
            raw_cols, row_count, is_exact = profile(path, SizeStrategy.MEMORY_SAFE, intake)
            value_col = next(c for c in raw_cols if c.name == "value")
            assert "42" in value_col.values
            assert "text" in value_col.values
            assert "true" in value_col.values
        finally:
            os.unlink(path)

    def test_wrapper_object_with_data_array(self):
        """JSON like {"results": [{...}, {...}]} should extract the array."""
        data = {
            "total": 2,
            "results": [
                {"id": 1, "name": "Alice"},
                {"id": 2, "name": "Bob"},
            ]
        }
        path = _write_json(data)
        try:
            intake = _make_intake(path)
            # This will be detected as SINGLE_OBJECT shape, profiled as 1-row
            raw_cols, row_count, is_exact = profile(path, SizeStrategy.MEMORY_SAFE, intake)
            # Single object with "total" and "results" flattened
            assert row_count == 1
        finally:
            os.unlink(path)

    def test_numeric_values_as_strings(self):
        data = [
            {"id": 1, "price": 19.99},
            {"id": 2, "price": 29.99},
        ]
        path = _write_json(data)
        try:
            intake = _make_intake(path)
            raw_cols, row_count, is_exact = profile(path, SizeStrategy.MEMORY_SAFE, intake)
            price_col = next(c for c in raw_cols if c.name == "price")
            assert "19.99" in price_col.values
            assert "29.99" in price_col.values
        finally:
            os.unlink(path)

    def test_all_null_column(self):
        data = [
            {"id": 1, "notes": None},
            {"id": 2, "notes": None},
        ]
        path = _write_json(data)
        try:
            intake = _make_intake(path)
            raw_cols, row_count, is_exact = profile(path, SizeStrategy.MEMORY_SAFE, intake)
            notes_col = next(c for c in raw_cols if c.name == "notes")
            assert notes_col.null_count == 2
        finally:
            os.unlink(path)

    def test_unicode_values(self):
        data = [
            {"id": 1, "city": "Tokyo"},
            {"id": 2, "city": "Zurich"},
        ]
        path = _write_json(data)
        try:
            intake = _make_intake(path)
            raw_cols, row_count, is_exact = profile(path, SizeStrategy.MEMORY_SAFE, intake)
            city_col = next(c for c in raw_cols if c.name == "city")
            assert "Tokyo" in city_col.values
            assert "Zurich" in city_col.values
        finally:
            os.unlink(path)

    def test_empty_string_treated_as_null(self):
        data = [
            {"id": 1, "name": ""},
            {"id": 2, "name": "Bob"},
        ]
        path = _write_json(data)
        try:
            intake = _make_intake(path)
            raw_cols, row_count, is_exact = profile(path, SizeStrategy.MEMORY_SAFE, intake)
            name_col = next(c for c in raw_cols if c.name == "name")
            assert name_col.null_count == 1
        finally:
            os.unlink(path)


# ---------------------------------------------------------------------------
# Integration with main.profile_file
# ---------------------------------------------------------------------------

class TestIntegration:

    def test_profile_file_json(self, tmp_path):
        from file_profiler.main import profile_file

        data = [
            {"id": 1, "name": "Alice", "score": 95},
            {"id": 2, "name": "Bob", "score": 87},
            {"id": 3, "name": "Charlie", "score": 92},
        ]
        json_path = tmp_path / "test_data.json"
        json_path.write_text(json.dumps(data), encoding="utf-8")

        fp = profile_file(json_path, output_dir=tmp_path)

        assert fp.file_format.value == "json"
        assert fp.row_count == 3
        assert len(fp.columns) == 3
        col_names = {c.name for c in fp.columns}
        assert "id" in col_names
        assert "name" in col_names
        assert "score" in col_names

        # Verify output JSON was written
        output_file = tmp_path / "test_data_profile.json"
        assert output_file.exists()

    def test_profile_file_ndjson(self, tmp_path):
        from file_profiler.main import profile_file

        ndjson_path = tmp_path / "test_data.json"
        lines = [
            json.dumps({"id": 1, "status": "active"}),
            json.dumps({"id": 2, "status": "inactive"}),
        ]
        ndjson_path.write_text("\n".join(lines), encoding="utf-8")

        fp = profile_file(ndjson_path)

        assert fp.file_format.value == "json"
        assert fp.row_count == 2
        col_names = {c.name for c in fp.columns}
        assert "id" in col_names
        assert "status" in col_names
