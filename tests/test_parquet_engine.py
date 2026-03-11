"""
Tests for file_profiler/engines/parquet_engine.py
"""

from __future__ import annotations

import datetime
import json
from pathlib import Path
from typing import Optional

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from file_profiler.config import settings
from file_profiler.engines.parquet_engine import profile
from file_profiler.models.enums import SizeStrategy
from file_profiler.models.file_profile import RawColumnData


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_parquet(path: Path, table: pa.Table) -> None:
    pq.write_table(table, str(path))


def _simple_table() -> pa.Table:
    return pa.table({
        "order_id": pa.array([1, 2, 3, 4, 5], type=pa.int64()),
        "customer": pa.array(["Alice", "Bob", "Carol", "Dave", "Eve"]),
        "amount":   pa.array([99.5, 150.0, 200.0, 75.25, 300.0], type=pa.float64()),
        "active":   pa.array([True, False, True, False, True], type=pa.bool_()),
    })


def _col(raw_cols: list[RawColumnData], name: str) -> RawColumnData:
    for c in raw_cols:
        if c.name == name:
            return c
    raise KeyError(f"Column '{name}' not found in {[c.name for c in raw_cols]}")


# ---------------------------------------------------------------------------
# Step A — Schema metadata read
# ---------------------------------------------------------------------------

class TestSchemaRead:

    def test_row_count_exact_from_metadata(self, tmp_path):
        p = tmp_path / "data.parquet"
        _write_parquet(p, _simple_table())
        _, row_count, is_exact = profile(p, SizeStrategy.MEMORY_SAFE)
        assert row_count == 5
        assert is_exact is True

    def test_all_columns_detected(self, tmp_path):
        p = tmp_path / "data.parquet"
        _write_parquet(p, _simple_table())
        raw_cols, _, _ = profile(p, SizeStrategy.MEMORY_SAFE)
        names = [c.name for c in raw_cols]
        assert "order_id" in names
        assert "customer" in names
        assert "amount"   in names
        assert "active"   in names

    def test_empty_file_returns_empty_list(self, tmp_path):
        p = tmp_path / "empty.parquet"
        _write_parquet(p, pa.table({"id": pa.array([], type=pa.int64())}))
        raw_cols, row_count, is_exact = profile(p, SizeStrategy.MEMORY_SAFE)
        assert raw_cols == []
        assert row_count == 0
        assert is_exact is True

    def test_returns_raw_column_data_objects(self, tmp_path):
        p = tmp_path / "data.parquet"
        _write_parquet(p, _simple_table())
        raw_cols, _, _ = profile(p, SizeStrategy.MEMORY_SAFE)
        assert all(isinstance(c, RawColumnData) for c in raw_cols)


# ---------------------------------------------------------------------------
# Declared types
# ---------------------------------------------------------------------------

class TestDeclaredTypes:

    def test_integer_declared_type(self, tmp_path):
        p = tmp_path / "data.parquet"
        _write_parquet(p, _simple_table())
        c = _col(profile(p, SizeStrategy.MEMORY_SAFE)[0], "order_id")
        assert "int" in c.declared_type.lower()

    def test_float_declared_type(self, tmp_path):
        p = tmp_path / "data.parquet"
        _write_parquet(p, _simple_table())
        c = _col(profile(p, SizeStrategy.MEMORY_SAFE)[0], "amount")
        assert "float" in c.declared_type.lower() or "double" in c.declared_type.lower()

    def test_string_declared_type(self, tmp_path):
        p = tmp_path / "data.parquet"
        _write_parquet(p, _simple_table())
        c = _col(profile(p, SizeStrategy.MEMORY_SAFE)[0], "customer")
        assert "string" in c.declared_type.lower() or "utf" in c.declared_type.lower()

    def test_boolean_declared_type(self, tmp_path):
        p = tmp_path / "data.parquet"
        _write_parquet(p, _simple_table())
        c = _col(profile(p, SizeStrategy.MEMORY_SAFE)[0], "active")
        assert "bool" in c.declared_type.lower()

    def test_date_declared_type(self, tmp_path):
        p = tmp_path / "dates.parquet"
        t = pa.table({
            "event_date": pa.array(
                [datetime.date(2024, 1, 1), datetime.date(2024, 6, 15)],
                type=pa.date32(),
            )
        })
        _write_parquet(p, t)
        c = _col(profile(p, SizeStrategy.MEMORY_SAFE)[0], "event_date")
        assert "date" in c.declared_type.lower()

    def test_timestamp_declared_type(self, tmp_path):
        p = tmp_path / "ts.parquet"
        t = pa.table({
            "created_at": pa.array(
                [datetime.datetime(2024, 1, 1, 12, 0, 0),
                 datetime.datetime(2024, 6, 1,  8, 30, 0)],
                type=pa.timestamp("us"),
            )
        })
        _write_parquet(p, t)
        c = _col(profile(p, SizeStrategy.MEMORY_SAFE)[0], "created_at")
        assert "timestamp" in c.declared_type.lower()


# ---------------------------------------------------------------------------
# Value formatting
# ---------------------------------------------------------------------------

class TestValueFormatting:

    def test_integer_values_as_strings(self, tmp_path):
        p = tmp_path / "data.parquet"
        _write_parquet(p, _simple_table())
        c = _col(profile(p, SizeStrategy.MEMORY_SAFE)[0], "order_id")
        assert all(isinstance(v, str) for v in c.values if v is not None)
        assert "1" in c.values

    def test_boolean_values_lowercase(self, tmp_path):
        p = tmp_path / "data.parquet"
        _write_parquet(p, _simple_table())
        c = _col(profile(p, SizeStrategy.MEMORY_SAFE)[0], "active")
        non_null = [v for v in c.values if v is not None]
        assert all(v in ("true", "false") for v in non_null)

    def test_date_values_iso_format(self, tmp_path):
        p = tmp_path / "dates.parquet"
        t = pa.table({
            "d": pa.array([datetime.date(2024, 1, 15)], type=pa.date32())
        })
        _write_parquet(p, t)
        c = _col(profile(p, SizeStrategy.MEMORY_SAFE)[0], "d")
        assert c.values[0] == "2024-01-15"

    def test_null_values_are_none(self, tmp_path):
        p = tmp_path / "nulls.parquet"
        t = pa.table({"x": pa.array([1, None, 3], type=pa.int64())})
        _write_parquet(p, t)
        c = _col(profile(p, SizeStrategy.MEMORY_SAFE)[0], "x")
        assert None in c.values


# ---------------------------------------------------------------------------
# Null counts
# ---------------------------------------------------------------------------

class TestNullCount:

    def test_null_count_correct(self, tmp_path):
        p = tmp_path / "nulls.parquet"
        t = pa.table({
            "a": pa.array([1, None, 3, None, 5], type=pa.int64()),
            "b": pa.array(["x", "y", None, "w", "v"]),
        })
        _write_parquet(p, t)
        raw_cols, _, _ = profile(p, SizeStrategy.MEMORY_SAFE)
        assert _col(raw_cols, "a").null_count == 2
        assert _col(raw_cols, "b").null_count == 1

    def test_zero_nulls(self, tmp_path):
        p = tmp_path / "nonull.parquet"
        _write_parquet(p, _simple_table())
        c = _col(profile(p, SizeStrategy.MEMORY_SAFE)[0], "order_id")
        assert c.null_count == 0

    def test_fully_null_column(self, tmp_path):
        p = tmp_path / "allnull.parquet"
        t = pa.table({"x": pa.array([None, None, None], type=pa.int64())})
        _write_parquet(p, t)
        c = _col(profile(p, SizeStrategy.MEMORY_SAFE)[0], "x")
        assert c.null_count == 3

    def test_total_count_equals_row_count(self, tmp_path):
        p = tmp_path / "data.parquet"
        _write_parquet(p, _simple_table())
        raw_cols, row_count, _ = profile(p, SizeStrategy.MEMORY_SAFE)
        for c in raw_cols:
            assert c.total_count == row_count == 5


# ---------------------------------------------------------------------------
# Step B — Nested field flattening
# ---------------------------------------------------------------------------

class TestNestedFields:

    @staticmethod
    def _struct_table() -> pa.Table:
        address_type = pa.struct([
            pa.field("street", pa.string()),
            pa.field("city",   pa.string()),
        ])
        return pa.table({
            "name": pa.array(["Alice", "Bob"]),
            "address": pa.array(
                [{"street": "123 Main St", "city": "NYC"},
                 {"street": "456 Oak Ave", "city": "LA"}],
                type=address_type,
            ),
        })

    def test_struct_column_expanded_to_leaves(self, tmp_path):
        p = tmp_path / "nested.parquet"
        _write_parquet(p, self._struct_table())
        raw_cols, _, _ = profile(p, SizeStrategy.MEMORY_SAFE)
        names = [c.name for c in raw_cols]
        assert "address"        not in names   # struct itself disappears
        assert "address_street" in names
        assert "address_city"   in names

    def test_struct_field_values_correct(self, tmp_path):
        p = tmp_path / "nested.parquet"
        _write_parquet(p, self._struct_table())
        raw_cols, _, _ = profile(p, SizeStrategy.MEMORY_SAFE)
        city_col = _col(raw_cols, "address_city")
        assert "NYC" in city_col.values
        assert "LA"  in city_col.values

    def test_flat_fields_also_present(self, tmp_path):
        p = tmp_path / "nested.parquet"
        _write_parquet(p, self._struct_table())
        raw_cols, _, _ = profile(p, SizeStrategy.MEMORY_SAFE)
        assert "name" in [c.name for c in raw_cols]

    def test_deeply_nested_struct(self, tmp_path):
        inner = pa.struct([pa.field("zip", pa.string())])
        outer = pa.struct([pa.field("city", pa.string()), pa.field("geo", inner)])
        t = pa.table({
            "loc": pa.array(
                [{"city": "NYC", "geo": {"zip": "10001"}}],
                type=outer,
            )
        })
        p = tmp_path / "deep.parquet"
        _write_parquet(p, t)
        raw_cols, _, _ = profile(p, SizeStrategy.MEMORY_SAFE)
        names = [c.name for c in raw_cols]
        assert "loc_city"    in names
        assert "loc_geo_zip" in names

    def test_list_column_stringified(self, tmp_path):
        p = tmp_path / "lists.parquet"
        t = pa.table({
            "tags": pa.array([["a", "b"], ["c"], None], type=pa.list_(pa.string()))
        })
        _write_parquet(p, t)
        raw_cols, _, _ = profile(p, SizeStrategy.MEMORY_SAFE)
        c = _col(raw_cols, "tags")
        non_null = [v for v in c.values if v is not None]
        # values should be JSON strings, not bare Python lists
        assert all(isinstance(v, str) for v in non_null)
        parsed = json.loads(non_null[0])
        assert isinstance(parsed, list)

    def test_list_column_declared_type_contains_list(self, tmp_path):
        p = tmp_path / "lists.parquet"
        t = pa.table({
            "tags": pa.array([["a", "b"]], type=pa.list_(pa.string()))
        })
        _write_parquet(p, t)
        raw_cols, _, _ = profile(p, SizeStrategy.MEMORY_SAFE)
        c = _col(raw_cols, "tags")
        assert "list" in c.declared_type.lower()


# ---------------------------------------------------------------------------
# Sampling strategies
# ---------------------------------------------------------------------------

class TestSampling:

    @staticmethod
    def _large_table(n_rows: int) -> pa.Table:
        return pa.table({
            "id":  pa.array(list(range(n_rows)), type=pa.int64()),
            "val": pa.array([f"v{i}" for i in range(n_rows)]),
        })

    def test_memory_safe_all_rows_included(self, tmp_path):
        p = tmp_path / "small.parquet"
        _write_parquet(p, self._large_table(20))
        raw_cols, _, _ = profile(p, SizeStrategy.MEMORY_SAFE)
        c = _col(raw_cols, "id")
        assert len(c.values) == 20

    def test_lazy_scan_bounded_by_sample_row_count(self, tmp_path):
        p = tmp_path / "big.parquet"
        n = settings.SAMPLE_ROW_COUNT + 200
        _write_parquet(p, self._large_table(n))
        raw_cols, row_count, _ = profile(p, SizeStrategy.LAZY_SCAN)
        assert row_count == n           # exact from metadata
        c = _col(raw_cols, "id")
        assert len(c.values) <= settings.SAMPLE_ROW_COUNT

    def test_lazy_scan_row_count_exact(self, tmp_path):
        p = tmp_path / "big.parquet"
        _write_parquet(p, self._large_table(300))
        _, row_count, is_exact = profile(p, SizeStrategy.LAZY_SCAN)
        assert row_count == 300
        assert is_exact is True

    def test_stream_only_samples_every_kth_row(self, tmp_path):
        p = tmp_path / "big.parquet"
        n = settings.STREAM_SKIP_INTERVAL * 10
        _write_parquet(p, self._large_table(n))
        raw_cols, _, _ = profile(p, SizeStrategy.STREAM_ONLY)
        c = _col(raw_cols, "id")
        expected = n // settings.STREAM_SKIP_INTERVAL
        assert abs(len(c.values) - expected) <= 2

    def test_lazy_scan_samples_non_empty(self, tmp_path):
        p = tmp_path / "big.parquet"
        _write_parquet(p, self._large_table(200))
        raw_cols, _, _ = profile(p, SizeStrategy.LAZY_SCAN)
        c = _col(raw_cols, "id")
        assert len(c.values) > 0


# ---------------------------------------------------------------------------
# Integration with main pipeline
# ---------------------------------------------------------------------------

class TestMainIntegration:

    def test_profile_file_parquet(self, tmp_path):
        from file_profiler.main import profile_file
        from file_profiler.models.enums import FileFormat
        from file_profiler.models.file_profile import FileProfile

        p = tmp_path / "orders.parquet"
        _write_parquet(p, _simple_table())
        fp = profile_file(p)
        assert isinstance(fp, FileProfile)
        assert fp.file_format == FileFormat.PARQUET
        assert fp.row_count   == 5
        assert len(fp.columns) == 4

    def test_profile_file_parquet_table_name(self, tmp_path):
        from file_profiler.main import profile_file

        p = tmp_path / "orders_2024.parquet"
        _write_parquet(p, _simple_table())
        fp = profile_file(p)
        assert fp.table_name == "orders_2024"

    def test_profile_file_parquet_type_inference(self, tmp_path):
        from file_profiler.main import profile_file
        from file_profiler.models.enums import InferredType

        p = tmp_path / "data.parquet"
        _write_parquet(p, _simple_table())
        fp = profile_file(p)
        col_map = {c.name: c for c in fp.columns}
        # Parquet integers should infer as INTEGER
        assert col_map["order_id"].inferred_type == InferredType.INTEGER
        # Boolean should infer as BOOLEAN
        assert col_map["active"].inferred_type == InferredType.BOOLEAN

    def test_profile_directory_includes_parquet(self, tmp_path):
        from file_profiler.main import profile_directory

        csv_file = tmp_path / "data.csv"
        csv_file.write_text("id,val\n1,a\n2,b\n", encoding="utf-8")
        pq_file = tmp_path / "data.parquet"
        _write_parquet(pq_file, _simple_table())

        results = profile_directory(tmp_path)
        assert len(results) == 2
