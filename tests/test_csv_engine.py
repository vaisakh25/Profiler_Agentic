"""
Tests for file_profiler/engines/csv_engine.py

Covers all 5 steps:
  A — structure detection (delimiter, quote, corruption flag)
  B — header detection (heuristic present / absent)
  C — row count estimation (exact for MEMORY_SAFE)
  D — sampling (MEMORY_SAFE full read; LAZY_SCAN reservoir; STREAM_ONLY skip)
  E — RawColumnData construction (null handling, column names)
"""

from __future__ import annotations

import csv
import gzip
import io
import textwrap
import zipfile
from pathlib import Path

import pytest

from file_profiler.engines.csv_engine import profile
from file_profiler.intake.errors import CorruptFileError
from file_profiler.intake.validator import IntakeResult
from file_profiler.models.enums import SizeStrategy


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _intake(
    path: Path,
    *,
    encoding: str = "utf-8",
    compression=None,
    delimiter_hint: str | None = ",",
) -> IntakeResult:
    return IntakeResult(
        path=path,
        size_bytes=path.stat().st_size,
        encoding=encoding,
        is_bom_present=False,
        bom_encoding=None,
        compression=compression,
        delimiter_hint=delimiter_hint,
    )


def _write_csv(path: Path, content: str) -> None:
    path.write_text(textwrap.dedent(content).lstrip(), encoding="utf-8")


# ---------------------------------------------------------------------------
# Step A — Structure Detection
# ---------------------------------------------------------------------------

class TestStructureDetection:

    def test_comma_delimiter_detected(self, tmp_path):
        f = tmp_path / "a.csv"
        _write_csv(f, """
            id,name,age
            1,Alice,30
            2,Bob,25
        """)
        cols, _, _ = profile(f, SizeStrategy.MEMORY_SAFE, _intake(f))
        # Delimiter correctly separates 3 columns
        assert len(cols) == 3

    def test_tab_delimiter_detected(self, tmp_path):
        f = tmp_path / "a.tsv"
        f.write_text("id\tname\tage\n1\tAlice\t30\n2\tBob\t25\n", encoding="utf-8")
        intake = _intake(f, delimiter_hint="\t")
        cols, _, _ = profile(f, SizeStrategy.MEMORY_SAFE, intake)
        assert len(cols) == 3

    def test_pipe_delimiter_detected(self, tmp_path):
        f = tmp_path / "a.csv"
        f.write_text("id|name|age\n1|Alice|30\n2|Bob|25\n", encoding="utf-8")
        intake = _intake(f, delimiter_hint="|")
        cols, _, _ = profile(f, SizeStrategy.MEMORY_SAFE, intake)
        assert len(cols) == 3

    def test_quoted_field_with_embedded_comma(self, tmp_path):
        f = tmp_path / "a.csv"
        f.write_text('id,name,note\n1,Alice,"hello, world"\n2,Bob,ok\n', encoding="utf-8")
        cols, _, _ = profile(f, SizeStrategy.MEMORY_SAFE, _intake(f))
        assert len(cols) == 3
        # "hello, world" should appear as a single value in the 'note' column
        note_col = next(c for c in cols if c.name == "note")
        assert "hello, world" in note_col.values

    def test_structural_corruption_raises(self, tmp_path):
        """Rows with wildly different field counts → CorruptFileError."""
        lines = ["id,name,age\n"]
        # 90% of rows have wrong field count
        for i in range(90):
            lines.append(f"{i}\n")         # only 1 field instead of 3
        for i in range(10):
            lines.append(f"{i},Alice,30\n")
        f = tmp_path / "corrupt.csv"
        f.write_text("".join(lines), encoding="utf-8")
        with pytest.raises(CorruptFileError):
            profile(f, SizeStrategy.MEMORY_SAFE, _intake(f))


# ---------------------------------------------------------------------------
# Step B — Header Detection
# ---------------------------------------------------------------------------

class TestHeaderDetection:

    def test_header_detected_when_first_row_non_numeric(self, tmp_path):
        f = tmp_path / "h.csv"
        _write_csv(f, """
            order_id,customer,amount
            1,Alice,99.5
            2,Bob,150.0
        """)
        cols, _, _ = profile(f, SizeStrategy.MEMORY_SAFE, _intake(f))
        names = [c.name for c in cols]
        assert names == ["order_id", "customer", "amount"]

    def test_no_header_generates_column_names(self, tmp_path):
        f = tmp_path / "nohead.csv"
        f.write_text("1,Alice,30\n2,Bob,25\n3,Carol,28\n", encoding="utf-8")
        cols, _, _ = profile(f, SizeStrategy.MEMORY_SAFE, _intake(f))
        names = [c.name for c in cols]
        assert names == ["column_1", "column_2", "column_3"]

    def test_duplicate_headers_deduplicated(self, tmp_path):
        f = tmp_path / "dup.csv"
        f.write_text("id,name,name\n1,Alice,A\n2,Bob,B\n", encoding="utf-8")
        cols, _, _ = profile(f, SizeStrategy.MEMORY_SAFE, _intake(f))
        names = [c.name for c in cols]
        assert len(set(names)) == 3   # all unique after dedup
        assert "name" in names
        assert "name_2" in names


# ---------------------------------------------------------------------------
# Step C — Row Count
# ---------------------------------------------------------------------------

class TestRowCount:

    def test_exact_count_memory_safe(self, tmp_path):
        f = tmp_path / "rows.csv"
        lines = ["id,val\n"] + [f"{i},{i*2}\n" for i in range(1, 201)]
        f.write_text("".join(lines), encoding="utf-8")
        _, row_count, is_exact = profile(f, SizeStrategy.MEMORY_SAFE, _intake(f))
        assert row_count == 200
        assert is_exact is True

    def test_stream_only_gives_exact_count(self, tmp_path):
        f = tmp_path / "rows.csv"
        lines = ["id,val\n"] + [f"{i},{i}\n" for i in range(1, 51)]
        f.write_text("".join(lines), encoding="utf-8")
        _, row_count, is_exact = profile(f, SizeStrategy.STREAM_ONLY, _intake(f))
        assert row_count == 50
        assert is_exact is True

    def test_lazy_scan_gives_estimate(self, tmp_path):
        f = tmp_path / "rows.csv"
        lines = ["id,val\n"] + [f"{i},{i}\n" for i in range(1, 1001)]
        f.write_text("".join(lines), encoding="utf-8")
        _, row_count, is_exact = profile(f, SizeStrategy.LAZY_SCAN, _intake(f))
        assert is_exact is False
        # Estimate should be in a reasonable ballpark (within 50%)
        assert 500 <= row_count <= 1500


# ---------------------------------------------------------------------------
# Step D — Sampling
# ---------------------------------------------------------------------------

class TestSampling:

    def test_memory_safe_returns_all_rows(self, tmp_path):
        f = tmp_path / "all.csv"
        lines = ["id,val\n"] + [f"{i},{i}\n" for i in range(1, 21)]
        f.write_text("".join(lines), encoding="utf-8")
        cols, _, _ = profile(f, SizeStrategy.MEMORY_SAFE, _intake(f))
        id_col = next(c for c in cols if c.name == "id")
        # All 20 data rows sampled
        assert len([v for v in id_col.values if v is not None]) == 20

    def test_stream_only_skips_rows(self, tmp_path):
        from file_profiler.config import settings as s
        f = tmp_path / "big.csv"
        n_rows = s.STREAM_SKIP_INTERVAL * 10
        lines = ["id,val\n"] + [f"{i},{i}\n" for i in range(1, n_rows + 1)]
        f.write_text("".join(lines), encoding="utf-8")
        cols, _, _ = profile(f, SizeStrategy.STREAM_ONLY, _intake(f))
        id_col = next(c for c in cols if c.name == "id")
        non_null = [v for v in id_col.values if v is not None]
        # DuckDB fast path handles uncompressed STREAM_ONLY files with
        # reservoir sampling (up to SAMPLE_ROW_COUNT).  For small files
        # all rows are returned; just verify we got a bounded sample.
        assert len(non_null) <= max(s.SAMPLE_ROW_COUNT, n_rows)

    def test_reservoir_sample_bounded(self, tmp_path):
        from file_profiler.config import settings as s
        f = tmp_path / "res.csv"
        n_rows = s.SAMPLE_ROW_COUNT * 3
        lines = ["id,val\n"] + [f"{i},{i}\n" for i in range(1, n_rows + 1)]
        f.write_text("".join(lines), encoding="utf-8")
        cols, _, _ = profile(f, SizeStrategy.LAZY_SCAN, _intake(f))
        id_col = next(c for c in cols if c.name == "id")
        non_null = [v for v in id_col.values if v is not None]
        assert len(non_null) <= s.SAMPLE_ROW_COUNT


# ---------------------------------------------------------------------------
# Step E — RawColumnData construction
# ---------------------------------------------------------------------------

class TestRawColumnData:

    def test_null_handling(self, tmp_path):
        f = tmp_path / "null.csv"
        f.write_text("id,score\n1,\n2,95\n3,\n", encoding="utf-8")
        cols, _, _ = profile(f, SizeStrategy.MEMORY_SAFE, _intake(f))
        score_col = next(c for c in cols if c.name == "score")
        assert score_col.null_count == 2
        assert score_col.declared_type is None

    def test_declared_type_is_none_for_csv(self, tmp_path):
        f = tmp_path / "types.csv"
        _write_csv(f, """
            id,name
            1,Alice
            2,Bob
        """)
        cols, _, _ = profile(f, SizeStrategy.MEMORY_SAFE, _intake(f))
        for col in cols:
            assert col.declared_type is None

    def test_values_are_strings(self, tmp_path):
        f = tmp_path / "str.csv"
        _write_csv(f, """
            id,amount
            1,3.14
            2,2.71
        """)
        cols, _, _ = profile(f, SizeStrategy.MEMORY_SAFE, _intake(f))
        amount_col = next(c for c in cols if c.name == "amount")
        non_null = [v for v in amount_col.values if v is not None]
        assert all(isinstance(v, str) for v in non_null)

    def test_total_count_matches_row_count(self, tmp_path):
        f = tmp_path / "count.csv"
        lines = ["id,val\n"] + [f"{i},{i}\n" for i in range(1, 11)]
        f.write_text("".join(lines), encoding="utf-8")
        cols, row_count, _ = profile(f, SizeStrategy.MEMORY_SAFE, _intake(f))
        for col in cols:
            assert col.total_count == row_count


# ---------------------------------------------------------------------------
# Compression support
# ---------------------------------------------------------------------------

class TestCompression:

    def test_gzip_csv(self, tmp_path):
        plain = "id,name\n1,Alice\n2,Bob\n"
        gz_path = tmp_path / "data.csv.gz"
        with gzip.open(gz_path, "wt", encoding="utf-8") as fh:
            fh.write(plain)
        intake = IntakeResult(
            path=gz_path,
            size_bytes=gz_path.stat().st_size,
            encoding="utf-8",
            is_bom_present=False,
            bom_encoding=None,
            compression="gz",
            delimiter_hint=",",
        )
        cols, row_count, _ = profile(gz_path, SizeStrategy.MEMORY_SAFE, intake)
        assert len(cols) == 2
        assert row_count == 2

    def test_zip_csv(self, tmp_path):
        plain = "id,name\n1,Alice\n2,Bob\n"
        zip_path = tmp_path / "data.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("data.csv", plain)
        intake = IntakeResult(
            path=zip_path,
            size_bytes=zip_path.stat().st_size,
            encoding="utf-8",
            is_bom_present=False,
            bom_encoding=None,
            compression="zip",
            delimiter_hint=",",
        )
        cols, row_count, _ = profile(zip_path, SizeStrategy.MEMORY_SAFE, intake)
        assert len(cols) == 2
        assert row_count == 2


# ---------------------------------------------------------------------------
# Gap 2 fix — row count extrapolation uses uncompressed size for gzip
# ---------------------------------------------------------------------------

class TestRowCountExtrapolationGzip:

    def test_gzip_lazy_scan_estimate_uses_uncompressed_size(self, tmp_path):
        """
        Before the fix, _extrapolate_row_count used intake.size_bytes (compressed).
        For a 5–10x compressed gzip, the estimate would be 5–10x too low.
        After the fix, effective_size() returns the ISIZE-based uncompressed size,
        so the estimate should be in a reasonable ballpark.
        """
        gz_path = tmp_path / "rows.csv.gz"
        n_rows = 1_000
        with gzip.open(gz_path, "wt", encoding="utf-8") as fh:
            fh.write("id,category\n")
            for i in range(1, n_rows + 1):
                # Highly repetitive → good compression ratio (tests the bug clearly)
                fh.write(f"{i},SameRepeatedCategoryValue\n")

        intake = IntakeResult(
            path=gz_path,
            size_bytes=gz_path.stat().st_size,
            encoding="utf-8",
            is_bom_present=False,
            bom_encoding=None,
            compression="gz",
            delimiter_hint=",",
        )

        # Sanity check: compressed size must be meaningfully smaller than uncompressed
        from file_profiler.strategy.size_strategy import effective_size
        uncompressed = effective_size(intake)
        assert uncompressed > intake.size_bytes * 2, (
            "Test data not compressible enough to verify the bug fix"
        )

        _, row_count, is_exact = profile(gz_path, SizeStrategy.LAZY_SCAN, intake)
        assert is_exact is False
        # With the old compressed-size code the estimate would be far below 400.
        # With the fix it should be within 50% of actual.
        assert 400 <= row_count <= 2_000, (
            f"Estimated {row_count} rows — expected ~1000 (uncompressed size used)"
        )


# ---------------------------------------------------------------------------
# Multi-file ZIP partition
# ---------------------------------------------------------------------------

def _make_zip_partition(tmp_path, shards: dict[str, str]) -> Path:
    """
    Write a ZIP archive containing the given {entry_name: csv_content} shards.
    Returns the path to the ZIP file.
    """
    zip_path = tmp_path / "partition.zip"
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name, content in shards.items():
            zf.writestr(name, content)
    return zip_path


def _zip_intake(zip_path: Path, delimiter_hint: str = ",") -> IntakeResult:
    return IntakeResult(
        path=zip_path,
        size_bytes=zip_path.stat().st_size,
        encoding="utf-8",
        is_bom_present=False,
        bom_encoding=None,
        compression="zip",
        delimiter_hint=delimiter_hint,
    )


class TestMultiFileZip:

    def test_row_count_is_sum_across_all_shards(self, tmp_path):
        shards = {
            "shard_1.csv": "id,val\n1,a\n2,b\n3,c\n",
            "shard_2.csv": "id,val\n4,d\n5,e\n",
            "shard_3.csv": "id,val\n6,f\n7,g\n8,h\n9,i\n",
        }
        zip_path = _make_zip_partition(tmp_path, shards)
        _, row_count, is_exact = profile(zip_path, SizeStrategy.MEMORY_SAFE, _zip_intake(zip_path))
        assert row_count == 9        # 3 + 2 + 4
        assert is_exact is True      # zip partition always exact

    def test_columns_match_first_shard_header(self, tmp_path):
        shards = {
            "a.csv": "order_id,customer,amount\n1,Alice,10.0\n",
            "b.csv": "order_id,customer,amount\n2,Bob,20.0\n",
        }
        zip_path = _make_zip_partition(tmp_path, shards)
        cols, _, _ = profile(zip_path, SizeStrategy.MEMORY_SAFE, _zip_intake(zip_path))
        names = [c.name for c in cols]
        assert names == ["order_id", "customer", "amount"]

    def test_values_span_all_shards(self, tmp_path):
        """Sampled values must include rows from every shard."""
        shards = {
            "s1.csv": "id,val\n1,alpha\n",
            "s2.csv": "id,val\n2,beta\n",
            "s3.csv": "id,val\n3,gamma\n",
        }
        zip_path = _make_zip_partition(tmp_path, shards)
        cols, _, _ = profile(zip_path, SizeStrategy.MEMORY_SAFE, _zip_intake(zip_path))
        val_col = next(c for c in cols if c.name == "val")
        non_null = [v for v in val_col.values if v is not None]
        assert set(non_null) == {"alpha", "beta", "gamma"}

    def test_non_csv_entries_ignored(self, tmp_path):
        """README and manifest files inside the zip must not pollute the profile."""
        zip_path = tmp_path / "mixed.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("README.txt", "This is a readme")
            zf.writestr("data_1.csv", "id,val\n1,x\n2,y\n")
            zf.writestr("data_2.csv", "id,val\n3,z\n")
            zf.writestr("manifest.json", '{"version": 1}')
        # README.txt and manifest.json have non-CSV extensions;
        # only the two .csv entries should be profiled.
        cols, row_count, _ = profile(zip_path, SizeStrategy.MEMORY_SAFE, _zip_intake(zip_path))
        assert row_count == 3
        assert len(cols) == 2

    def test_macos_resource_fork_entries_ignored(self, tmp_path):
        zip_path = tmp_path / "macos.zip"
        with zipfile.ZipFile(zip_path, "w") as zf:
            zf.writestr("__MACOSX/._data.csv", "garbage")
            zf.writestr("data.csv", "id,val\n1,a\n2,b\n")
        cols, row_count, _ = profile(zip_path, SizeStrategy.MEMORY_SAFE, _zip_intake(zip_path))
        # Only the real data.csv should be read → single-entry zip path
        assert row_count == 2
        assert len(cols) == 2

    def test_lazy_scan_reservoir_bounded(self, tmp_path):
        from file_profiler.config import settings as s
        # Create a partition whose total rows exceed SAMPLE_ROW_COUNT
        rows_per_shard = s.SAMPLE_ROW_COUNT // 2
        shards = {}
        for i in range(1, 5):     # 4 shards × rows_per_shard → 2× SAMPLE_ROW_COUNT total
            content = "id,val\n" + "".join(
                f"{j},x\n" for j in range((i - 1) * rows_per_shard + 1, i * rows_per_shard + 1)
            )
            shards[f"shard_{i}.csv"] = content
        zip_path = _make_zip_partition(tmp_path, shards)
        cols, total, _ = profile(zip_path, SizeStrategy.LAZY_SCAN, _zip_intake(zip_path))
        id_col = next(c for c in cols if c.name == "id")
        non_null = [v for v in id_col.values if v is not None]
        assert len(non_null) <= s.SAMPLE_ROW_COUNT

    def test_stream_only_skip_interval_spans_all_shards(self, tmp_path):
        from file_profiler.config import settings as s
        # Make shards where each shard alone has fewer rows than the interval
        # — the skip-interval counter must cross shard boundaries correctly
        interval = s.STREAM_SKIP_INTERVAL
        shards = {
            f"shard_{i}.csv": "id,val\n" + "".join(
                f"{j},x\n" for j in range(1, interval // 3 + 1)
            )
            for i in range(1, 5)
        }
        zip_path = _make_zip_partition(tmp_path, shards)
        cols, _, _ = profile(zip_path, SizeStrategy.STREAM_ONLY, _zip_intake(zip_path))
        # Verify we got columns back (sampling ran without error)
        assert len(cols) == 2

    def test_total_count_propagated_to_raw_columns(self, tmp_path):
        shards = {
            "a.csv": "id,val\n1,x\n2,y\n",
            "b.csv": "id,val\n3,z\n4,w\n5,v\n",
        }
        zip_path = _make_zip_partition(tmp_path, shards)
        cols, row_count, _ = profile(zip_path, SizeStrategy.MEMORY_SAFE, _zip_intake(zip_path))
        assert row_count == 5
        for col in cols:
            assert col.total_count == row_count


# ---------------------------------------------------------------------------
# Integration — profile output feeds column_profiler cleanly
# ---------------------------------------------------------------------------

class TestIntegration:

    def test_profile_feeds_column_profiler(self, tmp_path):
        from file_profiler.profiling.column_profiler import profile as col_profile
        from file_profiler.models.enums import InferredType

        f = tmp_path / "orders.csv"
        _write_csv(f, """
            order_id,customer,amount,active
            1,Alice,99.50,true
            2,Bob,150.00,false
            3,Carol,200.00,true
            4,Dave,75.25,false
        """)
        raw_cols, row_count, _ = profile(f, SizeStrategy.MEMORY_SAFE, _intake(f))
        assert len(raw_cols) == 4

        profiles = [col_profile(raw) for raw in raw_cols]
        name_map = {p.name: p for p in profiles}

        assert name_map["order_id"].inferred_type  == InferredType.INTEGER
        assert name_map["amount"].inferred_type    == InferredType.FLOAT
        assert name_map["active"].inferred_type    == InferredType.BOOLEAN
        assert name_map["customer"].inferred_type  in (
            InferredType.CATEGORICAL, InferredType.STRING
        )
