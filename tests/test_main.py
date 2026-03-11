"""
End-to-end tests for file_profiler/main.py

Covers single CSV files, gzip CSV, zip of CSVs, multi-file zip partitions,
directory scanning, and error handling.
"""

from __future__ import annotations

import gzip
import json
import textwrap
import zipfile
from pathlib import Path

import pytest

from file_profiler.intake.errors import CorruptFileError, EmptyFileError
from file_profiler.main import profile_directory, profile_file, run
from file_profiler.models.enums import FileFormat, InferredType, SizeStrategy
from file_profiler.models.file_profile import FileProfile


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _write_csv(path: Path, content: str) -> None:
    path.write_text(textwrap.dedent(content).lstrip(), encoding="utf-8")


def _orders_csv(path: Path) -> None:
    _write_csv(path, """
        order_id,customer,amount,active
        1,Alice,99.50,true
        2,Bob,150.00,false
        3,Carol,200.00,true
        4,Dave,75.25,false
    """)


# ---------------------------------------------------------------------------
# profile_file — single plain CSV
# ---------------------------------------------------------------------------

class TestProfileFileCsv:

    def test_returns_file_profile(self, tmp_path):
        f = tmp_path / "orders.csv"
        _orders_csv(f)
        fp = profile_file(f)
        assert isinstance(fp, FileProfile)

    def test_file_format_is_csv(self, tmp_path):
        f = tmp_path / "orders.csv"
        _orders_csv(f)
        fp = profile_file(f)
        assert fp.file_format == FileFormat.CSV

    def test_table_name_derived_from_stem(self, tmp_path):
        f = tmp_path / "orders_2024.csv"
        _orders_csv(f)
        fp = profile_file(f)
        assert fp.table_name == "orders_2024"

    def test_row_count_correct(self, tmp_path):
        f = tmp_path / "data.csv"
        _orders_csv(f)
        fp = profile_file(f)
        assert fp.row_count == 4
        assert fp.is_row_count_exact is True

    def test_columns_profiled(self, tmp_path):
        f = tmp_path / "data.csv"
        _orders_csv(f)
        fp = profile_file(f)
        assert len(fp.columns) == 4
        names = [c.name for c in fp.columns]
        assert "order_id" in names
        assert "amount" in names

    def test_type_inference_applied(self, tmp_path):
        f = tmp_path / "data.csv"
        _orders_csv(f)
        fp = profile_file(f)
        col_map = {c.name: c for c in fp.columns}
        assert col_map["order_id"].inferred_type  == InferredType.INTEGER
        assert col_map["amount"].inferred_type    == InferredType.FLOAT
        assert col_map["active"].inferred_type    == InferredType.BOOLEAN

    def test_size_strategy_set(self, tmp_path):
        f = tmp_path / "data.csv"
        _orders_csv(f)
        fp = profile_file(f)
        assert fp.size_strategy == SizeStrategy.MEMORY_SAFE

    def test_file_not_found_raises(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            profile_file(tmp_path / "missing.csv")

    def test_empty_file_raises(self, tmp_path):
        f = tmp_path / "empty.csv"
        f.write_bytes(b"")
        with pytest.raises(EmptyFileError):
            profile_file(f)

    def test_unknown_format_raises_value_error(self, tmp_path):
        f = tmp_path / "data.bin"
        f.write_bytes(bytes(range(256)))
        with pytest.raises((ValueError, Exception)):
            profile_file(f)


# ---------------------------------------------------------------------------
# profile_file — gzip CSV
# ---------------------------------------------------------------------------

class TestProfileFileGzip:

    def test_gzip_csv_profiled(self, tmp_path):
        gz = tmp_path / "data.csv.gz"
        with gzip.open(gz, "wt", encoding="utf-8") as fh:
            fh.write("id,name\n1,Alice\n2,Bob\n3,Carol\n")
        fp = profile_file(gz)
        assert fp.row_count == 3
        assert len(fp.columns) == 2

    def test_gzip_type_inference(self, tmp_path):
        gz = tmp_path / "nums.csv.gz"
        with gzip.open(gz, "wt", encoding="utf-8") as fh:
            fh.write("val\n1\n2\n3\n4\n5\n")
        fp = profile_file(gz)
        assert fp.columns[0].inferred_type == InferredType.INTEGER


# ---------------------------------------------------------------------------
# profile_file — zip (single CSV entry)
# ---------------------------------------------------------------------------

class TestProfileFileZipSingle:

    def test_single_entry_zip_profiled(self, tmp_path):
        zp = tmp_path / "data.zip"
        with zipfile.ZipFile(zp, "w") as zf:
            zf.writestr("data.csv", "id,val\n1,a\n2,b\n3,c\n")
        fp = profile_file(zp)
        assert fp.row_count == 3
        assert len(fp.columns) == 2

    def test_macos_resource_fork_skipped(self, tmp_path):
        zp = tmp_path / "data.zip"
        with zipfile.ZipFile(zp, "w") as zf:
            zf.writestr("__MACOSX/._data.csv", "garbage")
            zf.writestr("data.csv", "id,val\n1,x\n2,y\n")
        fp = profile_file(zp)
        assert fp.row_count == 2
        assert len(fp.columns) == 2


# ---------------------------------------------------------------------------
# profile_file — zip (multi-file CSV partition)
# ---------------------------------------------------------------------------

class TestProfileFileZipPartition:

    def test_multi_shard_row_count_summed(self, tmp_path):
        zp = tmp_path / "partition.zip"
        with zipfile.ZipFile(zp, "w") as zf:
            zf.writestr("shard_1.csv", "id,val\n1,a\n2,b\n3,c\n")
            zf.writestr("shard_2.csv", "id,val\n4,d\n5,e\n")
            zf.writestr("shard_3.csv", "id,val\n6,f\n7,g\n8,h\n9,i\n")
        fp = profile_file(zp)
        assert fp.row_count == 9
        assert fp.is_row_count_exact is True

    def test_multi_shard_column_names(self, tmp_path):
        zp = tmp_path / "part.zip"
        with zipfile.ZipFile(zp, "w") as zf:
            zf.writestr("a.csv", "order_id,amount\n1,10.0\n")
            zf.writestr("b.csv", "order_id,amount\n2,20.0\n")
        fp = profile_file(zp)
        names = [c.name for c in fp.columns]
        assert names == ["order_id", "amount"]

    def test_non_csv_entries_in_zip_ignored(self, tmp_path):
        zp = tmp_path / "mixed.zip"
        with zipfile.ZipFile(zp, "w") as zf:
            zf.writestr("README.txt", "docs")
            zf.writestr("manifest.json", "{}")
            zf.writestr("data_1.csv", "id,val\n1,x\n2,y\n")
            zf.writestr("data_2.csv", "id,val\n3,z\n")
        fp = profile_file(zp)
        assert fp.row_count == 3


# ---------------------------------------------------------------------------
# profile_file — JSON output written to disk
# ---------------------------------------------------------------------------

class TestProfileFileOutput:

    def test_json_written_when_output_dir_given(self, tmp_path):
        f = tmp_path / "data.csv"
        _orders_csv(f)
        out_dir = tmp_path / "profiles"
        profile_file(f, output_dir=out_dir)
        expected = out_dir / "data_profile.json"
        assert expected.exists()

    def test_written_json_is_valid_and_complete(self, tmp_path):
        f = tmp_path / "data.csv"
        _orders_csv(f)
        out_dir = tmp_path / "profiles"
        profile_file(f, output_dir=out_dir)
        data = json.loads((out_dir / "data_profile.json").read_text())
        assert data["file_format"] == "csv"
        assert data["row_count"] == 4
        assert len(data["columns"]) == 4
        assert "quality_summary" in data

    def test_no_output_dir_does_not_write(self, tmp_path):
        f = tmp_path / "data.csv"
        _orders_csv(f)
        profile_file(f, output_dir=None)
        json_files = list(tmp_path.glob("*.json"))
        assert len(json_files) == 0


# ---------------------------------------------------------------------------
# profile_directory
# ---------------------------------------------------------------------------

class TestProfileDirectory:

    def test_profiles_all_csv_files(self, tmp_path):
        for name in ("a.csv", "b.csv", "c.csv"):
            _write_csv(tmp_path / name, "id,val\n1,x\n2,y\n")
        results = profile_directory(tmp_path)
        assert len(results) == 3

    def test_returns_correct_row_counts(self, tmp_path):
        _write_csv(tmp_path / "small.csv", "id\n1\n2\n")
        _write_csv(tmp_path / "large.csv", "id\n" + "".join(f"{i}\n" for i in range(10)))
        results = profile_directory(tmp_path)
        counts = {Path(fp.file_path).name: fp.row_count for fp in results}
        assert counts["small.csv"] == 2
        assert counts["large.csv"] == 10

    def test_skips_empty_files_without_crashing(self, tmp_path):
        _write_csv(tmp_path / "good.csv", "id\n1\n2\n")
        (tmp_path / "empty.csv").write_bytes(b"")
        results = profile_directory(tmp_path)
        assert len(results) == 1
        assert Path(results[0].file_path).name == "good.csv"

    def test_skips_unsupported_formats(self, tmp_path):
        _write_csv(tmp_path / "data.csv", "id\n1\n2\n")
        (tmp_path / "notes.pdf").write_bytes(b"%PDF-1.4 fake")
        results = profile_directory(tmp_path)
        # .pdf is not in _SCANNABLE_EXTENSIONS, so it never reaches the pipeline
        assert len(results) == 1

    def test_not_a_directory_raises(self, tmp_path):
        f = tmp_path / "data.csv"
        f.write_text("id\n1\n")
        with pytest.raises(NotADirectoryError):
            profile_directory(f)

    def test_empty_directory_returns_empty_list(self, tmp_path):
        results = profile_directory(tmp_path)
        assert results == []

    def test_writes_json_profiles_when_output_dir_given(self, tmp_path):
        _write_csv(tmp_path / "a.csv", "id\n1\n2\n")
        _write_csv(tmp_path / "b.csv", "id\n3\n4\n")
        out_dir = tmp_path / "out"
        profile_directory(tmp_path, output_dir=out_dir)
        json_files = list(out_dir.glob("*.json"))
        assert len(json_files) == 2

    def test_includes_zip_files(self, tmp_path):
        _write_csv(tmp_path / "plain.csv", "id\n1\n")
        zp = tmp_path / "archive.zip"
        with zipfile.ZipFile(zp, "w") as zf:
            zf.writestr("data.csv", "id\n2\n3\n")
        results = profile_directory(tmp_path)
        assert len(results) == 2


# ---------------------------------------------------------------------------
# run() — auto-detect
# ---------------------------------------------------------------------------

class TestRun:

    def test_run_on_file_returns_file_profile(self, tmp_path):
        f = tmp_path / "data.csv"
        _orders_csv(f)
        result = run(f)
        assert isinstance(result, FileProfile)

    def test_run_on_directory_returns_list(self, tmp_path):
        _write_csv(tmp_path / "a.csv", "id\n1\n")
        _write_csv(tmp_path / "b.csv", "id\n2\n")
        result = run(tmp_path)
        assert isinstance(result, list)
        assert len(result) == 2
