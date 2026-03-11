"""
Tests for file_profiler/output/profile_writer.py
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from file_profiler.models.enums import (
    Cardinality,
    FileFormat,
    InferredType,
    QualityFlag,
    SizeStrategy,
)
from file_profiler.models.file_profile import (
    ColumnProfile,
    FileProfile,
    QualitySummary,
    TopValue,
)
from file_profiler.output.profile_writer import write


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _minimal_profile(tmp_path: Path, *, n_cols: int = 2) -> FileProfile:
    cols = [
        ColumnProfile(
            name               = f"col_{i}",
            declared_type      = None,
            inferred_type      = InferredType.STRING,
            confidence_score   = 1.0,
            null_count         = 0,
            distinct_count     = 10,
        )
        for i in range(n_cols)
    ]
    return FileProfile(
        file_format        = FileFormat.CSV,
        file_path          = str(tmp_path / "data.csv"),
        table_name         = "data",
        row_count          = 100,
        is_row_count_exact = True,
        encoding           = "utf-8",
        size_bytes         = 4096,
        size_strategy      = SizeStrategy.MEMORY_SAFE,
        columns            = cols,
    )


# ---------------------------------------------------------------------------
# Basic write
# ---------------------------------------------------------------------------

class TestWrite:

    def test_creates_json_file(self, tmp_path):
        profile = _minimal_profile(tmp_path)
        out = tmp_path / "profile.json"
        write(profile, out)
        assert out.exists()

    def test_output_is_valid_json(self, tmp_path):
        profile = _minimal_profile(tmp_path)
        out = tmp_path / "profile.json"
        write(profile, out)
        data = json.loads(out.read_text(encoding="utf-8"))
        assert isinstance(data, dict)

    def test_creates_parent_directories(self, tmp_path):
        profile = _minimal_profile(tmp_path)
        out = tmp_path / "nested" / "deep" / "profile.json"
        write(profile, out)
        assert out.exists()

    def test_overwrites_existing_file(self, tmp_path):
        profile = _minimal_profile(tmp_path)
        out = tmp_path / "profile.json"
        out.write_text("old content")
        write(profile, out)
        data = json.loads(out.read_text())
        assert "columns" in data   # valid profile, not "old content"


# ---------------------------------------------------------------------------
# JSON schema structure
# ---------------------------------------------------------------------------

class TestSchema:

    def _load(self, tmp_path: Path, profile: FileProfile = None) -> dict:
        profile = profile or _minimal_profile(tmp_path)
        out = tmp_path / "p.json"
        write(profile, out)
        return json.loads(out.read_text(encoding="utf-8"))

    def test_top_level_keys_present(self, tmp_path):
        data = self._load(tmp_path)
        for key in (
            "source_type", "file_format", "file_path", "table_name",
            "row_count", "is_row_count_exact", "encoding", "size_strategy",
            "columns", "structural_issues", "quality_summary",
        ):
            assert key in data, f"Missing top-level key: {key}"

    def test_source_type_is_file(self, tmp_path):
        data = self._load(tmp_path)
        assert data["source_type"] == "file"

    def test_file_format_serialised_as_string(self, tmp_path):
        data = self._load(tmp_path)
        assert data["file_format"] == "csv"     # enum .value, not "FileFormat.CSV"

    def test_size_strategy_serialised_as_string(self, tmp_path):
        data = self._load(tmp_path)
        assert data["size_strategy"] == "MEMORY_SAFE"

    def test_columns_is_list(self, tmp_path):
        data = self._load(tmp_path)
        assert isinstance(data["columns"], list)
        assert len(data["columns"]) == 2

    def test_column_keys_present(self, tmp_path):
        data = self._load(tmp_path)
        col = data["columns"][0]
        for key in (
            "name", "declared_type", "inferred_type", "confidence_score",
            "null_count", "distinct_count", "quality_flags", "sample_values",
        ):
            assert key in col, f"Missing column key: {key}"

    def test_inferred_type_serialised_as_string(self, tmp_path):
        data = self._load(tmp_path)
        assert data["columns"][0]["inferred_type"] == "STRING"

    def test_quality_flags_are_strings(self, tmp_path):
        col = ColumnProfile(
            name="x", declared_type=None,
            inferred_type=InferredType.STRING, confidence_score=1.0,
            null_count=90, distinct_count=0,
            quality_flags=[QualityFlag.HIGH_NULL_RATIO, QualityFlag.FULLY_NULL],
        )
        profile = _minimal_profile(tmp_path, n_cols=0)
        profile.columns = [col]
        out = tmp_path / "p.json"
        write(profile, out)
        data = json.loads(out.read_text())
        flags = data["columns"][0]["quality_flags"]
        assert isinstance(flags, list)
        assert all(isinstance(f, str) for f in flags)
        assert "HIGH_NULL_RATIO" in flags
        assert "FULLY_NULL" in flags

    def test_top_values_serialised(self, tmp_path):
        col = ColumnProfile(
            name="status", declared_type=None,
            inferred_type=InferredType.CATEGORICAL, confidence_score=1.0,
            null_count=0, distinct_count=2,
            top_values=[TopValue("active", 80), TopValue("inactive", 20)],
        )
        profile = _minimal_profile(tmp_path, n_cols=0)
        profile.columns = [col]
        out = tmp_path / "p.json"
        write(profile, out)
        data = json.loads(out.read_text())
        tv = data["columns"][0]["top_values"]
        assert tv[0] == {"value": "active", "count": 80}
        assert tv[1] == {"value": "inactive", "count": 20}

    def test_none_fields_preserved_as_null(self, tmp_path):
        data = self._load(tmp_path)
        col = data["columns"][0]
        assert col["declared_type"] is None
        assert col["min"] is None
        assert col["max"] is None


# ---------------------------------------------------------------------------
# Quality summary
# ---------------------------------------------------------------------------

class TestQualitySummary:

    def test_columns_profiled_count(self, tmp_path):
        profile = _minimal_profile(tmp_path, n_cols=5)
        out = tmp_path / "p.json"
        write(profile, out)
        data = json.loads(out.read_text())
        assert data["quality_summary"]["columns_profiled"] == 5

    def test_columns_with_issues_counted(self, tmp_path):
        col_clean = ColumnProfile(
            name="id", declared_type=None,
            inferred_type=InferredType.INTEGER, confidence_score=1.0,
            null_count=0, distinct_count=100,
        )
        col_issue = ColumnProfile(
            name="opt", declared_type=None,
            inferred_type=InferredType.STRING, confidence_score=1.0,
            null_count=80, distinct_count=2,
            quality_flags=[QualityFlag.HIGH_NULL_RATIO],
        )
        profile = _minimal_profile(tmp_path, n_cols=0)
        profile.columns = [col_clean, col_issue]
        out = tmp_path / "p.json"
        write(profile, out)
        data = json.loads(out.read_text())
        qs = data["quality_summary"]
        assert qs["columns_profiled"] == 2
        assert qs["columns_with_issues"] == 1
        assert qs["null_heavy_columns"] == 1

    def test_type_conflict_counted(self, tmp_path):
        col = ColumnProfile(
            name="data", declared_type=None,
            inferred_type=InferredType.STRING, confidence_score=0.5,
            null_count=0, distinct_count=10,
            quality_flags=[QualityFlag.TYPE_CONFLICT],
        )
        profile = _minimal_profile(tmp_path, n_cols=0)
        profile.columns = [col]
        out = tmp_path / "p.json"
        write(profile, out)
        data = json.loads(out.read_text())
        assert data["quality_summary"]["type_conflict_columns"] == 1

    def test_no_issues_all_zeros(self, tmp_path):
        profile = _minimal_profile(tmp_path, n_cols=3)
        out = tmp_path / "p.json"
        write(profile, out)
        data = json.loads(out.read_text())
        qs = data["quality_summary"]
        assert qs["columns_with_issues"] == 0
        assert qs["null_heavy_columns"] == 0
        assert qs["type_conflict_columns"] == 0
        assert qs["corrupt_rows_detected"] == 0
