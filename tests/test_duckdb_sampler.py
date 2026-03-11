"""Smoke tests for the DuckDB sampler used by STREAM_ONLY CSV profiling."""

import csv
import tempfile
from pathlib import Path

import pytest

from file_profiler.engines.duckdb_sampler import duckdb_count, duckdb_sample


@pytest.fixture
def large_csv(tmp_path: Path) -> Path:
    """Generate a 20k-row CSV to test DuckDB sampling."""
    fpath = tmp_path / "test_large.csv"
    with open(fpath, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["id", "name", "value", "city"])
        for i in range(20_000):
            writer.writerow([i, f"name_{i}", round(i * 1.5, 2), f"city_{i % 50}"])
    return fpath


def test_duckdb_count(large_csv: Path):
    count = duckdb_count(large_csv)
    assert count == 20_000


def test_duckdb_sample_size(large_csv: Path):
    headers, rows = duckdb_sample(large_csv, sample_size=500)
    assert headers == ["id", "name", "value", "city"]
    assert len(rows) == 500


def test_duckdb_sample_values_are_strings(large_csv: Path):
    _, rows = duckdb_sample(large_csv, sample_size=10)
    for row in rows:
        for val in row:
            assert isinstance(val, str)


def test_duckdb_sample_deterministic(large_csv: Path):
    _, rows1 = duckdb_sample(large_csv, sample_size=100)
    _, rows2 = duckdb_sample(large_csv, sample_size=100)
    assert rows1 == rows2


def test_duckdb_integration_with_build_raw_columns(large_csv: Path):
    """Verify DuckDB output feeds cleanly into _build_raw_columns."""
    from file_profiler.engines.csv_engine import _build_raw_columns

    headers, rows = duckdb_sample(large_csv, sample_size=100)
    count = duckdb_count(large_csv)
    raw_cols = _build_raw_columns(headers, rows, count)

    assert len(raw_cols) == 4
    assert raw_cols[0].name == "id"
    assert raw_cols[0].total_count == 20_000
    assert len(raw_cols[0].values) == 100
