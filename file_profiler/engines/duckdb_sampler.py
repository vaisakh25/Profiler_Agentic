"""
DuckDB-accelerated sampling for STREAM_ONLY (>2 GB) CSV files.

Replaces the Python-based skip-interval sampling with DuckDB's parallel,
vectorized CSV reader and reservoir sampling.  Called only when
strategy == SizeStrategy.STREAM_ONLY.

Functions:
  duckdb_count(path, delimiter, encoding)  -> int
  duckdb_sample(path, delimiter, encoding, sample_size) -> list[list[str]]
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

import duckdb

from file_profiler.config import settings
from file_profiler.config.env import DUCKDB_MEMORY_LIMIT, DUCKDB_THREADS

log = logging.getLogger(__name__)


def duckdb_count(
    path: Path,
    delimiter: str = ",",
    encoding: str = "utf-8",
    has_header: bool = True,
) -> int:
    """
    Fast parallel row count via DuckDB's read_csv.

    Returns the exact number of data rows (excludes header).
    """
    con = _connect()
    try:
        result = con.execute(
            "SELECT COUNT(*) FROM read_csv($path, "
            "delim = $delim, header = $header, auto_detect = true, "
            "ignore_errors = true, parallel = true)",
            {"path": str(path), "delim": delimiter, "header": has_header},
        ).fetchone()
        count = result[0] if result else 0
        log.debug("DuckDB count: %s -> %d rows", path.name, count)
        return count
    finally:
        con.close()


def duckdb_sample(
    path: Path,
    delimiter: str = ",",
    encoding: str = "utf-8",
    has_header: bool = True,
    sample_size: int = settings.SAMPLE_ROW_COUNT,
) -> tuple[list[str], list[list[str]]]:
    """
    Reservoir-sample rows from a large CSV via DuckDB.

    Returns:
        (headers, rows) where:
        - headers is a list of column name strings
        - rows is a list of rows, each row a list of string values (None -> "")
    """
    sample_size = int(sample_size)  # ensure integer for SQL literal
    con = _connect()
    try:
        # USING SAMPLE requires literal constants — build the sample clause
        # directly. sample_size is from settings (not user input).
        rel = con.sql(
            f"SELECT * FROM read_csv($path, "
            f"delim = $delim, header = $header, auto_detect = true, "
            f"ignore_errors = true, parallel = true, all_varchar = true) "
            f"USING SAMPLE {sample_size} ROWS (reservoir, 42)",
            params={
                "path": str(path),
                "delim": delimiter,
                "header": has_header,
            },
        )

        headers = rel.columns
        rows: list[list[str]] = []
        for record in rel.fetchall():
            rows.append([str(v) if v is not None else "" for v in record])

        log.debug(
            "DuckDB sample: %s -> %d columns, %d rows sampled",
            path.name, len(headers), len(rows),
        )
        return headers, rows
    finally:
        con.close()


def _connect() -> duckdb.DuckDBPyConnection:
    """Create an in-memory DuckDB connection with configurable resource limits."""
    con = duckdb.connect(":memory:")
    con.execute(f"SET memory_limit = '{DUCKDB_MEMORY_LIMIT}'")
    con.execute(f"SET threads = {DUCKDB_THREADS}")
    return con
