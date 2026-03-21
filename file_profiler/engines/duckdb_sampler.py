"""
DuckDB-accelerated counting and sampling for large tables (> DUCKDB_ROW_THRESHOLD).

Provides parallel, vectorized counting and reservoir sampling for:
  - CSV  (plain + gzip)   via read_csv
  - Parquet               via read_parquet
  - JSON / NDJSON         via read_json_auto

Functions:
  duckdb_count(path, ...)            -> int          [CSV]
  duckdb_sample(path, ...)           -> (headers, rows)  [CSV]
  duckdb_count_parquet(path)         -> int          [Parquet]
  duckdb_sample_parquet(path, ...)   -> (headers, rows)  [Parquet]
  duckdb_count_json(path)            -> int          [JSON/NDJSON]
  duckdb_sample_json(path, ...)      -> (headers, rows)  [JSON/NDJSON]
"""

from __future__ import annotations

import atexit
import json as _json
import logging
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Optional

import duckdb

from file_profiler.config import settings
from file_profiler.config.env import DUCKDB_MEMORY_LIMIT, DUCKDB_THREADS

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Thread-local connection pool — one connection per thread, lazily created,
# closed at interpreter exit.  Eliminates repeated connection setup overhead
# when the same thread makes multiple DuckDB calls (the common case for
# single-file profiling and for each worker in the thread pool).
# ---------------------------------------------------------------------------

_thread_local = threading.local()
_all_connections: list[duckdb.DuckDBPyConnection] = []
_all_connections_lock = threading.Lock()


def _get_thread_connection() -> duckdb.DuckDBPyConnection:
    """Return the thread-local DuckDB connection, creating one if needed."""
    con = getattr(_thread_local, "duckdb_con", None)
    if con is None:
        con = _connect()
        _thread_local.duckdb_con = con
        with _all_connections_lock:
            _all_connections.append(con)
    return con


@atexit.register
def _cleanup_connections():
    """Close all thread-local connections at interpreter exit."""
    with _all_connections_lock:
        for con in _all_connections:
            try:
                con.close()
            except Exception:
                pass
        _all_connections.clear()


@contextmanager
def duckdb_connection():
    """Context manager that yields a reusable DuckDB connection.

    Uses a thread-local singleton so repeated calls on the same thread
    share a single connection with zero setup overhead.

    Example::

        with duckdb_connection() as con:
            count = duckdb_count(path, _con=con)
            headers, rows = duckdb_sample(path, _con=con)
    """
    yield _get_thread_connection()


def duckdb_count(
    path: Path,
    delimiter: str = ",",
    encoding: str = "utf-8",
    has_header: bool = True,
    _con: Optional[duckdb.DuckDBPyConnection] = None,
) -> int:
    """
    Fast parallel row count via DuckDB's read_csv.

    Returns the exact number of data rows (excludes header).
    Pass ``_con`` to reuse an existing connection (from ``duckdb_connection()``).
    """
    con = _con or _get_thread_connection()
    result = con.execute(
        "SELECT COUNT(*) FROM read_csv($path, "
        "delim = $delim, header = $header, auto_detect = true, "
        "ignore_errors = true, parallel = true)",
        {"path": str(path), "delim": delimiter, "header": has_header},
    ).fetchone()
    count = result[0] if result else 0
    log.debug("DuckDB count: %s -> %d rows", path.name, count)
    return count


def duckdb_sample(
    path: Path,
    delimiter: str = ",",
    encoding: str = "utf-8",
    has_header: bool = True,
    sample_size: int = settings.SAMPLE_ROW_COUNT,
    _con: Optional[duckdb.DuckDBPyConnection] = None,
) -> tuple[list[str], list[list[str]]]:
    """
    Reservoir-sample rows from a large CSV via DuckDB.

    Returns:
        (headers, rows) where:
        - headers is a list of column name strings
        - rows is a list of rows, each row a list of string values (None -> "")

    Pass ``_con`` to reuse an existing connection (from ``duckdb_connection()``).
    """
    sample_size = int(sample_size)  # ensure integer for SQL literal
    con = _con or _get_thread_connection()
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


# ---------------------------------------------------------------------------
# Parquet
# ---------------------------------------------------------------------------

def duckdb_count_parquet(
    path: Path,
    _con: Optional[duckdb.DuckDBPyConnection] = None,
) -> int:
    """Fast parallel row count for a Parquet file via DuckDB."""
    con = _con or _get_thread_connection()
    result = con.execute(
        "SELECT COUNT(*) FROM read_parquet($path)",
        {"path": str(path)},
    ).fetchone()
    count = result[0] if result else 0
    log.debug("DuckDB count (Parquet): %s -> %d rows", path.name, count)
    return count


def duckdb_sample_parquet(
    path: Path,
    sample_size: int = settings.SAMPLE_ROW_COUNT,
    _con: Optional[duckdb.DuckDBPyConnection] = None,
) -> tuple[list[str], list[list[str]]]:
    """
    Reservoir-sample rows from a Parquet file via DuckDB.

    Returns (headers, rows) with all values converted to strings.
    """
    sample_size = int(sample_size)
    con = _con or _get_thread_connection()
    rel = con.sql(
        f"SELECT * FROM read_parquet($path) "
        f"USING SAMPLE {sample_size} ROWS (reservoir, 42)",
        params={"path": str(path)},
    )
    headers = rel.columns
    rows: list[list[str]] = []
    for record in rel.fetchall():
        rows.append([_to_str(v) for v in record])

    log.debug(
        "DuckDB sample (Parquet): %s -> %d columns, %d rows sampled",
        path.name, len(headers), len(rows),
    )
    return headers, rows


# ---------------------------------------------------------------------------
# JSON / NDJSON
# ---------------------------------------------------------------------------

def duckdb_count_json(
    path: Path,
    _con: Optional[duckdb.DuckDBPyConnection] = None,
) -> int:
    """Fast row count for a JSON / NDJSON file via DuckDB."""
    con = _con or _get_thread_connection()
    result = con.execute(
        "SELECT COUNT(*) FROM read_json_auto($path)",
        {"path": str(path)},
    ).fetchone()
    count = result[0] if result else 0
    log.debug("DuckDB count (JSON): %s -> %d rows", path.name, count)
    return count


def duckdb_sample_json(
    path: Path,
    sample_size: int = settings.SAMPLE_ROW_COUNT,
    _con: Optional[duckdb.DuckDBPyConnection] = None,
) -> tuple[list[str], list[list[str]]]:
    """
    Reservoir-sample rows from a JSON / NDJSON file via DuckDB.

    Returns (headers, rows) with all values converted to strings.
    """
    sample_size = int(sample_size)
    con = _con or _get_thread_connection()
    rel = con.sql(
        f"SELECT * FROM read_json_auto($path) "
        f"USING SAMPLE {sample_size} ROWS (reservoir, 42)",
        params={"path": str(path)},
    )
    headers = rel.columns
    rows: list[list[str]] = []
    for record in rel.fetchall():
        rows.append([_to_str(v) for v in record])

    log.debug(
        "DuckDB sample (JSON): %s -> %d columns, %d rows sampled",
        path.name, len(headers), len(rows),
    )
    return headers, rows


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _to_str(v: Any) -> str:
    """Convert a DuckDB result value to a string for RawColumnData.

    Mirrors the value-to-string logic used by the Parquet and JSON engines
    so that downstream column profiling sees consistent representations.
    """
    if v is None:
        return ""
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, (dict, list)):
        try:
            return _json.dumps(v, default=str)
        except (TypeError, ValueError):
            return str(v)
    if isinstance(v, bytes):
        return v.hex()
    if hasattr(v, "isoformat"):
        return v.isoformat()
    return str(v)


def _connect() -> duckdb.DuckDBPyConnection:
    """Create an in-memory DuckDB connection with configurable resource limits."""
    con = duckdb.connect(":memory:")
    con.execute(f"SET memory_limit = '{DUCKDB_MEMORY_LIMIT}'")
    con.execute(f"SET threads = {DUCKDB_THREADS}")
    return con
