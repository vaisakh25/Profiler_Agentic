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
import os
import threading
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from contextlib import contextmanager
from pathlib import Path
from typing import Any, Callable, Optional, TypeVar

import duckdb

from file_profiler.config import settings
from file_profiler.config.env import DUCKDB_MEMORY_LIMIT, DUCKDB_THREADS

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# DuckDB operation timeout (seconds) — prevents indefinite hangs
# ---------------------------------------------------------------------------

DUCKDB_OPERATION_TIMEOUT = 60  # 60 seconds per DuckDB query


# ---------------------------------------------------------------------------
# Global connection semaphore — limits concurrent DuckDB connections across
# all threads/processes to prevent resource contention and file locking issues
# when profiling directories in parallel.
# ---------------------------------------------------------------------------

# Max concurrent DuckDB connections (conservative limit to prevent deadlock)
_MAX_CONCURRENT_DUCKDB_CONNECTIONS = 2
_duckdb_semaphore = threading.Semaphore(_MAX_CONCURRENT_DUCKDB_CONNECTIONS)


# ---------------------------------------------------------------------------
# Timeout wrapper for DuckDB operations
# ---------------------------------------------------------------------------

T = TypeVar('T')


class DuckDBTimeoutError(Exception):
    """Raised when a DuckDB operation exceeds the timeout limit."""
    pass


def _with_timeout(func: Callable[[], T], timeout: float, operation_name: str) -> T:
    """Execute a function with a timeout.
    
    Args:
        func: Callable to execute
        timeout: Maximum time in seconds
        operation_name: Description for logging
        
    Returns:
        Result from func()
        
    Raises:
        DuckDBTimeoutError: If operation exceeds timeout
    """
    with ThreadPoolExecutor(max_workers=1) as executor:
        future = executor.submit(func)
        try:
            return future.result(timeout=timeout)
        except FuturesTimeoutError:
            log.error(
                "DuckDB operation '%s' exceeded timeout of %ds — cancelling",
                operation_name, timeout
            )
            raise DuckDBTimeoutError(
                f"DuckDB {operation_name} exceeded {timeout}s timeout"
            )


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
    
    Acquires a global semaphore to limit concurrent DuckDB connections
    and prevent resource contention during parallel directory profiling.

    Example::

        with duckdb_connection() as con:
            count = duckdb_count(path, _con=con)
            headers, rows = duckdb_sample(path, _con=con)
    """
    # Acquire semaphore to limit concurrent connections
    _duckdb_semaphore.acquire()
    try:
        yield _get_thread_connection()
    finally:
        _duckdb_semaphore.release()


def duckdb_count(
    path: Path,
    delimiter: str = ",",
    encoding: str = "utf-8",
    has_header: bool = True,
    _con: Optional[duckdb.DuckDBPyConnection] = None,
    timeout: float = DUCKDB_OPERATION_TIMEOUT,
) -> int:
    """
    Fast parallel row count via DuckDB's read_csv.

    Returns the exact number of data rows (excludes header).
    Pass ``_con`` to reuse an existing connection (from ``duckdb_connection()``).
    
    Raises:
        DuckDBTimeoutError: If operation exceeds timeout
    """
    con = _con or _get_thread_connection()
    
    def _execute():
        result = con.execute(
            "SELECT COUNT(*) FROM read_csv($path, "
            "delim = $delim, header = $header, auto_detect = true, "
            "ignore_errors = true, parallel = true)",
            {"path": str(path), "delim": delimiter, "header": has_header},
        ).fetchone()
        return result[0] if result else 0
    
    count = _with_timeout(
        _execute,
        timeout=timeout,
        operation_name=f"count({path.name})"
    )
    log.debug("DuckDB count: %s -> %d rows", path.name, count)
    return count


def duckdb_sample(
    path: Path,
    delimiter: str = ",",
    encoding: str = "utf-8",
    has_header: bool = True,
    sample_size: int = settings.SAMPLE_ROW_COUNT,
    _con: Optional[duckdb.DuckDBPyConnection] = None,
    timeout: float = DUCKDB_OPERATION_TIMEOUT,
) -> tuple[list[str], list[list[str]]]:
    """
    Reservoir-sample rows from a large CSV via DuckDB.

    Returns:
        (headers, rows) where:
        - headers is a list of column name strings
        - rows is a list of rows, each row a list of string values (None -> "")

    Pass ``_con`` to reuse an existing connection (from ``duckdb_connection()``).
    
    Raises:
        DuckDBTimeoutError: If operation exceeds timeout
    """
    sample_size = int(sample_size)  # ensure integer for SQL literal
    con = _con or _get_thread_connection()
    
    def _execute():
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
        return headers, rows
    
    headers, rows = _with_timeout(
        _execute,
        timeout=timeout,
        operation_name=f"sample({path.name})"
    )

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
    timeout: float = DUCKDB_OPERATION_TIMEOUT,
) -> int:
    """Fast parallel row count for a Parquet file via DuckDB.
    
    Raises:
        DuckDBTimeoutError: If operation exceeds timeout
    """
    con = _con or _get_thread_connection()
    
    def _execute():
        result = con.execute(
            "SELECT COUNT(*) FROM read_parquet($path)",
            {"path": str(path)},
        ).fetchone()
        return result[0] if result else 0
    
    count = _with_timeout(
        _execute,
        timeout=timeout,
        operation_name=f"count_parquet({path.name})"
    )
    log.debug("DuckDB count (Parquet): %s -> %d rows", path.name, count)
    return count


def duckdb_sample_parquet(
    path: Path,
    sample_size: int = settings.SAMPLE_ROW_COUNT,
    _con: Optional[duckdb.DuckDBPyConnection] = None,
    timeout: float = DUCKDB_OPERATION_TIMEOUT,
) -> tuple[list[str], list[list[str]]]:
    """
    Reservoir-sample rows from a Parquet file via DuckDB.

    Returns (headers, rows) with all values converted to strings.
    
    Raises:
        DuckDBTimeoutError: If operation exceeds timeout
    """
    sample_size = int(sample_size)
    con = _con or _get_thread_connection()
    
    def _execute():
        rel = con.sql(
            f"SELECT * FROM read_parquet($path) "
            f"USING SAMPLE {sample_size} ROWS (reservoir, 42)",
            params={"path": str(path)},
        )
        headers = rel.columns
        rows: list[list[str]] = []
        for record in rel.fetchall():
            rows.append([_to_str(v) for v in record])
        return headers, rows
    
    headers, rows = _with_timeout(
        _execute,
        timeout=timeout,
        operation_name=f"sample_parquet({path.name})"
    )

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
    timeout: float = DUCKDB_OPERATION_TIMEOUT,
) -> int:
    """Fast row count for a JSON / NDJSON file via DuckDB.
    
    Raises:
        DuckDBTimeoutError: If operation exceeds timeout
    """
    con = _con or _get_thread_connection()
    
    def _execute():
        result = con.execute(
            "SELECT COUNT(*) FROM read_json_auto($path)",
            {"path": str(path)},
        ).fetchone()
        return result[0] if result else 0
    
    count = _with_timeout(
        _execute,
        timeout=timeout,
        operation_name=f"count_json({path.name})"
    )
    log.debug("DuckDB count (JSON): %s -> %d rows", path.name, count)
    return count


def duckdb_sample_json(
    path: Path,
    sample_size: int = settings.SAMPLE_ROW_COUNT,
    _con: Optional[duckdb.DuckDBPyConnection] = None,
    timeout: float = DUCKDB_OPERATION_TIMEOUT,
) -> tuple[list[str], list[list[str]]]:
    """
    Reservoir-sample rows from a JSON / NDJSON file via DuckDB.

    Returns (headers, rows) with all values converted to strings.
    
    Raises:
        DuckDBTimeoutError: If operation exceeds timeout
    """
    sample_size = int(sample_size)
    con = _con or _get_thread_connection()
    
    def _execute():
        rel = con.sql(
            f"SELECT * FROM read_json_auto($path) "
            f"USING SAMPLE {sample_size} ROWS (reservoir, 42)",
            params={"path": str(path)},
        )
        headers = rel.columns
        rows: list[list[str]] = []
        for record in rel.fetchall():
            rows.append([_to_str(v) for v in record])
        return headers, rows
    
    headers, rows = _with_timeout(
        _execute,
        timeout=timeout,
        operation_name=f"sample_json({path.name})"
    )

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
    """Create an in-memory DuckDB connection with configurable resource limits.
    
    Sets a per-process temp directory to avoid file lock conflicts when
    multiple workers profile files in parallel.
    """
    con = duckdb.connect(":memory:")
    con.execute(f"SET memory_limit = '{DUCKDB_MEMORY_LIMIT}'")
    con.execute(f"SET threads = {DUCKDB_THREADS}")
    
    # Set per-process temp directory to avoid file lock conflicts
    # during parallel profiling. Uses PID to ensure uniqueness.
    pid = os.getpid()
    temp_dir = f"/tmp/duckdb_worker_{pid}"
    try:
        con.execute(f"SET temp_directory = '{temp_dir}'")
        log.debug("DuckDB connection created (PID %d, temp_dir=%s)", pid, temp_dir)
    except Exception as exc:
        # Fallback if temp_directory setting fails (older DuckDB versions)
        log.debug("Could not set DuckDB temp_directory: %s", exc)
    
    return con
