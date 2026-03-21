"""
Database engine — profiles tables inside DuckDB (.duckdb) and SQLite (.db) files.

Unlike file-based engines (CSV, Parquet, JSON, Excel), a single database file
contains multiple tables.  This engine enumerates all user tables, then for
each table: counts rows, reservoir-samples data, and produces RawColumnData
suitable for the standard column profiler (Layers 6-8).

Functions:
  list_tables(path, fmt)             -> list[str]
  profile(path, fmt, table_filter)   -> list[TableResult]

Each TableResult contains (table_name, raw_columns, row_count, is_exact)
matching the signature expected by main.py.
"""

from __future__ import annotations

import json as _json
import logging
import random
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import duckdb

from file_profiler.config import settings
from file_profiler.config.env import DUCKDB_MEMORY_LIMIT, DUCKDB_THREADS
from file_profiler.models.enums import FileFormat
from file_profiler.models.file_profile import RawColumnData

log = logging.getLogger(__name__)


@dataclass
class TableResult:
    """Profiling output for a single table inside a database file."""
    table_name: str
    raw_columns: list[RawColumnData]
    row_count: int
    is_row_count_exact: bool


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def list_tables(path: Path, fmt: FileFormat) -> list[str]:
    """Return the names of all user tables in the database file."""
    if fmt == FileFormat.DUCKDB:
        return _duckdb_list_tables(path)
    elif fmt == FileFormat.SQLITE:
        return _sqlite_list_tables(path)
    raise ValueError(f"db_engine.list_tables: unsupported format {fmt}")


def profile(
    path: Path,
    fmt: FileFormat,
    table_filter: Optional[list[str]] = None,
) -> list[TableResult]:
    """
    Profile all (or selected) tables in a database file.

    Args:
        path:         Path to the .duckdb or .db file.
        fmt:          FileFormat.DUCKDB or FileFormat.SQLITE.
        table_filter: If provided, only profile these table names.

    Returns:
        List of TableResult, one per table.
    """
    tables = list_tables(path, fmt)
    if table_filter:
        filter_set = set(table_filter)
        tables = [t for t in tables if t in filter_set]

    if not tables:
        log.warning("No tables found in %s", path.name)
        return []

    log.info("Found %d table(s) in %s: %s", len(tables), path.name, tables)

    if fmt == FileFormat.DUCKDB:
        return _duckdb_profile_tables(path, tables)
    elif fmt == FileFormat.SQLITE:
        return _sqlite_profile_tables(path, tables)
    raise ValueError(f"db_engine.profile: unsupported format {fmt}")


# ---------------------------------------------------------------------------
# DuckDB implementation
# ---------------------------------------------------------------------------

def _duckdb_list_tables(path: Path) -> list[str]:
    """List user tables in a DuckDB file across all user schemas.

    Returns schema-qualified names (schema.table) when tables exist outside
    the 'main' schema.  System schemas (information_schema, pg_catalog) are
    excluded.
    """
    con = _duckdb_connect(path)
    try:
        rows = con.execute(
            "SELECT table_schema, table_name FROM information_schema.tables "
            "WHERE table_type = 'BASE TABLE' "
            "AND table_schema NOT IN ('information_schema', 'pg_catalog') "
            "ORDER BY table_schema, table_name"
        ).fetchall()

        # Check if all tables are in 'main' — if so, use unqualified names
        schemas = {r[0] for r in rows}
        if schemas == {"main"}:
            return [r[1] for r in rows]

        # Multiple schemas — use schema-qualified names
        return [f"{r[0]}.{r[1]}" for r in rows]
    finally:
        con.close()


def _duckdb_profile_tables(path: Path, tables: list[str]) -> list[TableResult]:
    """Profile each table in a DuckDB file."""
    con = _duckdb_connect(path)
    results: list[TableResult] = []
    try:
        for table in tables:
            try:
                result = _duckdb_profile_one(con, table)
                results.append(result)
            except Exception as exc:
                log.error("Failed to profile DuckDB table '%s': %s", table, exc)
    finally:
        con.close()
    return results


def _duckdb_profile_one(con: duckdb.DuckDBPyConnection, table: str) -> TableResult:
    """Profile a single table via DuckDB.

    Table name may be schema-qualified (e.g. 'core.patient') or plain.
    """
    # Parse schema-qualified name
    if "." in table:
        schema, tname = table.split(".", 1)
    else:
        schema, tname = "main", table

    # Build SQL-safe qualified reference
    qualified = f'"{schema}"."{tname}"'

    # Column metadata from information_schema
    col_meta = con.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_schema = $1 AND table_name = $2 "
        "ORDER BY ordinal_position",
        [schema, tname],
    ).fetchall()

    col_names = [cm[0] for cm in col_meta]
    col_types = {cm[0]: cm[1] for cm in col_meta}

    # Combined COUNT(*) + null counts in a SINGLE query (was 2 separate queries)
    null_counts: dict[str, int] = {}
    if col_names:
        null_exprs = ", ".join(
            f'COUNT(*) FILTER (WHERE "{cname}" IS NULL)' for cname in col_names
        )
        combined_row = con.execute(
            f"SELECT COUNT(*), {null_exprs} FROM {qualified}"
        ).fetchone()
        row_count = combined_row[0] if combined_row else 0
        if combined_row:
            for i, cname in enumerate(col_names):
                null_counts[cname] = combined_row[i + 1] or 0
    else:
        count_result = con.execute(
            f"SELECT COUNT(*) FROM {qualified}"
        ).fetchone()
        row_count = count_result[0] if count_result else 0

    # Skip sampling for empty tables
    if row_count == 0:
        return TableResult(
            table_name=table,
            raw_columns=[
                RawColumnData(
                    name=cname,
                    declared_type=col_types.get(cname),
                    values=[],
                    total_count=0,
                    null_count=0,
                )
                for cname in col_names
            ],
            row_count=0,
            is_row_count_exact=True,
        )

    sample_size = min(row_count, settings.SAMPLE_ROW_COUNT)
    if row_count > settings.SAMPLE_ROW_COUNT:
        rel = con.sql(
            f"SELECT * FROM {qualified} "
            f"USING SAMPLE {sample_size} ROWS (reservoir, 42)"
        )
    else:
        rel = con.sql(f"SELECT * FROM {qualified}")

    sampled_rows = rel.fetchall()
    sampled_headers = rel.columns

    # Pivot: rows → columns
    raw_columns: list[RawColumnData] = []
    for i, cname in enumerate(sampled_headers):
        values = [_to_str(row[i]) for row in sampled_rows]
        raw_columns.append(RawColumnData(
            name=cname,
            declared_type=col_types.get(cname),
            values=values,
            total_count=row_count,
            null_count=null_counts.get(cname, 0),
        ))

    log.debug("DuckDB table '%s': %d rows, %d columns", table, row_count, len(raw_columns))
    return TableResult(
        table_name=table,
        raw_columns=raw_columns,
        row_count=row_count,
        is_row_count_exact=True,
    )


def _duckdb_connect(path: Path) -> duckdb.DuckDBPyConnection:
    """Open a read-only DuckDB connection to a database file."""
    con = duckdb.connect(str(path), read_only=True)
    con.execute(f"SET memory_limit = '{DUCKDB_MEMORY_LIMIT}'")
    con.execute(f"SET threads = {DUCKDB_THREADS}")
    return con


# ---------------------------------------------------------------------------
# SQLite implementation
# ---------------------------------------------------------------------------

def _sqlite_list_tables(path: Path) -> list[str]:
    """List user tables in a SQLite file."""
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    try:
        cur = con.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type = 'table' AND name NOT LIKE 'sqlite_%' "
            "ORDER BY name"
        )
        return [r[0] for r in cur.fetchall()]
    finally:
        con.close()


def _sqlite_profile_tables(path: Path, tables: list[str]) -> list[TableResult]:
    """Profile each table in a SQLite file."""
    con = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    results: list[TableResult] = []
    try:
        for table in tables:
            try:
                result = _sqlite_profile_one(con, table)
                results.append(result)
            except Exception as exc:
                log.error("Failed to profile SQLite table '%s': %s", table, exc)
    finally:
        con.close()
    return results


def _sqlite_profile_one(con: sqlite3.Connection, table: str) -> TableResult:
    """Profile a single table via SQLite."""
    # Column metadata via PRAGMA
    cur = con.execute(f'PRAGMA table_info("{table}")')
    pragma_rows = cur.fetchall()
    # pragma_rows: (cid, name, type, notnull, dflt_value, pk)
    col_names = [r[1] for r in pragma_rows]
    col_types = {r[1]: r[2] for r in pragma_rows}

    # Combined COUNT(*) + null counts in a SINGLE query (was 2 separate queries)
    null_counts: dict[str, int] = {}
    if col_names:
        null_exprs = ", ".join(
            f'SUM(CASE WHEN "{cname}" IS NULL THEN 1 ELSE 0 END)'
            for cname in col_names
        )
        cur = con.execute(f'SELECT COUNT(*), {null_exprs} FROM "{table}"')
        combined_row = cur.fetchone()
        row_count = combined_row[0] if combined_row else 0
        if combined_row:
            for i, cname in enumerate(col_names):
                null_counts[cname] = combined_row[i + 1] or 0
    else:
        cur = con.execute(f'SELECT COUNT(*) FROM "{table}"')
        row_count = cur.fetchone()[0]

    # Sample rows using reservoir sampling (O(n)) instead of
    # ORDER BY RANDOM() LIMIT k (O(n log n) due to full sort).
    sample_size = min(row_count, settings.SAMPLE_ROW_COUNT)
    if row_count > settings.SAMPLE_ROW_COUNT:
        cur = con.execute(f'SELECT * FROM "{table}"')
        sampled_col_names = [desc[0] for desc in cur.description] if cur.description else col_names
        sampled_rows = _reservoir_sample_cursor(cur, sample_size)
    else:
        cur = con.execute(f'SELECT * FROM "{table}"')
        sampled_rows = cur.fetchall()
        sampled_col_names = [desc[0] for desc in cur.description] if cur.description else col_names

    # Pivot: rows → columns
    raw_columns: list[RawColumnData] = []
    for i, cname in enumerate(sampled_col_names):
        values = [_to_str(row[i]) for row in sampled_rows]
        raw_columns.append(RawColumnData(
            name=cname,
            declared_type=col_types.get(cname),
            values=values,
            total_count=row_count,
            null_count=null_counts.get(cname, 0),
        ))

    log.debug("SQLite table '%s': %d rows, %d columns", table, row_count, len(raw_columns))
    return TableResult(
        table_name=table,
        raw_columns=raw_columns,
        row_count=row_count,
        is_row_count_exact=True,
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _reservoir_sample_cursor(cursor, k: int) -> list[tuple]:
    """Vitter's Algorithm R on a database cursor — O(n) with no sorting."""
    rng = random.Random(42)
    sample: list[tuple] = []
    for i, row in enumerate(cursor):
        if i < k:
            sample.append(row)
        else:
            j = rng.randint(0, i)
            if j < k:
                sample[j] = row
    return sample


def _to_str(v: Any) -> str:
    """Convert a database value to a string for RawColumnData."""
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
