"""
Layer 5 — Parquet Profiling Engine

Steps:
  A — Read schema metadata (row count, declared types) — zero data rows at this stage.
  B — Flatten nested fields:
        Struct  → recursively expand leaf columns using underscore-joined paths
                  (e.g. user.address.city  →  user_address_city)
        List / Map / other complex types → kept as leaf nodes; values are
                  serialised to JSON strings so the column profiler sees plain text.
  C — Column-level profiling via pyarrow: exact null counts from Parquet row-group
      statistics when written; fall back to counting from sampled data otherwise.
  D — Large-file strategy: one top-level column at a time, row group by row group.
      Never SELECT * / read_table the full multi-GB file.

Library used: pyarrow only (DuckDB is not required).

Entry point:
  profile(path, strategy, intake=None) -> tuple[list[RawColumnData], int, bool]

Returns:
  (raw_columns, row_count, is_row_count_exact)
  Row count is always exact — Parquet metadata stores the true row count.
"""

from __future__ import annotations

import json as _json
import logging
import random
from collections import defaultdict
from dataclasses import dataclass, field as dc_field
from pathlib import Path
from typing import Any, Optional

import pyarrow as pa
import pyarrow.parquet as pq

from file_profiler.config import settings
from file_profiler.intake.validator import IntakeResult
from file_profiler.models.enums import SizeStrategy
from file_profiler.models.file_profile import RawColumnData

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Internal flat-column descriptor
# ---------------------------------------------------------------------------

@dataclass
class _FlatCol:
    """
    Describes one leaf column in the profile output, derived from the Parquet schema.

    For a plain file with no nesting, top_field == flat_name and nested_steps is [].
    For a nested struct, e.g. user.address.city:
        flat_name     = "user_address_city"
        top_field     = "user"
        nested_steps  = ["address", "city"]
        parquet_path  = "user.address.city"   (used to look up column stats)
    """
    flat_name:     str          # name used in the output ColumnProfile
    parquet_path:  str          # dot-path for Parquet column statistics lookup
    declared_type: str          # str(pa.DataType), e.g. "int64", "utf8", "list<item: int64>"
    is_stringified: bool        # True → List / Map / complex; values → JSON strings
    top_field:     str          # top-level Parquet column to read (column pruning key)
    nested_steps:  list[str]    # path within top_field to reach this leaf


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

@traceable(
    name="engine.parquet.profile",
    run_type="chain",
    process_outputs=compact_text_output,
)
def profile(
    path: str | Path,
    strategy: SizeStrategy,
    intake: Optional[IntakeResult] = None,
) -> tuple[list[RawColumnData], int, bool]:
    """
    Profile a Parquet file and return per-column raw data for the column profiler.

    Args:
        path:     Path to the Parquet file.
        strategy: Size strategy selected by Layer 3.
        intake:   IntakeResult from Layer 1 (not required for Parquet; kept for
                  signature consistency with the CSV engine).

    Returns:
        (raw_columns, row_count, is_row_count_exact)
        Row count is always exact for Parquet.

    Raises:
        pa.ArrowIOError — file cannot be opened or is not a valid Parquet file.
    """
    path = Path(path).resolve()

    # ── Step A: read schema metadata ─────────────────────────────────────────
    metadata = pq.read_metadata(str(path))
    schema   = pq.read_schema(str(path))
    total_rows = metadata.num_rows

    log.debug(
        "Parquet: %s — %d rows, %d top-level column(s)",
        path.name, total_rows, len(schema),
    )

    if total_rows == 0:
        log.warning("Parquet file %s has 0 rows — returning empty profile.", path.name)
        return [], 0, True

    # ── Step B: flatten schema ────────────────────────────────────────────────
    flat_cols = _flatten_schema(schema)

    if not flat_cols:
        log.warning("Parquet file %s produced no leaf columns.", path.name)
        return [], total_rows, True

    # ── DuckDB fast path for large flat Parquet files ─────────────────────────
    has_nesting = any(fc.nested_steps or fc.is_stringified for fc in flat_cols)
    if total_rows > settings.DUCKDB_ROW_THRESHOLD and not has_nesting:
        try:
            raw_columns = _profile_with_duckdb(path, metadata, flat_cols, total_rows)
            log.debug("Parquet: %s — profiled %d flat column(s) via DuckDB", path.name, len(raw_columns))
            return raw_columns, total_rows, True
        except Exception as exc:
            log.warning("DuckDB failed for %s: %s — falling back to pyarrow", path.name, exc)

    # ── Steps C/D: read and profile ───────────────────────────────────────────
    if strategy == SizeStrategy.MEMORY_SAFE:
        raw_columns = _profile_memory_safe(path, flat_cols, total_rows)
    else:
        raw_columns = _profile_row_groups(path, metadata, flat_cols, total_rows, strategy)

    log.debug(
        "Parquet: %s — profiled %d flat column(s)", path.name, len(raw_columns)
    )
    return raw_columns, total_rows, True


# ---------------------------------------------------------------------------
# Step B — Schema flattening
# ---------------------------------------------------------------------------

def _flatten_schema(schema: pa.Schema) -> list[_FlatCol]:
    """
    Walk the pyarrow schema and return one _FlatCol per leaf field.

    Struct fields are recursively expanded.
    List, Map, and other non-struct complex types are kept as single leaf
    nodes with is_stringified=True.
    """
    result: list[_FlatCol] = []
    for i in range(len(schema)):
        _walk_field(schema.field(i), prefix_parts=[], result=result)
    return result


def _walk_field(
    field: pa.Field,
    prefix_parts: list[str],
    result: list[_FlatCol],
) -> None:
    """Recursively expand a field into _FlatCol entries."""
    current_parts = prefix_parts + [field.name]
    dtype = field.type

    if pa.types.is_struct(dtype):
        # Recurse into each sub-field
        for i in range(dtype.num_fields):
            _walk_field(dtype.field(i), current_parts, result)
        return

    flat_name    = "_".join(current_parts)
    parquet_path = ".".join(current_parts)
    top_field    = current_parts[0]
    nested_steps = current_parts[1:]

    is_stringified = (
        pa.types.is_list(dtype)
        or pa.types.is_large_list(dtype)
        or pa.types.is_map(dtype)
        or pa.types.is_fixed_size_list(dtype)
    )

    result.append(_FlatCol(
        flat_name     = flat_name,
        parquet_path  = parquet_path,
        declared_type = str(dtype),
        is_stringified = is_stringified,
        top_field     = top_field,
        nested_steps  = nested_steps,
    ))


# ---------------------------------------------------------------------------
# DuckDB fast path (flat schemas only, > DUCKDB_ROW_THRESHOLD rows)
# ---------------------------------------------------------------------------

def _profile_with_duckdb(
    path: Path,
    metadata: pq.FileMetaData,
    flat_cols: list[_FlatCol],
    total_rows: int,
) -> list[RawColumnData]:
    """
    Profile a flat Parquet file via DuckDB for fast parallel reservoir sampling.

    Only used when the schema has no nested structs or complex types
    (checked by the caller).  Exact null counts are still sourced from
    Parquet row-group metadata (zero I/O).
    """
    from file_profiler.engines.duckdb_sampler import duckdb_sample_parquet

    headers, sampled_rows = duckdb_sample_parquet(path)

    if not sampled_rows:
        log.warning("DuckDB: no rows sampled from %s", path.name)
        return []

    # Exact null counts from Parquet metadata (zero I/O)
    null_from_meta: dict[str, Optional[int]] = {
        fc.flat_name: _null_count_from_metadata(metadata, fc.parquet_path)
        for fc in flat_cols
    }
    fc_by_name = {fc.flat_name: fc for fc in flat_cols}

    raw_cols: list[RawColumnData] = []
    for col_idx, name in enumerate(headers):
        values: list[Optional[str]] = []
        sample_nulls = 0
        for row in sampled_rows:
            v = row[col_idx] if col_idx < len(row) else ""
            if v == "":
                values.append(None)
                sample_nulls += 1
            else:
                values.append(v)

        # Prefer exact null count from metadata; fall back to sample count
        null_count = null_from_meta.get(name)
        if null_count is None:
            null_count = sample_nulls

        fc = fc_by_name.get(name)
        declared_type = fc.declared_type if fc else None

        raw_cols.append(RawColumnData(
            name=name,
            declared_type=declared_type,
            values=values,
            total_count=total_rows,
            null_count=null_count,
        ))

    log.info(
        "DuckDB profiled (Parquet): %s (%d rows, %d columns, %d sampled)",
        path.name, total_rows, len(headers), len(sampled_rows),
    )
    return raw_cols


# ---------------------------------------------------------------------------
# Step C — Profile strategy: MEMORY_SAFE
# ---------------------------------------------------------------------------

def _profile_memory_safe(
    path: Path,
    flat_cols: list[_FlatCol],
    total_rows: int,
) -> list[RawColumnData]:
    """
    Read the full Parquet file into memory once, then extract each flat column.
    Only called for files < MEMORY_SAFE_MAX_BYTES.
    """
    try:
        table = pq.read_table(str(path))
    except Exception as exc:
        log.error("Cannot read Parquet file %s: %s", path.name, exc)
        return []

    result: list[RawColumnData] = []
    for fc in flat_cols:
        try:
            col = table.column(fc.top_field)
            leaf = _navigate_nested(col, fc.nested_steps)
            pylist = _chunked_to_pylist(leaf)
            null_count = sum(1 for v in pylist if v is None)
            values = [_val_to_str(v, fc.is_stringified) for v in pylist]
            result.append(RawColumnData(
                name          = fc.flat_name,
                declared_type = fc.declared_type,
                values        = values,
                total_count   = total_rows,
                null_count    = null_count,
            ))
        except (KeyError, AttributeError, pa.ArrowInvalid, pa.ArrowNotImplementedError) as exc:
            log.warning("Skipping column %s in %s: %s", fc.flat_name, path.name, exc)

    return result


# ---------------------------------------------------------------------------
# Step D — Profile strategy: LAZY_SCAN / STREAM_ONLY (row group iteration)
# ---------------------------------------------------------------------------

def _profile_row_groups(
    path: Path,
    metadata: pq.FileMetaData,
    flat_cols: list[_FlatCol],
    total_rows: int,
    strategy: SizeStrategy,
) -> list[RawColumnData]:
    """
    Profile large Parquet files by reading ALL needed top-level columns
    in a single pass over row groups.

    Previously each top-level column triggered a separate full pass over
    all row groups.  Now a single pass reads all top-level columns per
    row group, avoiding repeated seeking.
    """
    k        = settings.SAMPLE_ROW_COUNT
    interval = settings.STREAM_SKIP_INTERVAL

    # Group flat columns by their top-level Parquet field (column pruning key)
    by_top: dict[str, list[_FlatCol]] = defaultdict(list)
    for fc in flat_cols:
        by_top[fc.top_field].append(fc)

    all_top_fields = sorted(by_top.keys())

    # Per-flat-col accumulators
    samples   = {fc.flat_name: [] for fc in flat_cols}
    null_scan = {fc.flat_name: 0  for fc in flat_cols}
    rng_map   = {fc.flat_name: random.Random(42) for fc in flat_cols}

    # Null counts from Parquet metadata (zero I/O)
    null_meta: dict[str, Optional[int]] = {
        fc.flat_name: _null_count_from_metadata(metadata, fc.parquet_path)
        for fc in flat_cols
    }
    needs_null_scan = any(v is None for v in null_meta.values())

    pf = pq.ParquetFile(str(path))
    global_i = 0

    for rg_idx in range(pf.metadata.num_row_groups):
        # Read ALL top-level columns in ONE row-group read
        try:
            chunk_table = pf.read_row_group(rg_idx, columns=all_top_fields)
        except Exception as exc:
            log.warning("Cannot read row group %d: %s", rg_idx, exc)
            continue

        chunk_len = chunk_table.num_rows

        # Process every flat column from this single row-group read
        for top_field, col_group in by_top.items():
            try:
                top_col = chunk_table.column(top_field)
            except KeyError:
                continue

            for fc in col_group:
                try:
                    leaf   = _navigate_nested(top_col, fc.nested_steps)
                    pylist = _chunked_to_pylist(leaf)
                except (AttributeError, pa.ArrowInvalid, pa.ArrowNotImplementedError) as exc:
                    log.debug("Cannot navigate %s in rg %d: %s", fc.flat_name, rg_idx, exc)
                    continue

                if needs_null_scan and null_meta[fc.flat_name] is None:
                    null_scan[fc.flat_name] += sum(1 for v in pylist if v is None)

                str_vals    = [_val_to_str(v, fc.is_stringified) for v in pylist]
                rng         = rng_map[fc.flat_name]
                col_samples = samples[fc.flat_name]

                if strategy == SizeStrategy.LAZY_SCAN:
                    for local_i, val in enumerate(str_vals):
                        gi = global_i + local_i
                        if gi < k:
                            col_samples.append(val)
                        else:
                            j = rng.randint(0, gi)
                            if j < k:
                                col_samples[j] = val
                else:
                    for local_i, val in enumerate(str_vals):
                        gi = global_i + local_i
                        if gi % interval == 0:
                            col_samples.append(val)

        global_i += chunk_len

    # Build results preserving original schema order
    results: list[RawColumnData] = []
    for fc in flat_cols:
        nc = null_meta[fc.flat_name] if null_meta[fc.flat_name] is not None else null_scan[fc.flat_name]
        s = samples[fc.flat_name]
        if s or nc is not None:
            results.append(RawColumnData(
                name          = fc.flat_name,
                declared_type = fc.declared_type,
                values        = s,
                total_count   = total_rows,
                null_count    = nc if nc is not None else 0,
            ))
    return results


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _navigate_nested(col: pa.ChunkedArray, nested_steps: list[str]) -> pa.ChunkedArray:
    """
    Navigate struct nesting inside a ChunkedArray.

    Each step indexes a named field inside a StructArray chunk.
    Returns a ChunkedArray of the leaf type.
    """
    if not nested_steps:
        return col

    result_chunks: list[pa.Array] = []
    for chunk in col.chunks:
        arr = chunk
        for step in nested_steps:
            # StructArray.field(name) → sub-Array (may itself be a StructArray)
            arr = arr.field(step)
        result_chunks.append(arr)

    return pa.chunked_array(result_chunks)


def _chunked_to_pylist(col: pa.ChunkedArray) -> list:
    """Convert a ChunkedArray to a Python list, combining all chunks."""
    return col.to_pylist()


def _val_to_str(value: Any, is_stringified: bool) -> Optional[str]:
    """
    Convert a Python value (from pyarrow.to_pylist()) to Optional[str].

    Null → None.
    Booleans → "true" / "false" (lowercase, consistent with type inference).
    Dates / timestamps → ISO 8601 string.
    Complex types (list, map) → JSON string when is_stringified is True.
    Everything else → str().
    """
    if value is None:
        return None

    if is_stringified:
        try:
            return _json.dumps(value, default=str)
        except Exception:
            return str(value)

    if isinstance(value, bool):
        return "true" if value else "false"

    if isinstance(value, bytes):
        return value.hex()

    # datetime.date, datetime.datetime, datetime.time, pd.Timestamp
    if hasattr(value, "isoformat"):
        return value.isoformat()

    return str(value)


def _null_count_from_metadata(
    metadata: pq.FileMetaData,
    parquet_path: str,
) -> Optional[int]:
    """
    Retrieve the total null count for a column from Parquet row-group statistics.

    Returns None if no row group has statistics for this column path.
    This is a zero-I/O operation — uses only the metadata footer.
    """
    total_null = 0
    found_any  = False

    for rg_idx in range(metadata.num_row_groups):
        rg = metadata.row_group(rg_idx)
        for col_idx in range(rg.num_columns):
            col_chunk = rg.column(col_idx)
            if col_chunk.path_in_schema == parquet_path:
                stats = col_chunk.statistics
                if stats is not None and stats.has_null_count:
                    total_null += stats.null_count
                    found_any   = True
                break   # found the column in this row group; move to next

    return total_null if found_any else None
