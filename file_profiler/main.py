"""
Orchestrator — runs the full file profiling pipeline.

Pipeline (per file):
  Layer 1  intake/validator.py       validate(path)  -> IntakeResult
  Layer 2  classification/classifier.py  classify(intake) -> FileFormat
  Layer 3  strategy/size_strategy.py     select(intake)   -> SizeStrategy
  Layer 4  engines/csv_engine.py         profile(...)     -> raw columns  [CSV]
  Layer 5  engines/parquet_engine.py    profile(...)     -> raw columns  [Parquet]
  Layer 6.5 standardization/normalizer.py standardize(raw) -> cleaned columns
  Layer 7  profiling/column_profiler.py  profile(raw)     -> ColumnProfile  ×N
  Layer 8  quality/structural_checker.py check(...)       -> flagged profiles
  Layer 11 output/profile_writer.py      write(...)       -> JSON on disk

Entry points
------------
run(path, output_dir)
    Auto-detects file vs directory.  Returns FileProfile or list[FileProfile].

profile_file(path, output_dir)
    Profile a single file.  Raises on intake/format errors.

profile_directory(dir_path, output_dir)
    Profile every supported file in a directory (non-recursive).
    Logs and skips files that fail intake or have unsupported formats.

analyze_relationships(profiles, output_path)
    Detect FK relationships across a set of already-profiled tables.
    Returns RelationshipReport; optionally writes relationships.json.
"""

from __future__ import annotations

import logging
import os
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from pathlib import Path

from file_profiler.analysis.relationship_detector import detect as _detect_relationships
from file_profiler.classification.classifier import classify
from file_profiler.config import settings
from file_profiler.engines import csv_engine, db_engine, excel_engine, json_engine, parquet_engine
from file_profiler.intake.errors import CorruptFileError, EmptyFileError
from file_profiler.intake.validator import validate
from file_profiler.models.enums import FileFormat, QualityFlag, SizeStrategy
from file_profiler.models.file_profile import FileProfile
from file_profiler.models.relationships import RelationshipReport
from file_profiler.output.profile_writer import write
from file_profiler.output.relationship_writer import write as _write_relationships
from file_profiler.profiling.column_profiler import profile as profile_column
from file_profiler.quality.structural_checker import check as structural_check
from file_profiler.standardization.normalizer import standardize
from file_profiler.strategy.size_strategy import select

log = logging.getLogger(__name__)

# Extensions that profile_directory will attempt to profile.
# .gz and .zip are included because they may wrap CSV files.
_SCANNABLE_EXTENSIONS = frozenset({
    ".csv", ".tsv", ".dat", ".psv",    # plain-text CSV variants
    ".gz",                              # gzip-compressed (assumed CSV content)
    ".zip",                             # zip archive (CSV shards or single CSV)
    ".parquet", ".pq", ".parq",         # Parquet files
    ".json", ".jsonl", ".ndjson",       # JSON / NDJSON files
    ".xlsx", ".xls",                    # Excel files
    ".duckdb",                          # DuckDB database files
    ".db", ".sqlite", ".sqlite3",       # SQLite database files
})


# ---------------------------------------------------------------------------
# Column profiling — parallel across columns
# ---------------------------------------------------------------------------

# Minimum columns before parallel profiling kicks in; below this the
# thread-pool overhead outweighs the benefit.
_MIN_COLS_FOR_PARALLEL = 8

# Workers for column profiling — capped to avoid over-subscription when
# profile_directory already runs multiple files in parallel.
_COL_PROFILE_WORKERS = min(os.cpu_count() or 4, 12)


def _profile_columns_parallel(raw_columns: list) -> list:
    """
    Profile columns using a thread pool when the column count is large enough.

    Column profiling is mostly CPU-bound (type inference, statistics) but
    each column is fully independent.  ThreadPoolExecutor releases the GIL
    during C-extension work (regex, numpy) giving 2-4× speedup on wide
    tables (50+ columns).  Falls back to sequential for narrow tables where
    pool overhead would dominate.
    """
    if len(raw_columns) < _MIN_COLS_FOR_PARALLEL:
        return [profile_column(raw) for raw in raw_columns]

    workers = min(_COL_PROFILE_WORKERS, len(raw_columns))
    with ThreadPoolExecutor(max_workers=workers) as pool:
        return list(pool.map(profile_column, raw_columns))


# ---------------------------------------------------------------------------
# Public entry points
# ---------------------------------------------------------------------------

def run(
    path: str | Path,
    output_dir: str | Path | None = None,
    parallel: bool = True,
) -> "FileProfile | list[FileProfile]":
    """
    Profile a single file or every supported file in a directory.

    Args:
        path:       File path or directory path.
        output_dir: If provided, write JSON profile(s) here.
        parallel:   Use parallel workers for directory profiling (default True).

    Returns:
        FileProfile if path is a file.
        list[FileProfile] if path is a directory (one profile per file).
    """
    path = Path(path).resolve()
    if path.is_dir():
        return profile_directory(path, output_dir=output_dir, parallel=parallel)
    # Database files produce multiple profiles — check by extension.
    if path.suffix.lower() in {".duckdb", ".db", ".sqlite", ".sqlite3"}:
        return profile_database(path, output_dir=output_dir)
    return profile_file(path, output_dir=output_dir)


def profile_file(
    path: str | Path,
    output_dir: str | Path | None = None,
    progress_callback: "callable | None" = None,
) -> FileProfile:
    """
    Run the full pipeline on a single file.

    Args:
        path:       Path to the file (plain CSV, .csv.gz, or .zip archive).
        output_dir: Directory to write the JSON profile; skipped if None.

    Returns:
        Completed FileProfile.

    Raises:
        FileNotFoundError    — path does not exist or is not a file.
        EmptyFileError       — file is 0 bytes.
        CorruptFileError     — structural corruption exceeds threshold.
        ValueError           — format could not be determined (UNKNOWN).
        NotImplementedError  — format recognised but engine not yet built.
    """
    path = Path(path).resolve()
    log.info("Profiling: %s", path.name)

    def _progress(step: int, total: int = 8, msg: str = "") -> None:
        if progress_callback is not None:
            progress_callback(step, total, msg)

    # ── Layer 1 — Intake ─────────────────────────────────────────────────────
    _progress(1, 8, "Intake validation")
    intake = validate(path)

    # ── Layer 2 — Classification ──────────────────────────────────────────────
    _progress(2, 8, "Classifying file format")
    fmt = classify(intake)

    if fmt == FileFormat.UNKNOWN:
        raise ValueError(
            f"Cannot determine format for '{path.name}'. "
            f"File will not be profiled."
        )

    if fmt in (FileFormat.DUCKDB, FileFormat.SQLITE):
        raise ValueError(
            f"'{path.name}' is a {fmt.value} database containing multiple tables. "
            f"Use profile_database() instead of profile_file()."
        )

    if fmt not in (FileFormat.CSV, FileFormat.PARQUET, FileFormat.JSON, FileFormat.EXCEL):
        raise NotImplementedError(
            f"'{path.name}' detected as {fmt.value}. "
            f"Only CSV, Parquet, JSON, Excel, DuckDB, and SQLite engines are available."
        )

    # ── Layer 3 — Size strategy ───────────────────────────────────────────────
    _progress(3, 8, "Selecting size strategy")
    strategy = select(intake)
    log.debug("%s → format=%s  strategy=%s", path.name, fmt.value, strategy.value)

    # ── Layer 4/5 — Format engine ─────────────────────────────────────────────
    _progress(4, 8, f"Running {fmt.value} engine")
    if fmt == FileFormat.CSV:
        raw_columns, row_count, is_exact = csv_engine.profile(path, strategy, intake)
    elif fmt == FileFormat.JSON:
        raw_columns, row_count, is_exact = json_engine.profile(path, strategy, intake)
    elif fmt == FileFormat.EXCEL:
        raw_columns, row_count, is_exact = excel_engine.profile(path, strategy, intake)
    else:  # PARQUET
        raw_columns, row_count, is_exact = parquet_engine.profile(path, strategy, intake)

    # ── Layer 6.5 — Standardization ───────────────────────────────────────────
    _progress(5, 8, "Standardizing columns")
    std_report = None
    if settings.STANDARDIZATION_ENABLED:
        raw_columns, std_report = standardize(raw_columns)

    # ── Layer 7 — Column profiler (parallel across columns) ─────────────────
    _progress(6, 8, "Profiling columns")
    col_profiles = _profile_columns_parallel(raw_columns)

    # Wire original_name and quality flags from standardization
    if std_report is not None:
        for cp, detail in zip(col_profiles, std_report.details):
            if detail.name_changed:
                cp.original_name = detail.original_name
            if detail.nulls_normalized > 0:
                if QualityFlag.NULL_VARIANT_NORMALIZED not in cp.quality_flags:
                    cp.quality_flags.append(QualityFlag.NULL_VARIANT_NORMALIZED)

    # ── Layer 8 — Structural checker ─────────────────────────────────────────
    _progress(7, 8, "Running quality checks")
    col_profiles, structural_issues = structural_check(
        col_profiles,
        corrupt_row_count=0,      # CSV engine raises before this point on hard corruption
        encoding=intake.encoding,
    )

    # ── Assemble FileProfile ──────────────────────────────────────────────────
    file_profile = FileProfile(
        source_type        = "file",
        file_format        = fmt,
        file_path          = str(path),
        table_name         = path.stem,
        row_count          = row_count,
        is_row_count_exact = is_exact,
        encoding           = intake.encoding,
        size_bytes         = intake.size_bytes,
        size_strategy      = strategy,
        corrupt_row_count  = 0,
        columns            = col_profiles,
        structural_issues  = structural_issues,
        standardization_applied = (std_report is not None)
    )

    # ── Layer 11 — Write output ───────────────────────────────────────────────
    _progress(8, 8, "Writing output")
    if output_dir is not None:
        output_path = Path(output_dir) / f"{path.stem}_profile.json"
        write(file_profile, output_path)
        log.info("Profile written → %s", output_path)

    return file_profile


def profile_database(
    path: str | Path,
    fmt: FileFormat | None = None,
    output_dir: str | Path | None = None,
    table_filter: list[str] | None = None,
) -> list[FileProfile]:
    """
    Profile all tables inside a DuckDB or SQLite database file.

    A single database file contains multiple tables, so this returns a list
    of FileProfile objects — one per table.

    Args:
        path:         Path to the .duckdb or .db/.sqlite file.
        fmt:          FileFormat.DUCKDB or FileFormat.SQLITE.  Auto-detected if None.
        output_dir:   Write JSON profiles here (one per table); skipped if None.
        table_filter: If provided, only profile these table names.

    Returns:
        List of FileProfile objects, one per table.
    """
    path = Path(path).resolve()
    log.info("Profiling database: %s", path.name)

    # ── Layer 1 — Intake ─────────────────────────────────────────────────────
    intake = validate(path)

    # ── Layer 2 — Classification ──────────────────────────────────────────────
    if fmt is None:
        fmt = classify(intake)

    if fmt not in (FileFormat.DUCKDB, FileFormat.SQLITE):
        raise ValueError(
            f"'{path.name}' detected as {fmt.value}, not a database file."
        )

    # ── Layer 4 — Database engine (enumerate tables, count, sample) ───────────
    table_results = db_engine.profile(path, fmt, table_filter=table_filter)

    if not table_results:
        log.warning("No tables found in %s", path.name)
        return []

    # ── Layers 6.5–8 per table (parallel when multiple tables) ─────────────
    def _process_table(tr):
        """Run standardize → column profile → structural check for one table."""
        # Layer 6.5 — Standardization
        raw_columns = tr.raw_columns
        std_report = None
        if settings.STANDARDIZATION_ENABLED:
            raw_columns, std_report = standardize(raw_columns)

        # Layer 7 — Column profiler (parallel across columns)
        col_profiles = _profile_columns_parallel(raw_columns)

        # Wire standardization metadata
        if std_report is not None:
            for cp, detail in zip(col_profiles, std_report.details):
                if detail.name_changed:
                    cp.original_name = detail.original_name
                if detail.nulls_normalized > 0:
                    if QualityFlag.NULL_VARIANT_NORMALIZED not in cp.quality_flags:
                        cp.quality_flags.append(QualityFlag.NULL_VARIANT_NORMALIZED)

        # Layer 8 — Structural checker
        col_profiles, structural_issues = structural_check(
            col_profiles,
            corrupt_row_count=0,
            encoding="binary",
        )

        # Assemble FileProfile
        file_profile = FileProfile(
            source_type="database",
            file_format=fmt,
            file_path=str(path),
            table_name=tr.table_name,
            row_count=tr.row_count,
            is_row_count_exact=tr.is_row_count_exact,
            encoding="binary",
            size_bytes=intake.size_bytes,
            size_strategy=SizeStrategy.MEMORY_SAFE,
            corrupt_row_count=0,
            columns=col_profiles,
            structural_issues=structural_issues,
            standardization_applied=(std_report is not None),
        )

        # Layer 11 — Write output
        if output_dir is not None:
            out_path = Path(output_dir) / f"{tr.table_name}_profile.json"
            write(file_profile, out_path)
            log.info("Profile written → %s", out_path)

        return file_profile

    profiles: list[FileProfile] = []
    workers = min(settings.MAX_PARALLEL_WORKERS, len(table_results))

    if workers > 1:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            future_to_table = {
                pool.submit(_process_table, tr): tr for tr in table_results
            }
            for future in as_completed(future_to_table):
                tr = future_to_table[future]
                try:
                    file_profile = future.result()
                    profiles.append(file_profile)
                    log.info(
                        "  ✓ %s.%s — %d col(s), %d row(s)",
                        path.stem, tr.table_name,
                        len(file_profile.columns), tr.row_count,
                    )
                except Exception as exc:
                    log.error(
                        "  ✗ %s.%s — failed: %s",
                        path.stem, tr.table_name, exc, exc_info=True,
                    )
    else:
        for tr in table_results:
            try:
                file_profile = _process_table(tr)
                profiles.append(file_profile)
                log.info(
                    "  ✓ %s.%s — %d col(s), %d row(s)",
                    path.stem, tr.table_name,
                    len(file_profile.columns), tr.row_count,
                )
            except Exception as exc:
                log.error(
                    "  ✗ %s.%s — failed: %s",
                    path.stem, tr.table_name, exc, exc_info=True,
                )

    return profiles


def profile_directory(
    dir_path: str | Path,
    output_dir: str | Path | None = None,
    parallel: bool = True,
) -> list[FileProfile]:
    """
    Profile every supported file in a directory (non-recursive).

    Files with unsupported or unrecognised formats are logged and skipped;
    they do not cause the entire batch to fail.

    Args:
        dir_path:   Directory to scan.
        output_dir: Write JSON profiles here (one per file); skipped if None.
        parallel:   Use parallel workers (default True).  Falls back to
                    sequential when workers=1 or only one candidate file.

    Returns:
        List of FileProfile objects for every successfully profiled file.

    Raises:
        NotADirectoryError — dir_path is not a directory.
    """
    dir_path = Path(dir_path).resolve()
    if not dir_path.is_dir():
        raise NotADirectoryError(f"Not a directory: {dir_path}")

    candidates = sorted(
        f for f in dir_path.iterdir()
        if f.is_file() and f.suffix.lower() in _SCANNABLE_EXTENSIONS
    )

    if not candidates:
        log.warning("No supported files found in %s", dir_path)
        return []

    log.info("Found %d candidate file(s) in %s", len(candidates), dir_path)

    workers = settings.MAX_PARALLEL_WORKERS
    use_parallel = parallel and workers > 1 and len(candidates) > 1

    if use_parallel:
        return _profile_directory_parallel(candidates, output_dir, workers)
    return _profile_directory_sequential(candidates, output_dir)


_DB_EXTENSIONS = frozenset({".duckdb", ".db", ".sqlite", ".sqlite3"})


def _profile_directory_sequential(
    candidates: list[Path],
    output_dir: str | Path | None,
) -> list[FileProfile]:
    """Profile files one at a time (original behaviour)."""
    results: list[FileProfile] = []
    for file_path in candidates:
        try:
            if file_path.suffix.lower() in _DB_EXTENSIONS:
                db_profiles = profile_database(file_path, output_dir=output_dir)
                results.extend(db_profiles)
            else:
                fp = profile_file(file_path, output_dir=output_dir)
                results.append(fp)
                log.info(
                    "  ✓ %s — %d col(s), %d row(s)",
                    file_path.name, len(fp.columns), fp.row_count,
                )
        except (EmptyFileError, CorruptFileError) as exc:
            log.warning("  ✗ %s — skipped (%s)", file_path.name, exc)
        except (ValueError, NotImplementedError) as exc:
            log.info("  – %s — skipped (%s)", file_path.name, exc)
        except Exception as exc:
            log.error(
                "  ✗ %s — unexpected error: %s",
                file_path.name, exc, exc_info=True,
            )
    return results


def _profile_one(
    file_path: Path,
    output_dir: str | Path | None,
) -> "FileProfile | list[FileProfile]":
    """Top-level worker function for ProcessPoolExecutor (must be picklable)."""
    if file_path.suffix.lower() in _DB_EXTENSIONS:
        return profile_database(file_path, output_dir=output_dir)
    return profile_file(file_path, output_dir=output_dir)


def _profile_directory_parallel(
    candidates: list[Path],
    output_dir: str | Path | None,
    workers: int,
) -> list[FileProfile]:
    """Profile files in parallel using a process pool.

    Uses ProcessPoolExecutor to bypass the GIL for CPU-bound CSV/Parquet
    profiling.  Falls back to ThreadPoolExecutor if process spawning fails
    (e.g. in environments that don't support multiprocessing).
    """
    results: list[FileProfile] = []
    # Map future → file_path for logging on completion.
    try:
        pool_cls = ProcessPoolExecutor
        # Quick picklability check — ProcessPoolExecutor needs top-level functions
        pool_cls(max_workers=1).shutdown(wait=False)
    except Exception:
        log.warning("ProcessPoolExecutor unavailable, falling back to threads")
        pool_cls = ThreadPoolExecutor
    with pool_cls(max_workers=workers) as pool:
        future_to_path = {
            pool.submit(_profile_one, fp, output_dir): fp
            for fp in candidates
        }
        for future in as_completed(future_to_path):
            file_path = future_to_path[future]
            try:
                result = future.result()
                if isinstance(result, list):
                    results.extend(result)
                    log.info(
                        "  ✓ %s — %d table(s) profiled",
                        file_path.name, len(result),
                    )
                else:
                    results.append(result)
                    log.info(
                        "  ✓ %s — %d col(s), %d row(s)",
                        file_path.name, len(result.columns), result.row_count,
                    )
            except (EmptyFileError, CorruptFileError) as exc:
                log.warning("  ✗ %s — skipped (%s)", file_path.name, exc)
            except (ValueError, NotImplementedError) as exc:
                log.info("  – %s — skipped (%s)", file_path.name, exc)
            except Exception as exc:
                log.error(
                    "  ✗ %s — unexpected error: %s",
                    file_path.name, exc, exc_info=True,
                )
    return results


# ---------------------------------------------------------------------------
# Cross-table relationship analysis
# ---------------------------------------------------------------------------

def profile_remote(
    uri: str,
    connection_id: str | None = None,
    table_filter: list[str] | None = None,
    output_dir: str | Path | None = None,
    progress_callback: "callable | None" = None,
) -> "FileProfile | list[FileProfile]":
    """
    Profile a remote data source (cloud storage or database).

    Supports:
        - Object storage: s3://, abfss://, gs:// (via DuckDB httpfs/azure)
        - Databases: postgresql:// (via DuckDB postgres_scanner),
          snowflake:// (via native SDK)

    Remote sources bypass the intake/classify/strategy layers and enter
    the pipeline at the RawColumnData level.  Everything downstream
    (standardization, column profiling, quality checks) is identical
    to local profiling.

    Args:
        uri:            Remote URI (e.g. "s3://bucket/path/file.parquet").
        connection_id:  Name of a registered connection for credentials.
                        If None, falls back to environment variables.
        table_filter:   For databases: only profile these table names.
        output_dir:     Directory to write JSON profiles; skipped if None.
        progress_callback: Optional (step, total, msg) callback.

    Returns:
        Single FileProfile (for a single file/table) or list[FileProfile]
        (for a directory/schema listing).
    """
    from file_profiler.connectors.uri_parser import parse_uri
    from file_profiler.connectors.registry import registry
    from file_profiler.connectors.connection_manager import get_connection_manager

    descriptor = parse_uri(uri, connection_id=connection_id)
    connector = registry.get(descriptor.scheme)
    mgr = get_connection_manager()
    credentials = mgr.resolve_credentials(descriptor)

    def _progress(step: int, total: int = 5, msg: str = "") -> None:
        if progress_callback is not None:
            progress_callback(step, total, msg)

    _progress(1, 5, f"Connecting to {descriptor.scheme}")

    if descriptor.is_object_storage:
        return _profile_remote_storage(
            descriptor, connector, credentials,
            table_filter, output_dir, _progress,
        )
    elif descriptor.is_database:
        return _profile_remote_database(
            descriptor, connector, credentials,
            table_filter, output_dir, _progress,
        )
    else:
        raise ValueError(f"Unsupported remote scheme: {descriptor.scheme}")


def _profile_remote_storage(
    descriptor, connector, credentials,
    table_filter, output_dir, _progress,
) -> "FileProfile | list[FileProfile]":
    """Profile files from cloud object storage (S3/ADLS/GCS)."""
    from file_profiler.connectors.duckdb_remote import (
        create_remote_connection,
        remote_count,
        remote_sample,
    )
    from file_profiler.models.enums import FileFormat, SizeStrategy

    if descriptor.is_directory_like:
        _progress(2, 5, "Listing remote objects")
        objects = connector.list_objects(descriptor, credentials)
        if table_filter:
            objects = [o for o in objects if o.name in table_filter]
        if not objects:
            log.warning("No profilable files found at %s", descriptor.raw_uri)
            return []

        con = create_remote_connection(descriptor, credentials)
        profiles = []
        for i, obj in enumerate(objects):
            _progress(3, 5, f"Profiling {obj.name} ({i+1}/{len(objects)})")
            try:
                fp = _profile_single_remote(
                    con, connector, descriptor, obj.uri, obj.name,
                    obj.file_format or "csv", output_dir,
                )
                profiles.append(fp)
            except Exception as exc:
                log.warning("Skipping %s: %s", obj.name, exc)
        con.close()
        _progress(5, 5, f"Done — {len(profiles)} files profiled")
        return profiles
    else:
        # Single file
        con = create_remote_connection(descriptor, credentials)
        _progress(2, 5, "Profiling remote file")
        name = descriptor.path.rsplit("/", 1)[-1] if "/" in descriptor.path else descriptor.path
        fmt = _guess_format(name)
        try:
            fp = _profile_single_remote(
                con, connector, descriptor, descriptor.raw_uri, name,
                fmt, output_dir,
            )
        finally:
            con.close()
        _progress(5, 5, "Done")
        return fp


def _profile_remote_database(
    descriptor, connector, credentials,
    table_filter, output_dir, _progress,
) -> list[FileProfile]:
    """Profile tables from a remote database (PostgreSQL/Snowflake)."""
    from file_profiler.models.enums import FileFormat, SizeStrategy

    # List tables
    _progress(2, 5, "Listing tables")
    if descriptor.table_name:
        # Single table specified
        from file_profiler.connectors.base import RemoteObject
        tables = [RemoteObject(
            name=descriptor.table_name,
            uri=descriptor.table_name,
            file_format=descriptor.scheme,
        )]
    else:
        tables = connector.list_objects(descriptor, credentials)

    if table_filter:
        tables = [t for t in tables if t.name in table_filter]

    if not tables:
        log.warning("No tables found at %s", descriptor.raw_uri)
        return []

    profiles = []

    if connector.supports_duckdb(descriptor):
        from file_profiler.connectors.duckdb_remote import (
            create_remote_connection,
            remote_count,
            remote_sample,
        )
        con = create_remote_connection(descriptor, credentials)
        for i, tbl in enumerate(tables):
            _progress(3, 5, f"Profiling {tbl.name} ({i+1}/{len(tables)})")
            try:
                scan_expr = connector.duckdb_scan_expression(descriptor, object_uri=tbl.name)
                row_count = remote_count(con, scan_expr)
                headers, rows = remote_sample(con, scan_expr)
                raw_columns = _rows_to_raw_columns(headers, rows, row_count)
                fp = _assemble_remote_profile(
                    raw_columns, row_count, tbl.name,
                    descriptor, output_dir,
                    source_type="remote_database",
                )
                profiles.append(fp)
            except Exception as exc:
                log.warning("Skipping table %s: %s", tbl.name, exc)
        con.close()
    else:
        # Native SDK path (Snowflake)
        for i, tbl in enumerate(tables):
            _progress(3, 5, f"Profiling {tbl.name} ({i+1}/{len(tables)})")
            try:
                row_count, headers, rows = connector.snowflake_count_and_sample(
                    descriptor, credentials, tbl.name,
                )
                raw_columns = _rows_to_raw_columns(headers, rows, row_count)
                fp = _assemble_remote_profile(
                    raw_columns, row_count, tbl.name,
                    descriptor, output_dir,
                    source_type="remote_database",
                )
                profiles.append(fp)
            except Exception as exc:
                log.warning("Skipping table %s: %s", tbl.name, exc)

    _progress(5, 5, f"Done — {len(profiles)} tables profiled")
    return profiles


def _profile_single_remote(
    con, connector, descriptor, file_uri, name, fmt_str, output_dir,
) -> FileProfile:
    """Profile a single remote file via DuckDB."""
    from file_profiler.connectors.duckdb_remote import remote_count, remote_sample

    scan_expr = connector.duckdb_scan_expression(descriptor, object_uri=file_uri)
    row_count = remote_count(con, scan_expr)
    headers, rows = remote_sample(con, scan_expr)
    raw_columns = _rows_to_raw_columns(headers, rows, row_count)

    return _assemble_remote_profile(
        raw_columns, row_count, Path(name).stem,
        descriptor, output_dir,
        source_type="remote_storage",
        file_format_str=fmt_str,
    )


def _rows_to_raw_columns(
    headers: list[str],
    rows: list[list],
    row_count: int,
) -> list:
    """Convert DuckDB/SDK sample output to RawColumnData list."""
    from file_profiler.models.file_profile import RawColumnData

    raw_columns = []
    for col_idx, col_name in enumerate(headers):
        values = []
        null_count = 0
        for row in rows:
            val = row[col_idx] if col_idx < len(row) else None
            if val is None:
                null_count += 1
            else:
                values.append(str(val))

        raw_columns.append(RawColumnData(
            name=col_name,
            declared_type=None,
            values=values,
            total_count=row_count,
            null_count=null_count,
        ))
    return raw_columns


def _assemble_remote_profile(
    raw_columns, row_count, table_name,
    descriptor, output_dir,
    source_type="remote_storage",
    file_format_str=None,
) -> FileProfile:
    """Run standardization + column profiling + quality checks on remote data."""
    from file_profiler.models.enums import FileFormat, SizeStrategy

    # Standardization
    std_report = None
    if settings.STANDARDIZATION_ENABLED:
        raw_columns, std_report = standardize(raw_columns)

    # Column profiling
    col_profiles = _profile_columns_parallel(raw_columns)

    # Wire standardization info
    if std_report is not None:
        for cp, detail in zip(col_profiles, std_report.details):
            if detail.name_changed:
                cp.original_name = detail.original_name
            if detail.nulls_normalized > 0:
                if QualityFlag.NULL_VARIANT_NORMALIZED not in cp.quality_flags:
                    cp.quality_flags.append(QualityFlag.NULL_VARIANT_NORMALIZED)

    # Quality checks
    col_profiles, structural_issues = structural_check(
        col_profiles, corrupt_row_count=0, encoding="utf-8",
    )

    # Map format string to enum
    fmt_map = {
        "csv": FileFormat.CSV, "parquet": FileFormat.PARQUET,
        "json": FileFormat.JSON, "excel": FileFormat.EXCEL,
        "postgresql": FileFormat.UNKNOWN, "snowflake": FileFormat.UNKNOWN,
    }
    file_fmt = fmt_map.get(file_format_str or "", FileFormat.UNKNOWN)

    fp = FileProfile(
        source_type=source_type,
        file_format=file_fmt,
        file_path=descriptor.raw_uri,
        table_name=table_name,
        row_count=row_count,
        is_row_count_exact=True,
        encoding="utf-8",
        size_bytes=0,
        size_strategy=SizeStrategy.MEMORY_SAFE,
        columns=col_profiles,
        structural_issues=structural_issues,
        standardization_applied=(std_report is not None),
        source_uri=descriptor.raw_uri,
        connection_id=descriptor.connection_id,
    )

    if output_dir is not None:
        out_path = Path(output_dir) / f"{table_name}_profile.json"
        write(fp, out_path)
        log.info("Remote profile written → %s", out_path)

    return fp


def _guess_format(filename: str) -> str:
    """Guess file format from extension."""
    ext = Path(filename).suffix.lower()
    mapping = {
        ".csv": "csv", ".tsv": "csv",
        ".parquet": "parquet", ".pq": "parquet", ".parq": "parquet",
        ".json": "json", ".jsonl": "json", ".ndjson": "json",
        ".gz": "csv",
    }
    return mapping.get(ext, "csv")


def analyze_relationships(
    profiles: "list[FileProfile]",
    output_path: "str | Path | None" = None,
) -> RelationshipReport:
    """
    Detect foreign-key candidates across a set of already-profiled tables.

    Produces intermediate relationship signals (name, type, cardinality,
    value-overlap scoring).  The output is saved as structured JSON and
    intended as input for the LLM enrichment pipeline — not as a final
    deliverable.  The enrichment REDUCE phase produces the final ER
    diagram and join recommendations.

    Args:
        profiles:    List of FileProfile objects (from profile_directory or
                     multiple profile_file calls).
        output_path: If provided, write the RelationshipReport as JSON here.

    Returns:
        RelationshipReport with FK candidates sorted by confidence descending.
    """
    report = _detect_relationships(profiles)
    if output_path is not None:
        output_path = Path(output_path)
        _write_relationships(report, output_path)
        log.info("Relationship report written → %s", output_path)
    return report
