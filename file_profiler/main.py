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
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

from file_profiler.analysis.relationship_detector import detect as _detect_relationships
from file_profiler.classification.classifier import classify
from file_profiler.config import settings
from file_profiler.engines import csv_engine, excel_engine, json_engine, parquet_engine
from file_profiler.intake.errors import CorruptFileError, EmptyFileError
from file_profiler.intake.validator import validate
from file_profiler.models.enums import FileFormat, QualityFlag
from file_profiler.models.file_profile import FileProfile
from file_profiler.models.relationships import RelationshipReport
from file_profiler.output.profile_writer import write
from file_profiler.output.er_diagram_writer import write as _write_er_diagram
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
})


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

    if fmt not in (FileFormat.CSV, FileFormat.PARQUET, FileFormat.JSON, FileFormat.EXCEL):
        raise NotImplementedError(
            f"'{path.name}' detected as {fmt.value}. "
            f"Only CSV, Parquet, JSON, and Excel engines are available in the current build."
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

    # ── Layer 7 — Column profiler ─────────────────────────────────────────────
    _progress(6, 8, "Profiling columns")
    col_profiles = [profile_column(raw) for raw in raw_columns]

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


def _profile_directory_sequential(
    candidates: list[Path],
    output_dir: str | Path | None,
) -> list[FileProfile]:
    """Profile files one at a time (original behaviour)."""
    results: list[FileProfile] = []
    for file_path in candidates:
        try:
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
) -> FileProfile:
    """Top-level worker function for ProcessPoolExecutor (must be picklable)."""
    return profile_file(file_path, output_dir=output_dir)


def _profile_directory_parallel(
    candidates: list[Path],
    output_dir: str | Path | None,
    workers: int,
) -> list[FileProfile]:
    """Profile files in parallel using a thread pool."""
    results: list[FileProfile] = []
    # Map future → file_path for logging on completion.
    with ThreadPoolExecutor(max_workers=workers) as pool:
        future_to_path = {
            pool.submit(_profile_one, fp, output_dir): fp
            for fp in candidates
        }
        for future in as_completed(future_to_path):
            file_path = future_to_path[future]
            try:
                fp = future.result()
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


# ---------------------------------------------------------------------------
# Cross-table relationship analysis
# ---------------------------------------------------------------------------

def analyze_relationships(
    profiles: "list[FileProfile]",
    output_path: "str | Path | None" = None,
    er_diagram_path: "str | Path | None" = None,
    er_min_confidence: float = 0.70,
) -> RelationshipReport:
    """
    Detect foreign-key candidates across a set of already-profiled tables.

    Args:
        profiles:          List of FileProfile objects (from profile_directory or
                           multiple profile_file calls).
        output_path:       If provided, write the RelationshipReport as JSON here.
        er_diagram_path:   If provided, write a Mermaid ER diagram (.md) here.
        er_min_confidence: Minimum confidence for a relationship to appear in
                           the ER diagram (default 0.70).

    Returns:
        RelationshipReport with FK candidates sorted by confidence descending.
    """
    report = _detect_relationships(profiles)
    if output_path is not None:
        output_path = Path(output_path)
        _write_relationships(report, output_path)
        log.info("Relationship report written → %s", output_path)
    if er_diagram_path is not None:
        _write_er_diagram(profiles, report, er_diagram_path, er_min_confidence)
    return report
