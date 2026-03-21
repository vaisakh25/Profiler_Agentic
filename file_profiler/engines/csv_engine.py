"""
Layer 4 — CSV Profiling Engine

Steps (all gated by SizeStrategy):
  A — Structure Detection  : delimiter, quote char, escape char, line endings, row width consistency
  B — Header Detection     : heuristic on first 5 rows; generate col names if absent
  C — Row Count Estimation : chunk-extrapolate or stream count; stores is_exact flag
  D — Sampling Strategy    : full read / chunked reservoir / skip-interval stream
  E — Type Inference       : delegates to profiling/type_inference.py

Multi-file ZIP support:
  When a ZIP archive contains multiple CSV entries (shards of one logical table),
  all entries are profiled as a single dataset.  Structure and headers are detected
  from the first entry; rows are sampled / counted across all entries.

Raises CorruptFileError if structural corruption exceeds STRUCTURAL_CORRUPTION_THRESHOLD.

Entry point:
  profile(path, strategy, intake) -> tuple[list[RawColumnData], int, bool]

Returns:
  (raw_columns, row_count, is_row_count_exact)
"""

from __future__ import annotations

import csv
import gzip
import io
import logging
import random
import zipfile
from contextlib import contextmanager
from pathlib import Path
from typing import Generator, Optional

from file_profiler.config import settings
from file_profiler.intake.errors import CorruptFileError
from file_profiler.intake.validator import IntakeResult
from file_profiler.models.enums import SizeStrategy
from file_profiler.models.file_profile import RawColumnData
from file_profiler.engines.duckdb_sampler import (
    duckdb_connection,
    duckdb_count,
    duckdb_sample,
)
from file_profiler.strategy.size_strategy import effective_size

log = logging.getLogger(__name__)

# File extensions (lower-case) treated as CSV-like when scanning a ZIP archive.
# .txt is deliberately excluded: it is too generic (READMEs, manifests) and
# would cause false positives in mixed-content archives.
_CSV_EXTENSIONS = frozenset({".csv", ".tsv", ".dat", ".psv", ""})


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def profile(
    path: str | Path,
    strategy: SizeStrategy,
    intake: IntakeResult,
) -> tuple[list[RawColumnData], int, bool]:
    """
    Profile a CSV file and return per-column raw data ready for the column profiler.

    Args:
        path:     Path to the CSV file (plain or compressed).
        strategy: Size strategy selected by Layer 3.
        intake:   IntakeResult from Layer 1 (encoding, compression, delimiter_hint).

    Returns:
        (raw_columns, row_count, is_row_count_exact)

    Raises:
        CorruptFileError — structural corruption rate exceeds threshold.
    """
    path = Path(path).resolve()

    # Multi-file ZIP: route to dedicated handler.
    if intake.compression == "zip":
        entries = _zip_csv_entries(path)
        if len(entries) > 1:
            log.debug("%s: ZIP partition with %d entries", path.name, len(entries))
            return _profile_zip_partition(path, strategy, intake, entries)

    # Single file (plain, gz, or single-entry zip).
    # Use DuckDB for any non-ZIP CSV with > DUCKDB_ROW_THRESHOLD rows.
    # A single connection is reused for both the quick-count and sample queries.
    if intake.compression != "zip":
        try:
            with duckdb_connection() as con:
                quick_count = duckdb_count(
                    path,
                    delimiter=intake.delimiter_hint or ",",
                    encoding=intake.encoding,
                    _con=con,
                )
                if quick_count > settings.DUCKDB_ROW_THRESHOLD:
                    return _profile_with_duckdb(path, intake, row_count=quick_count, _con=con)
        except Exception as exc:
            log.debug("DuckDB quick count failed for %s: %s — using Python path", path.name, exc)

    struct = _detect_structure(path, intake)

    # Single-pass: detect headers, count rows, and sample in one file read.
    headers, has_header, row_count, is_exact, sampled_rows = (
        _single_pass_profile(path, intake, struct, strategy)
    )

    if not sampled_rows:
        log.warning("CSV engine: no rows sampled from %s", path.name)
        return [], row_count, is_exact

    raw_columns = _build_raw_columns(headers, sampled_rows, row_count)
    return raw_columns, row_count, is_exact


# ---------------------------------------------------------------------------
# DuckDB fast path (STREAM_ONLY, uncompressed)
# ---------------------------------------------------------------------------

def _profile_with_duckdb(
    path: Path,
    intake: IntakeResult,
    row_count: int | None = None,
    _con=None,
) -> tuple[list[RawColumnData], int, bool]:
    """
    Profile a CSV via DuckDB (plain or gzip-compressed).

    DuckDB handles structure detection, delimiter sniffing, row counting,
    and reservoir sampling in parallel — replacing four separate Python
    streaming passes with two fast DuckDB queries.

    Falls back to the Python streaming path if DuckDB fails (e.g.
    unsupported encoding, malformed file that DuckDB rejects).
    """
    delimiter = intake.delimiter_hint or ","
    encoding = intake.encoding

    try:
        if row_count is None:
            row_count = duckdb_count(path, delimiter=delimiter, encoding=encoding, _con=_con)
        headers, sampled_rows = duckdb_sample(
            path, delimiter=delimiter, encoding=encoding, _con=_con,
        )
    except Exception as exc:
        log.warning(
            "DuckDB sampling failed for %s: %s — falling back to Python streaming",
            path.name, exc,
        )
        struct = _detect_structure(path, intake)
        headers_fb, has_header = _detect_headers(path, intake, struct)
        row_count = _stream_row_count(path, intake, struct)
        sampled_rows = _skip_interval_sample(path, intake, struct, has_header)
        if not sampled_rows:
            return [], row_count, True
        return _build_raw_columns(headers_fb, sampled_rows, row_count), row_count, True

    if not sampled_rows:
        log.warning("DuckDB: no rows sampled from %s", path.name)
        return [], row_count, True

    raw_columns = _build_raw_columns(headers, sampled_rows, row_count)
    log.info(
        "DuckDB profiled: %s (%d rows, %d columns, %d sampled)",
        path.name, row_count, len(headers), len(sampled_rows),
    )
    return raw_columns, row_count, True


# ---------------------------------------------------------------------------
# Step A — Structure Detection
# ---------------------------------------------------------------------------

class _CsvStructure:
    """Detected structural properties of a CSV file."""
    __slots__ = ("delimiter", "quotechar", "escapechar", "lineterminator",
                 "corrupt_row_ratio")

    def __init__(
        self,
        delimiter: str,
        quotechar: str,
        escapechar: Optional[str],
        lineterminator: str,
        corrupt_row_ratio: float,
    ) -> None:
        self.delimiter         = delimiter
        self.quotechar         = quotechar
        self.escapechar        = escapechar
        self.lineterminator    = lineterminator
        self.corrupt_row_ratio = corrupt_row_ratio


def _detect_structure(path: Path, intake: IntakeResult) -> _CsvStructure:
    """Read the probe lines then delegate to the pure-logic helper."""
    lines = _read_first_lines(path, intake, settings.CSV_STRUCTURE_PROBE_ROWS + 5)
    return _detect_structure_from_lines(path.name, lines, intake.delimiter_hint)


def _detect_structure_from_lines(
    source_name: str,
    lines: list[str],
    delimiter_hint: Optional[str] = None,
) -> _CsvStructure:
    """
    Detect structure from already-read probe lines.

    Extracted as a pure function so _profile_zip_partition can reuse it
    without reopening the file through the intake machinery.

    Raises CorruptFileError if corrupt row ratio > STRUCTURAL_CORRUPTION_THRESHOLD.
    """
    if not lines:
        raise CorruptFileError(f"Cannot read any lines from {source_name}")

    joined = "\n".join(lines)
    lineterminator = "\r\n" if "\r\n" in joined else "\n"

    delimiter = _determine_delimiter(lines, delimiter_hint)

    quotechar  = '"'
    escapechar = None
    try:
        sniffer_input = "\n".join(lines[:30])
        dialect = csv.Sniffer().sniff(sniffer_input, delimiters=delimiter)
        quotechar  = dialect.quotechar or '"'
        escapechar = dialect.escapechar if dialect.doublequote is False else None
    except csv.Error:
        pass

    corrupt_ratio = _measure_corruption(lines, delimiter, quotechar, escapechar)

    if corrupt_ratio > settings.STRUCTURAL_CORRUPTION_THRESHOLD:
        raise CorruptFileError(
            f"{source_name}: structural corruption {corrupt_ratio:.1%} exceeds "
            f"threshold {settings.STRUCTURAL_CORRUPTION_THRESHOLD:.1%}"
        )

    return _CsvStructure(
        delimiter=delimiter,
        quotechar=quotechar,
        escapechar=escapechar,
        lineterminator=lineterminator,
        corrupt_row_ratio=corrupt_ratio,
    )


def _determine_delimiter(lines: list[str], hint: Optional[str]) -> str:
    candidates = settings.CSV_CANDIDATE_DELIMITERS
    best_delim = ","
    best_score = -1.0

    for delim in candidates:
        counts = [line.count(delim) for line in lines[:20] if line.strip()]
        if not counts:
            continue
        avg = sum(counts) / len(counts)
        if avg == 0:
            continue
        variance = sum((c - avg) ** 2 for c in counts) / len(counts)
        score = avg / (1.0 + variance)
        if score > best_score:
            best_score = score
            best_delim = delim

    if hint and hint in candidates:
        hint_counts = [line.count(hint) for line in lines[:20] if line.strip()]
        avg_hint = sum(hint_counts) / len(hint_counts) if hint_counts else 0
        if avg_hint > 0:
            return hint

    return best_delim


def _measure_corruption(
    lines: list[str],
    delimiter: str,
    quotechar: str,
    escapechar: Optional[str],
) -> float:
    field_counts: list[int] = []
    for line in lines:
        if not line.strip():
            continue
        try:
            row = next(csv.reader(
                [line],
                delimiter=delimiter,
                quotechar=quotechar,
                escapechar=escapechar,
            ))
            field_counts.append(len(row))
        except csv.Error:
            field_counts.append(-1)

    if not field_counts:
        return 0.0

    mode_count = max(set(field_counts), key=field_counts.count)
    bad = sum(1 for c in field_counts if c != mode_count)
    return bad / len(field_counts)


# ---------------------------------------------------------------------------
# Combined single-pass: Header + Row Count + Sampling
# ---------------------------------------------------------------------------

def _single_pass_profile(
    path: Path,
    intake: IntakeResult,
    struct: _CsvStructure,
    strategy: SizeStrategy,
) -> tuple[list[str], bool, int, bool, list[list[str]]]:
    """
    Detect headers, count rows, and collect samples in ONE file read.

    Replaces three separate passes (_detect_headers, _estimate_row_count,
    _sample_rows) with a single streaming pass through the file.

    Returns:
        (headers, has_header, row_count, is_row_count_exact, sampled_rows)
    """
    k = settings.SAMPLE_ROW_COUNT
    rng = random.Random(42)
    interval = settings.STREAM_SKIP_INTERVAL

    # Accumulators
    header_probe: list[list[str]] = []
    sample: list[list[str]] = []
    row_count = 0
    is_exact = True

    # For LAZY_SCAN extrapolation
    total_bytes = 0
    extrapolation_rows = 0
    extrapolation_done = False
    max_extrapolation_rows = settings.ROW_COUNT_ESTIMATION_CHUNKS * settings.CHUNK_SIZE

    with _open_text(path, intake) as fh:
        reader = _make_reader(fh, struct)

        for i, row in enumerate(reader):
            # --- Header probe: buffer first N rows for header detection ---
            if i < settings.HEADER_DETECTION_ROWS:
                header_probe.append([cell.strip() for cell in row])

            # --- Determine header status after probe rows collected ---
            if i == settings.HEADER_DETECTION_ROWS - 1:
                headers, has_header = _detect_headers_from_rows(header_probe, struct)
                # Replay buffered data rows into the sample
                data_start = 1 if has_header else 0
                for buffered_row in header_probe[data_start:]:
                    _add_to_sample(
                        buffered_row, len(sample), sample, k, rng,
                        strategy, interval,
                    )
                    row_count += 1
                    if strategy == SizeStrategy.LAZY_SCAN and not extrapolation_done:
                        total_bytes += len(struct.delimiter.join(buffered_row).encode(
                            intake.encoding, errors="replace"
                        ))
                        extrapolation_rows += 1
                continue  # skip further processing for probe rows

            # --- Handle files shorter than HEADER_DETECTION_ROWS ---
            if i < settings.HEADER_DETECTION_ROWS:
                continue

            # --- Main streaming loop (after header detection) ---
            row_count += 1

            # Row count estimation for LAZY_SCAN (extrapolation)
            if strategy == SizeStrategy.LAZY_SCAN and not extrapolation_done:
                row_bytes = len(struct.delimiter.join(row).encode(
                    intake.encoding, errors="replace"
                ))
                total_bytes += row_bytes
                extrapolation_rows += 1
                if extrapolation_rows >= max_extrapolation_rows:
                    extrapolation_done = True

            # Sampling
            _add_to_sample(
                row, row_count - 1, sample, k, rng, strategy, interval,
            )

    # Handle files with fewer rows than HEADER_DETECTION_ROWS
    if not header_probe:
        return [], False, 0, True, []
    if len(header_probe) < settings.HEADER_DETECTION_ROWS:
        headers, has_header = _detect_headers_from_rows(header_probe, struct)
        data_start = 1 if has_header else 0
        sample = header_probe[data_start:]
        row_count = len(sample)
        return headers, has_header, row_count, True, sample

    # Compute final row count
    if strategy == SizeStrategy.LAZY_SCAN and extrapolation_rows > 0:
        bytes_per_row = total_bytes / extrapolation_rows
        uncompressed_size = effective_size(intake)
        estimated = int(uncompressed_size / bytes_per_row)
        is_exact = False
        log.debug(
            "Row count estimate: %d rows from %.1f bytes/row on %d byte file",
            estimated, bytes_per_row, uncompressed_size,
        )
        row_count = estimated

    return headers, has_header, row_count, is_exact, sample


def _add_to_sample(
    row: list[str],
    idx: int,
    sample: list[list[str]],
    k: int,
    rng: random.Random,
    strategy: SizeStrategy,
    interval: int,
) -> None:
    """Add a row to the sample using the strategy-appropriate method."""
    if strategy == SizeStrategy.MEMORY_SAFE:
        # Keep all rows
        sample.append(row)
    elif strategy == SizeStrategy.LAZY_SCAN:
        # Reservoir sampling (Vitter's Algorithm R)
        if idx < k:
            sample.append(row)
        else:
            j = rng.randint(0, idx)
            if j < k:
                sample[j] = row
    else:
        # STREAM_ONLY: skip-interval sampling
        if idx % interval == 0:
            sample.append(row)


# ---------------------------------------------------------------------------
# Step B — Header Detection
# ---------------------------------------------------------------------------

def _detect_headers(
    path: Path,
    intake: IntakeResult,
    struct: _CsvStructure,
) -> tuple[list[str], bool]:
    """Read the probe rows then delegate to the pure-logic helper."""
    rows = _parse_rows(path, intake, struct, max_rows=settings.HEADER_DETECTION_ROWS)
    return _detect_headers_from_rows(rows, struct)


def _detect_headers_from_rows(
    rows: list[list[str]],
    struct: _CsvStructure,
) -> tuple[list[str], bool]:
    """
    Determine whether the first row is a header or data.

    Heuristics:
    - All cells are non-numeric and no cell is blank → header present
    - Otherwise → no header; generate stable column_1, column_2, ... names

    Duplicate column names are a structural issue, not evidence the row is data;
    _deduplicate_headers handles them.
    """
    if not rows:
        return [], False

    first_row = rows[0]
    all_non_numeric = all(not _looks_numeric(cell) for cell in first_row)
    has_empty = any(cell.strip() == "" for cell in first_row)

    if all_non_numeric and not has_empty:
        headers = [cell.strip() for cell in first_row]
        return _deduplicate_headers(headers), True

    n_cols = len(first_row)
    return [f"column_{i + 1}" for i in range(n_cols)], False


def _looks_numeric(value: str) -> bool:
    v = value.strip().replace(",", "")
    try:
        float(v)
        return True
    except (ValueError, AttributeError):
        return False


def _deduplicate_headers(headers: list[str]) -> list[str]:
    seen: dict[str, int] = {}
    result: list[str] = []
    for h in headers:
        if h not in seen:
            seen[h] = 0
            result.append(h)
        else:
            seen[h] += 1
            result.append(f"{h}_{seen[h] + 1}")
    return result


# ---------------------------------------------------------------------------
# Step C — Row Count Estimation
# ---------------------------------------------------------------------------

def _estimate_row_count(
    path: Path,
    intake: IntakeResult,
    struct: _CsvStructure,
    strategy: SizeStrategy,
) -> tuple[int, bool]:
    if strategy == SizeStrategy.MEMORY_SAFE:
        return _exact_row_count(path, intake, struct), True

    if strategy == SizeStrategy.STREAM_ONLY:
        return _stream_row_count(path, intake, struct), True

    return _extrapolate_row_count(path, intake, struct), False


def _exact_row_count(path: Path, intake: IntakeResult, struct: _CsvStructure) -> int:
    count = 0
    with _open_text(path, intake) as fh:
        reader = _make_reader(fh, struct)
        for _ in reader:
            count += 1
    return max(0, count - 1)


def _stream_row_count(path: Path, intake: IntakeResult, struct: _CsvStructure) -> int:
    count = 0
    with _open_text(path, intake) as fh:
        reader = _make_reader(fh, struct)
        next(reader, None)
        for _ in reader:
            count += 1
    return count


def _extrapolate_row_count(
    path: Path,
    intake: IntakeResult,
    struct: _CsvStructure,
) -> int:
    """
    Compute bytes-per-row from the first ROW_COUNT_ESTIMATION_CHUNKS chunks,
    then multiply by the UNCOMPRESSED file size.

    Gap 2 fix: effective_size() (imported from size_strategy) returns the
    uncompressed size for gzip/zip files, not the on-disk compressed size.
    Without this, a 5x-compressed gzip file would produce a 5x underestimate.
    """
    total_rows  = 0
    total_bytes = 0
    chunks_read = 0

    uncompressed_size = effective_size(intake)   # ← correct size for extrapolation

    with _open_text(path, intake) as fh:
        reader = _make_reader(fh, struct)
        next(reader, None)  # skip header

        for row in reader:
            row_bytes = len(struct.delimiter.join(row).encode(
                intake.encoding, errors="replace"
            ))
            total_bytes += row_bytes
            total_rows  += 1

            if total_rows % settings.CHUNK_SIZE == 0:
                chunks_read += 1
                if chunks_read >= settings.ROW_COUNT_ESTIMATION_CHUNKS:
                    break

    if total_bytes == 0:
        return total_rows

    bytes_per_row = total_bytes / total_rows
    estimated = int(uncompressed_size / bytes_per_row)
    log.debug(
        "Row count estimate: %d rows from %.1f bytes/row on %d byte file",
        estimated, bytes_per_row, uncompressed_size,
    )
    return estimated


# ---------------------------------------------------------------------------
# Step D — Sampling Strategy
# ---------------------------------------------------------------------------

def _sample_rows(
    path: Path,
    intake: IntakeResult,
    struct: _CsvStructure,
    strategy: SizeStrategy,
    has_header: bool,
) -> list[list[str]]:
    if strategy == SizeStrategy.MEMORY_SAFE:
        return _read_all_rows(path, intake, struct, has_header)
    if strategy == SizeStrategy.LAZY_SCAN:
        return _reservoir_sample(path, intake, struct, has_header)
    return _skip_interval_sample(path, intake, struct, has_header)


def _read_all_rows(
    path: Path, intake: IntakeResult, struct: _CsvStructure, has_header: bool
) -> list[list[str]]:
    rows: list[list[str]] = []
    with _open_text(path, intake) as fh:
        reader = _make_reader(fh, struct)
        if has_header:
            next(reader, None)
        for row in reader:
            rows.append(row)
    return rows


def _reservoir_sample(
    path: Path, intake: IntakeResult, struct: _CsvStructure, has_header: bool
) -> list[list[str]]:
    """Vitter's Algorithm R — uniform random sample bounded to SAMPLE_ROW_COUNT rows."""
    k      = settings.SAMPLE_ROW_COUNT
    sample: list[list[str]] = []
    rng    = random.Random(42)

    with _open_text(path, intake) as fh:
        reader = _make_reader(fh, struct)
        if has_header:
            next(reader, None)
        for i, row in enumerate(reader):
            if i < k:
                sample.append(row)
            else:
                j = rng.randint(0, i)
                if j < k:
                    sample[j] = row

    return sample


def _skip_interval_sample(
    path: Path, intake: IntakeResult, struct: _CsvStructure, has_header: bool
) -> list[list[str]]:
    interval = settings.STREAM_SKIP_INTERVAL
    sample: list[list[str]] = []
    with _open_text(path, intake) as fh:
        reader = _make_reader(fh, struct)
        if has_header:
            next(reader, None)
        for i, row in enumerate(reader):
            if i % interval == 0:
                sample.append(row)
    return sample


# ---------------------------------------------------------------------------
# Step E — Build RawColumnData (pivot rows → columns)
# ---------------------------------------------------------------------------

def _build_raw_columns(
    headers: list[str],
    rows: list[list[str]],
    total_count: int,
) -> list[RawColumnData]:
    if not headers or not rows:
        return []

    n_cols = len(headers)
    columns: list[list[Optional[str]]] = [[] for _ in range(n_cols)]

    for row in rows:
        for col_idx in range(n_cols):
            value = row[col_idx].strip() if col_idx < len(row) else ""
            columns[col_idx].append(value)

    raw_cols: list[RawColumnData] = []
    for name, values in zip(headers, columns):
        null_count = sum(1 for v in values if v == "")
        normalised = [v if v != "" else None for v in values]
        raw_cols.append(RawColumnData(
            name=name,
            declared_type=None,
            values=normalised,
            total_count=total_count,
            null_count=null_count,
        ))

    return raw_cols


# ---------------------------------------------------------------------------
# Multi-file ZIP partition  (Layer 12 — multi-file partition support)
# ---------------------------------------------------------------------------

def _zip_csv_entries(path: Path) -> list[str]:
    """
    Return the names of all CSV-like entries inside a ZIP archive, sorted.

    Filters out:
    - Directories
    - macOS resource-fork entries (__MACOSX/)
    - Hidden files (name starts with '.')
    - Non-CSV extensions (.xlsx, .parquet, .json, etc.)

    Sorted alphabetically so entry order is deterministic regardless of
    which zip tool created the archive.
    """
    with zipfile.ZipFile(path, "r") as zf:
        entries: list[str] = []
        for info in zf.infolist():
            name = info.filename
            if info.is_dir():
                continue
            if name.startswith("__MACOSX") or Path(name).name.startswith("."):
                continue
            if Path(name).suffix.lower() in _CSV_EXTENSIONS:
                entries.append(name)
        return sorted(entries)


@contextmanager
def _zip_entry_text(
    zf: zipfile.ZipFile, entry: str, encoding: str
) -> Generator[io.TextIOWrapper, None, None]:
    """
    Context manager that yields a text stream for one ZIP entry.

    Uses detach() on exit so the TextIOWrapper does not close the underlying
    ZipExtFile (which is already managed by the enclosing ZipFile context).
    """
    binary = zf.open(entry)
    fh = io.TextIOWrapper(binary, encoding=encoding, errors="replace", newline="")
    try:
        yield fh
    finally:
        fh.detach()
        binary.close()


def _profile_zip_partition(
    path: Path,
    strategy: SizeStrategy,
    intake: IntakeResult,
    entries: list[str],
) -> tuple[list[RawColumnData], int, bool]:
    """
    Profile a ZIP archive that contains multiple CSV files (shards of one table).

    Strategy:
    - Structure and headers are detected from the first entry (assumed uniform).
    - Row count is the exact sum across all entries (streaming, no materialisation).
    - Sampling spans all entries:
        MEMORY_SAFE → all rows from all entries
        LAZY_SCAN   → single reservoir maintained across all entries
        STREAM_ONLY → skip-interval maintained across all entries
    - Schema is always exact (is_row_count_exact = True) because we stream-count.
    """
    encoding = intake.encoding

    # ── Single ZipFile context for steps A–D ─────────────────────────────────
    with zipfile.ZipFile(path, "r") as zf:
        # Step A: structure from first entry
        first_lines = _zip_read_first_lines(
            zf, entries[0], encoding, settings.CSV_STRUCTURE_PROBE_ROWS + 5
        )
        struct = _detect_structure_from_lines(
            f"{path.name}::{entries[0]}", first_lines, intake.delimiter_hint
        )

        # Step B: headers from first entry
        probe_rows = _zip_parse_rows(
            zf, entries[0], encoding, struct, settings.HEADER_DETECTION_ROWS
        )
        headers, has_header = _detect_headers_from_rows(probe_rows, struct)

        if not headers:
            log.warning("ZIP partition %s: no headers detected", path.name)
            return [], 0, True

        # Steps C+D: count and sample in a single pass across all entries
        total_count, sampled_rows = _zip_count_and_sample(
            zf, entries, encoding, struct, has_header, strategy
        )

    if not sampled_rows:
        log.warning("ZIP partition %s: no rows sampled", path.name)
        return [], total_count, True

    # ── Step E: build RawColumnData ──────────────────────────────────────────
    raw_columns = _build_raw_columns(headers, sampled_rows, total_count)
    return raw_columns, total_count, True


def _zip_read_first_lines(
    zf: zipfile.ZipFile, entry: str, encoding: str, n: int
) -> list[str]:
    lines: list[str] = []
    with _zip_entry_text(zf, entry, encoding) as fh:
        for line in fh:
            stripped = line.rstrip("\r\n")
            if stripped:
                lines.append(stripped)
            if len(lines) >= n:
                break
    return lines


def _zip_parse_rows(
    zf: zipfile.ZipFile,
    entry: str,
    encoding: str,
    struct: _CsvStructure,
    max_rows: int,
) -> list[list[str]]:
    rows: list[list[str]] = []
    with _zip_entry_text(zf, entry, encoding) as fh:
        reader = _make_reader(fh, struct)
        for i, row in enumerate(reader):
            rows.append([cell.strip() for cell in row])
            if i + 1 >= max_rows:
                break
    return rows


def _zip_stream_count(
    zf: zipfile.ZipFile,
    entry: str,
    encoding: str,
    struct: _CsvStructure,
    has_header: bool,
) -> int:
    count = 0
    with _zip_entry_text(zf, entry, encoding) as fh:
        reader = _make_reader(fh, struct)
        if has_header:
            next(reader, None)
        for _ in reader:
            count += 1
    return count


def _zip_sample_entries(
    path: Path,
    entries: list[str],
    encoding: str,
    struct: _CsvStructure,
    has_header: bool,
    strategy: SizeStrategy,
) -> list[list[str]]:
    """Dispatch to the strategy-appropriate sampling function."""
    if strategy == SizeStrategy.MEMORY_SAFE:
        return _zip_read_all(path, entries, encoding, struct, has_header)
    if strategy == SizeStrategy.LAZY_SCAN:
        return _zip_reservoir(path, entries, encoding, struct, has_header)
    return _zip_skip_interval(path, entries, encoding, struct, has_header)


def _zip_read_all(
    path: Path,
    entries: list[str],
    encoding: str,
    struct: _CsvStructure,
    has_header: bool,
) -> list[list[str]]:
    all_rows: list[list[str]] = []
    with zipfile.ZipFile(path, "r") as zf:
        for entry in entries:
            with _zip_entry_text(zf, entry, encoding) as fh:
                reader = _make_reader(fh, struct)
                if has_header:
                    next(reader, None)
                for row in reader:
                    all_rows.append(row)
    return all_rows


def _zip_reservoir(
    path: Path,
    entries: list[str],
    encoding: str,
    struct: _CsvStructure,
    has_header: bool,
) -> list[list[str]]:
    """
    Vitter's Algorithm R across all ZIP entries as one logical stream.
    The reservoir index (global_i) advances continuously across entry boundaries
    so the sample is uniform over the entire partition, not per-shard.
    """
    k       = settings.SAMPLE_ROW_COUNT
    sample: list[list[str]] = []
    rng     = random.Random(42)
    global_i = 0

    with zipfile.ZipFile(path, "r") as zf:
        for entry in entries:
            with _zip_entry_text(zf, entry, encoding) as fh:
                reader = _make_reader(fh, struct)
                if has_header:
                    next(reader, None)
                for row in reader:
                    if global_i < k:
                        sample.append(row)
                    else:
                        j = rng.randint(0, global_i)
                        if j < k:
                            sample[j] = row
                    global_i += 1

    return sample


def _zip_skip_interval(
    path: Path,
    entries: list[str],
    encoding: str,
    struct: _CsvStructure,
    has_header: bool,
) -> list[list[str]]:
    """
    Skip-interval sampling across all ZIP entries as one logical stream.
    The row counter (global_i) advances continuously across entry boundaries
    so the interval is uniform over the entire partition.
    """
    interval = settings.STREAM_SKIP_INTERVAL
    sample: list[list[str]] = []
    global_i = 0

    with zipfile.ZipFile(path, "r") as zf:
        for entry in entries:
            with _zip_entry_text(zf, entry, encoding) as fh:
                reader = _make_reader(fh, struct)
                if has_header:
                    next(reader, None)
                for row in reader:
                    if global_i % interval == 0:
                        sample.append(row)
                    global_i += 1

    return sample


def _zip_count_and_sample(
    zf: zipfile.ZipFile,
    entries: list[str],
    encoding: str,
    struct: _CsvStructure,
    has_header: bool,
    strategy: SizeStrategy,
) -> tuple[int, list[list[str]]]:
    """
    Count rows and collect samples in a SINGLE pass across all ZIP entries.

    Replaces the separate _zip_stream_count + _zip_sample_entries calls
    that previously required two full iterations through the archive.

    Returns:
        (total_row_count, sampled_rows)
    """
    k = settings.SAMPLE_ROW_COUNT
    interval = settings.STREAM_SKIP_INTERVAL
    rng = random.Random(42)
    sample: list[list[str]] = []
    total_count = 0

    for entry in entries:
        with _zip_entry_text(zf, entry, encoding) as fh:
            reader = _make_reader(fh, struct)
            if has_header:
                next(reader, None)
            for row in reader:
                # Sampling
                if strategy == SizeStrategy.MEMORY_SAFE:
                    sample.append(row)
                elif strategy == SizeStrategy.LAZY_SCAN:
                    if total_count < k:
                        sample.append(row)
                    else:
                        j = rng.randint(0, total_count)
                        if j < k:
                            sample[j] = row
                else:  # STREAM_ONLY
                    if total_count % interval == 0:
                        sample.append(row)
                total_count += 1
        log.debug("  %s: counted through entry", entry)

    return total_count, sample


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------

def _open_text(path: Path, intake: IntakeResult):
    """
    Return a text-mode file handle, transparently decompressing gz/zip.
    For zip, opens the first (and assumed only) CSV entry.
    """
    encoding = intake.encoding

    if intake.compression == "gz":
        return gzip.open(path, "rt", encoding=encoding, errors="replace")

    if intake.compression == "zip":
        return _ZipTextWrapper(path, encoding)

    return open(path, "r", encoding=encoding, errors="replace", newline="")


class _ZipTextWrapper:
    """
    Context manager: opens the first CSV-like entry of a zip archive as text.

    Uses _zip_csv_entries() to resolve the entry name so that:
    - macOS resource forks (__MACOSX/) are skipped
    - Non-CSV entries (README.txt, manifest.json) are skipped
    Falls back to namelist()[0] only if no CSV-like entry is found (unusual).
    """

    def __init__(self, path: Path, encoding: str) -> None:
        self._path     = path
        self._encoding = encoding
        self._zf       = None
        self._fh       = None

    def __enter__(self):
        self._zf = zipfile.ZipFile(self._path, "r")
        csv_entries = _zip_csv_entries(self._path)
        entry_name  = csv_entries[0] if csv_entries else self._zf.namelist()[0]
        binary = self._zf.open(entry_name)
        self._fh = io.TextIOWrapper(
            binary, encoding=self._encoding, errors="replace", newline=""
        )
        return self._fh

    def __exit__(self, *_):
        if self._fh:
            self._fh.detach()
        if self._zf:
            self._zf.close()


def _make_reader(fh, struct: _CsvStructure) -> csv.reader:
    kwargs: dict = dict(delimiter=struct.delimiter, quotechar=struct.quotechar)
    if struct.escapechar:
        kwargs["escapechar"] = struct.escapechar
    return csv.reader(fh, **kwargs)


def _read_first_lines(path: Path, intake: IntakeResult, n: int) -> list[str]:
    lines: list[str] = []
    try:
        with _open_text(path, intake) as fh:
            for line in fh:
                stripped = line.rstrip("\r\n")
                if stripped:
                    lines.append(stripped)
                if len(lines) >= n:
                    break
    except Exception as exc:
        log.warning("Could not read first lines from %s: %s", path.name, exc)
    return lines


def _parse_rows(
    path: Path,
    intake: IntakeResult,
    struct: _CsvStructure,
    max_rows: int,
) -> list[list[str]]:
    rows: list[list[str]] = []
    try:
        with _open_text(path, intake) as fh:
            reader = _make_reader(fh, struct)
            for i, row in enumerate(reader):
                rows.append([cell.strip() for cell in row])
                if i + 1 >= max_rows:
                    break
    except Exception as exc:
        log.warning("Row parse error in %s: %s", path.name, exc)
    return rows
