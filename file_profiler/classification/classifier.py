"""
Layer 2 — File Type Classifier

Entry point:  classify(intake: IntakeResult) -> FileFormat

Uses content sniffing (magic bytes + structure inspection), never file extension.
Takes IntakeResult so it can reuse compression, encoding, and delimiter_hint
already computed by the validator — no double file reads.

Detection order (most specific → most generic):
  1. Parquet  — PAR1 magic at file start AND end
  2. Excel    — OLE2 magic (xls) or XLSX-specific ZIP entries (xlsx)
  3. JSON     — decoded content starts with { or [, or first line is NDJSON
  4. CSV      — delimiter_hint is set and content is consistent tabular text
  5. UNKNOWN  — nothing matched; caller must skip profiling this file
"""

from __future__ import annotations

import json as _json
import logging
import zipfile
from pathlib import Path
from typing import Optional

from file_profiler.intake.validator import IntakeResult
from file_profiler.models.enums import FileFormat

log = logging.getLogger(__name__)

# Number of raw bytes read for magic byte and text structure checks.
_SNIFF_BYTES = 8_192

# Parquet: 4-byte magic at both the start and end of the file.
_PARQUET_MAGIC = b"PAR1"

# XLS (OLE2 Compound Document): first 8 bytes.
_OLE2_MAGIC = b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1"

# XLSX is a ZIP archive; these entries must be present for it to be Excel.
_XLSX_REQUIRED_ENTRIES = {"xl/workbook.xml", "xl/workbook.bin"}
_XLSX_CONTENT_TYPES    = "[Content_Types].xml"

# SQLite: 16-byte magic string at file start.
_SQLITE_MAGIC = b"SQLite format 3\x00"


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def classify(intake: IntakeResult) -> FileFormat:
    """
    Determine the true file format of an already-validated file.

    Args:
        intake: Result from intake.validator.validate() — must be successful.

    Returns:
        FileFormat enum value. Returns UNKNOWN if no format matched;
        the caller is responsible for skipping profiling on UNKNOWN files.
    """
    raw = _read_raw(intake.path)

    if _is_parquet(intake.path, raw, intake.compression):
        log.debug("Classified %s as PARQUET", intake.path.name)
        return FileFormat.PARQUET

    if _is_excel(intake.path, raw, intake.compression):
        log.debug("Classified %s as EXCEL", intake.path.name)
        return FileFormat.EXCEL

    if _is_sqlite(raw):
        log.debug("Classified %s as SQLITE", intake.path.name)
        return FileFormat.SQLITE

    if _is_duckdb(intake.path, raw):
        log.debug("Classified %s as DUCKDB", intake.path.name)
        return FileFormat.DUCKDB

    # Text format detection works on decoded, decompressed content.
    text = _decode_sniff(raw, intake)

    if _is_json(text):
        log.debug("Classified %s as JSON", intake.path.name)
        return FileFormat.JSON

    if _is_csv(text, intake.delimiter_hint):
        log.debug("Classified %s as CSV", intake.path.name)
        return FileFormat.CSV

    log.warning(
        "Could not determine format for %s — flagged as UNKNOWN. "
        "File will be skipped.",
        intake.path.name,
    )
    return FileFormat.UNKNOWN


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _read_raw(path: Path) -> bytes:
    """Read the first _SNIFF_BYTES from the file without any decompression."""
    with open(path, "rb") as fh:
        return fh.read(_SNIFF_BYTES)


def _decode_sniff(raw: bytes, intake: IntakeResult) -> str:
    """
    Return a decoded text snippet for structure-based checks (JSON, CSV).
    If the file is compressed, decompress first so we inspect real content.
    """
    import gzip

    content = raw

    if intake.compression == "gz":
        try:
            with gzip.open(intake.path, "rb") as fh:
                content = fh.read(_SNIFF_BYTES)
        except Exception as exc:
            log.debug("gz decompress failed during classification: %s", exc)

    elif intake.compression == "zip":
        try:
            with zipfile.ZipFile(intake.path, "r") as zf:
                entry = _zip_first_data_entry(zf)
                with zf.open(entry) as fh:
                    content = fh.read(_SNIFF_BYTES)
        except Exception as exc:
            log.debug("zip decompress failed during classification: %s", exc)

    # Strip BOM before decoding so it doesn't interfere with structure checks.
    for bom, _ in [(b"\xef\xbb\xbf", ""), (b"\xff\xfe", ""), (b"\xfe\xff", "")]:
        if content.startswith(bom):
            content = content[len(bom):]
            break

    return content.decode(intake.encoding, errors="replace")


# ---------------------------------------------------------------------------
# Format-specific detectors
# ---------------------------------------------------------------------------

def _zip_first_data_entry(zf: zipfile.ZipFile) -> str:
    """
    Return the first non-metadata entry in a ZIP archive.

    Skips:
    - Directories
    - macOS resource forks (__MACOSX/)
    - Hidden files (name starts with '.')

    Falls back to namelist()[0] if every entry is metadata (unusual).
    """
    for info in zf.infolist():
        name = info.filename
        if info.is_dir():
            continue
        if name.startswith("__MACOSX") or Path(name).name.startswith("."):
            continue
        return name
    return zf.namelist()[0]


def _is_parquet(path: Path, raw: bytes, compression: Optional[str]) -> bool:
    """
    Parquet files carry PAR1 at the first 4 bytes AND the last 4 bytes.
    Both must match — this prevents false positives from files that happen
    to start with PAR1 in their content.

    For compressed files only the start magic is checked (decompressing
    the tail of a large gzip stream is impractical).
    """
    if raw[:4] != _PARQUET_MAGIC:
        return False

    if compression:
        # Can only verify the start magic for compressed files.
        log.debug("Parquet start-magic matched in compressed file %s — accepting.", path.name)
        return True

    try:
        with open(path, "rb") as fh:
            fh.seek(-4, 2)   # 4 bytes from end of file
            tail = fh.read(4)
        if tail != _PARQUET_MAGIC:
            log.debug(
                "PAR1 at start but not at end of %s — not Parquet.", path.name
            )
            return False
        return True
    except OSError as exc:
        log.debug("Could not seek to end of %s: %s", path.name, exc)
        return False


def _is_excel(path: Path, raw: bytes, compression: Optional[str]) -> bool:
    """
    Two Excel variants:
      XLS  — OLE2 Compound Document, detected by 8-byte magic.
      XLSX — ZIP archive containing xl/workbook.xml (or xl/workbook.bin)
             and [Content_Types].xml.

    An XLSX file is a ZIP, so the validator sets compression='zip'.
    We must distinguish an XLSX from a regular ZIP (e.g. a zipped CSV)
    by inspecting the ZIP directory for Excel-specific entries.
    """
    # XLS (OLE2)
    if raw[:8] == _OLE2_MAGIC:
        return True

    # XLSX — only attempt if the file looks like a ZIP archive.
    if compression == "zip" or raw[:4] == b"PK\x03\x04":
        try:
            with zipfile.ZipFile(path, "r") as zf:
                names = set(zf.namelist())
                has_content_types = _XLSX_CONTENT_TYPES in names
                has_workbook      = bool(names & _XLSX_REQUIRED_ENTRIES)
                return has_content_types and has_workbook
        except zipfile.BadZipFile:
            pass
        except Exception as exc:
            log.debug("ZIP inspection failed for %s: %s", path.name, exc)

    return False


def _is_sqlite(raw: bytes) -> bool:
    """SQLite files start with the 16-byte magic string 'SQLite format 3\\0'."""
    return raw[:16] == _SQLITE_MAGIC


def _is_duckdb(path: Path, raw: bytes) -> bool:
    """
    DuckDB files don't have a universally stable magic byte sequence across
    versions, so we use a two-step check:
      1. Extension hint (.duckdb) — since content sniffing alone is unreliable.
      2. Attempt to open with DuckDB in read-only mode — if it succeeds and
         has an information_schema, it's a valid DuckDB database.
    """
    if path.suffix.lower() != ".duckdb":
        return False
    try:
        import duckdb as _duckdb
        con = _duckdb.connect(str(path), read_only=True)
        try:
            con.execute("SELECT 1 FROM information_schema.tables LIMIT 1")
            return True
        finally:
            con.close()
    except Exception:
        return False


def _is_json(text: str) -> bool:
    """
    Three JSON shapes are accepted:
      1. Object   — stripped content starts with {
      2. Array    — stripped content starts with [
      3. NDJSON   — first non-empty line is a valid JSON object
    """
    stripped = text.lstrip()
    if not stripped:
        return False

    # Shape 1 & 2: object or array at root level.
    if stripped[0] in ("{", "["):
        return True

    # Shape 3: NDJSON — validate the first non-empty line.
    for line in stripped.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            parsed = _json.loads(line)
            return isinstance(parsed, dict)
        except _json.JSONDecodeError:
            return False

    return False


def _is_csv(text: str, delimiter_hint: Optional[str]) -> bool:
    """
    This check runs only after Parquet, Excel, and JSON have been ruled out.
    At that point, any clean plain-text content is treated as CSV.

    Single-column files produce no delimiter for csv.Sniffer, so
    delimiter_hint can legitimately be None for a valid CSV.  We do not gate
    on it here; it is kept as a parameter for logging context only.

    The only negative signal is binary content (high null-byte ratio), which
    catches files that slipped past the earlier binary-format detectors.
    """
    if not text.strip():
        return False

    sample = text[:1024]
    null_ratio = sample.count("\x00") / max(len(sample), 1)
    if null_ratio > 0.10:
        log.debug("Content appears binary (null_ratio=%.2f) — not CSV.", null_ratio)
        return False

    return True
