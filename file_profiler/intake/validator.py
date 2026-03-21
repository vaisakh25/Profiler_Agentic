"""
Layer 1 — File Intake Validator

Entry point:  validate(path) -> IntakeResult

All downstream layers (classifier, size strategy, engines) require a successful
IntakeResult before they may be invoked. This module raises on hard failures and
logs + falls back on soft failures (encoding uncertainty, no delimiter found).
"""

from __future__ import annotations

import csv as _csv
import gzip
import io
import logging
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

from file_profiler.config import settings
from file_profiler.intake.errors import (
    EmptyFileError,
    UnsupportedEncodingError,
)

try:
    import chardet as _chardet
    _CHARDET_AVAILABLE = True
except ImportError:
    _CHARDET_AVAILABLE = False

log = logging.getLogger(__name__)

# Bytes read from the file for all sniffing operations.
_SNIFF_BYTES = 8_192

# BOM signatures checked in order — longest first to avoid partial matches.
_BOMS: list[tuple[bytes, str]] = [
    (b"\xef\xbb\xbf", "utf-8-sig"),   # UTF-8 with BOM
    (b"\xff\xfe",     "utf-16-le"),    # UTF-16 little-endian
    (b"\xfe\xff",     "utf-16-be"),    # UTF-16 big-endian
]

# Minimum chardet confidence to trust its result; below this, fall back to UTF-8.
_CHARDET_MIN_CONFIDENCE = 0.75

# Magic bytes for compression formats.
_GZIP_MAGIC = b"\x1f\x8b"
_ZIP_MAGIC  = b"PK\x03\x04"

# Parquet magic bytes: "PAR1" at file start.
_PARQUET_MAGIC = b"PAR1"

# File extensions that are self-describing binary formats — encoding and
# delimiter detection should be skipped entirely for these.
_BINARY_EXTENSIONS = frozenset({
    ".parquet", ".pq", ".parq",
    ".xlsx", ".xls",
    ".duckdb", ".db", ".sqlite", ".sqlite3",
})


# ---------------------------------------------------------------------------
# Public result type
# ---------------------------------------------------------------------------

@dataclass
class IntakeResult:
    """
    Outcome of a successful validation pass.
    Passed to every downstream layer — no layer should re-open the file
    without consulting this first.
    """
    path:           Path
    size_bytes:     int
    encoding:       str
    is_bom_present: bool
    bom_encoding:   Optional[str]   # e.g. 'utf-8-sig', 'utf-16-le'; None if no BOM
    compression:    Optional[str]   # 'gz', 'zip', or None
    delimiter_hint: Optional[str]   # best-guess CSV delimiter; None if not determinable


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def validate(path: str | Path) -> IntakeResult:
    """
    Validate that a file is readable and well-formed.

    Raises:
        FileNotFoundError       — path does not exist or is not a file
        EmptyFileError          — file exists but is 0 bytes
        UnsupportedEncodingError — sniff sample cannot be decoded by any codec

    Returns:
        IntakeResult with encoding, compression, and delimiter metadata.
    """
    path = Path(path).resolve()

    _check_exists(path)

    size_bytes = _check_size(path)

    # Read the sniff window once; all subsequent checks work from this buffer.
    raw_header = _read_raw_bytes(path)

    compression = _detect_compression(raw_header)

    # Binary formats (Parquet, Excel) are self-describing — skip encoding
    # and delimiter detection to avoid false positives.
    if path.suffix.lower() in _BINARY_EXTENSIONS or raw_header[:4] == _PARQUET_MAGIC:
        return IntakeResult(
            path=path,
            size_bytes=size_bytes,
            encoding="binary",
            is_bom_present=False,
            bom_encoding=None,
            compression=compression,
            delimiter_hint=None,
        )

    # If compressed, decompress the sniff window so encoding detection works
    # on actual content bytes, not the gzip/zip wrapper.
    sniff_bytes = _get_sniff_bytes(path, compression)

    bom_encoding, is_bom_present, sniff_no_bom = _detect_bom(sniff_bytes)

    encoding = _detect_encoding(sniff_no_bom, bom_encoding, path)

    delimiter_hint = _sniff_delimiter(sniff_no_bom, encoding)

    return IntakeResult(
        path=path,
        size_bytes=size_bytes,
        encoding=encoding,
        is_bom_present=is_bom_present,
        bom_encoding=bom_encoding,
        compression=compression,
        delimiter_hint=delimiter_hint,
    )


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _check_exists(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"File not found: {path}")
    if not path.is_file():
        raise FileNotFoundError(f"Path exists but is not a file: {path}")


def _check_size(path: Path) -> int:
    size = path.stat().st_size
    if size == 0:
        raise EmptyFileError(f"File is empty (0 bytes): {path}")
    return size


def _read_raw_bytes(path: Path) -> bytes:
    with open(path, "rb") as fh:
        return fh.read(_SNIFF_BYTES)


def _detect_compression(header: bytes) -> Optional[str]:
    if header[:2] == _GZIP_MAGIC:
        return "gz"
    if header[:4] == _ZIP_MAGIC:
        return "zip"
    return None


def _get_sniff_bytes(path: Path, compression: Optional[str]) -> bytes:
    """
    Return the first _SNIFF_BYTES bytes of actual file content,
    decompressing if necessary so encoding detection works on real data.
    """
    try:
        if compression == "gz":
            with gzip.open(path, "rb") as fh:
                return fh.read(_SNIFF_BYTES)
        if compression == "zip":
            with zipfile.ZipFile(path, "r") as zf:
                first_entry = zf.namelist()[0]
                with zf.open(first_entry) as fh:
                    return fh.read(_SNIFF_BYTES)
    except Exception as exc:
        # Decompression failed — fall back to raw bytes and let later layers handle it.
        log.warning("Decompression sniff failed for %s (%s): %s", path.name, compression, exc)

    with open(path, "rb") as fh:
        return fh.read(_SNIFF_BYTES)


def _detect_bom(raw: bytes) -> tuple[Optional[str], bool, bytes]:
    """
    Check for a BOM at the start of the byte stream.

    Returns:
        (bom_encoding, is_bom_present, raw_without_bom)
    """
    for signature, encoding_name in _BOMS:
        if raw.startswith(signature):
            log.debug("BOM detected: %s", encoding_name)
            return encoding_name, True, raw[len(signature):]
    return None, False, raw


def _detect_encoding(raw: bytes, bom_encoding: Optional[str], path: Path) -> str:
    """
    Determine the file encoding.

    Priority:
      1. BOM (authoritative)
      2. chardet (if available and confident)
      3. UTF-8 probe
      4. Latin-1 fallback (never raises — every byte is valid Latin-1)

    Raises:
        UnsupportedEncodingError — only if every codec attempt raises a decode error
        on the sniff sample and Latin-1 is somehow unavailable (practically impossible).
    """
    if bom_encoding:
        return bom_encoding

    if _CHARDET_AVAILABLE:
        result = _chardet.detect(raw)
        detected = result.get("encoding")
        confidence = result.get("confidence", 0.0)
        if detected and confidence >= _CHARDET_MIN_CONFIDENCE:
            log.debug(
                "chardet: %s (confidence %.2f) for %s", detected, confidence, path.name
            )
            return detected
        log.warning(
            "chardet low confidence (%.2f) for %s — trying UTF-8 fallback",
            confidence,
            path.name,
        )

    # UTF-8 probe — strict decode of the sniff sample.
    try:
        raw.decode("utf-8")
        return "utf-8"
    except UnicodeDecodeError:
        pass

    # Latin-1 never raises — every byte maps to a valid character.
    # Log so the caller knows encoding is uncertain.
    log.warning(
        "UTF-8 decode failed for %s — falling back to latin-1. "
        "Column values may contain mojibake.",
        path.name,
    )

    # Final check: if even latin-1 fails (should never happen), raise.
    try:
        raw.decode("latin-1")
        return "latin-1"
    except UnicodeDecodeError as exc:
        raise UnsupportedEncodingError(
            f"Cannot decode sniff sample for {path}: {exc}"
        ) from exc


def _sniff_delimiter(raw: bytes, encoding: str) -> Optional[str]:
    """
    Best-guess the CSV delimiter from the sniff sample.
    Returns None silently if detection fails — this is a hint, not a requirement.
    """
    try:
        text = raw.decode(encoding, errors="replace")
        first_lines = "\n".join(text.splitlines()[:10])
        if not first_lines.strip():
            return None
        dialect = _csv.Sniffer().sniff(
            first_lines,
            delimiters="".join(settings.CSV_CANDIDATE_DELIMITERS),
        )
        return dialect.delimiter
    except _csv.Error:
        return None
    except Exception as exc:
        log.debug("Delimiter sniff failed: %s", exc)
        return None
