"""
Layer 3 — Size Strategy Selector

Entry point:  select(intake: IntakeResult) -> SizeStrategy

Determines the read strategy before any data is touched, based on the
effective (uncompressed) file size to avoid OOM errors in downstream engines.

For compressed files the on-disk size is not what ends up in memory:
  - ZIP  : uncompressed size is read from the ZIP central directory (accurate).
  - GZIP : uncompressed size is stored in the last 4 bytes (ISIZE field, mod 2^32).
           If ISIZE is 0 (wrapped for files > 4 GB) a 5× safety multiplier is used.
"""

from __future__ import annotations

import logging
import struct
import zipfile
from pathlib import Path
from typing import Optional

from file_profiler.config import settings
from file_profiler.intake.validator import IntakeResult
from file_profiler.models.enums import SizeStrategy

log = logging.getLogger(__name__)

# Conservative expansion factor applied when the true uncompressed gzip size
# cannot be determined (ISIZE wrapped at 4 GB boundary).
_GZIP_EXPANSION_FACTOR = 5


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def select(intake: IntakeResult) -> SizeStrategy:
    """
    Select the appropriate read strategy for the file described by intake.

    Args:
        intake: Successful result from intake.validator.validate().

    Returns:
        SizeStrategy enum value to be passed to all downstream engines.
    """
    effective_bytes = effective_size(intake)
    strategy = _from_bytes(effective_bytes)

    log.debug(
        "%s  on_disk=%s  effective=%s  strategy=%s",
        intake.path.name,
        _human(intake.size_bytes),
        _human(effective_bytes),
        strategy.value,
    )
    return strategy


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def effective_size(intake: IntakeResult) -> int:
    """
    Return the size in bytes that will be relevant to memory usage.
    For uncompressed files this is just the file size.
    For compressed files we resolve the uncompressed size.

    Public so the CSV engine can reuse this for accurate row-count extrapolation.
    """
    if intake.compression == "zip":
        return _zip_uncompressed_size(intake.path, intake.size_bytes)

    if intake.compression == "gz":
        return _gz_uncompressed_size(intake.path, intake.size_bytes)

    return intake.size_bytes


def _from_bytes(size: int) -> SizeStrategy:
    """Pure size → strategy mapping. Isolated for direct testing."""
    if size < settings.MEMORY_SAFE_MAX_BYTES:
        return SizeStrategy.MEMORY_SAFE
    if size < settings.LAZY_SCAN_MAX_BYTES:
        return SizeStrategy.LAZY_SCAN
    return SizeStrategy.STREAM_ONLY


def _zip_uncompressed_size(path: Path, fallback: int) -> int:
    """
    Sum the uncompressed sizes of all entries in a ZIP archive.
    Falls back to compressed size if the ZIP cannot be read.
    """
    try:
        with zipfile.ZipFile(path, "r") as zf:
            total = sum(info.file_size for info in zf.infolist())
        return total if total > 0 else fallback
    except Exception as exc:
        log.warning("Could not read ZIP directory for %s: %s — using compressed size.", path.name, exc)
        return fallback


def _gz_uncompressed_size(path: Path, compressed_size: int) -> int:
    """
    Read the ISIZE field from the last 4 bytes of a gzip file.
    ISIZE is the uncompressed size modulo 2^32.

    If ISIZE is 0 the file is either empty or > 4 GB (wrapped);
    in that case apply a conservative expansion multiplier.
    """
    try:
        with open(path, "rb") as fh:
            fh.seek(-4, 2)
            isize = struct.unpack("<I", fh.read(4))[0]   # little-endian uint32

        if isize > 0:
            return isize

        # ISIZE == 0: either empty (caught in intake) or > 4 GB wrap-around.
        log.debug(
            "ISIZE=0 for %s — applying %dx expansion factor.", path.name, _GZIP_EXPANSION_FACTOR
        )
        return compressed_size * _GZIP_EXPANSION_FACTOR

    except Exception as exc:
        log.warning(
            "Could not read ISIZE from %s: %s — applying expansion factor.", path.name, exc
        )
        return compressed_size * _GZIP_EXPANSION_FACTOR


def _human(size: int) -> str:
    """Format a byte count as a human-readable string for log messages."""
    for unit in ("B", "KB", "MB", "GB"):
        if size < 1024:
            return f"{size:.1f} {unit}"
        size //= 1024
    return f"{size:.1f} TB"
