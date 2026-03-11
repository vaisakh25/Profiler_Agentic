"""
Layer 11 — Unified Profile Writer

Serialises a completed FileProfile to the unified JSON output schema.
Output is format-agnostic — identical structure regardless of whether the
source was CSV, Parquet, JSON, or Excel.

Responsibilities:
  - Compute and attach QualitySummary before writing.
  - Write atomically: write to a temp file, then os.replace → no partial writes.
  - Serialise all dataclass fields, converting enums to their string values.

Entry point:
  write(profile: FileProfile, output_path: str | Path) -> None
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from dataclasses import fields, is_dataclass
from enum import Enum
from pathlib import Path

from file_profiler.models.enums import QualityFlag
from file_profiler.models.file_profile import FileProfile, QualitySummary

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def write(profile: FileProfile, output_path: str | Path) -> None:
    """
    Serialise profile to JSON and write atomically to output_path.

    Attaches a freshly computed QualitySummary to the profile before writing.
    Creates output_path's parent directories if they do not exist.

    Args:
        profile:     Completed FileProfile (columns must already be populated).
        output_path: Destination path for the JSON file.

    Raises:
        OSError — if the file cannot be written (permissions, disk full, etc.).
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Recompute quality summary from the final column profiles.
    profile.quality_summary = compute_quality_summary(profile)

    data = serialise(profile)

    # Atomic write: write to a sibling temp file, then rename.
    tmp_fd, tmp_path = tempfile.mkstemp(
        dir=output_path.parent, suffix=".tmp", prefix=output_path.stem + "_"
    )
    try:
        with os.fdopen(tmp_fd, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2, ensure_ascii=False)
            fh.write("\n")   # trailing newline — friendly for diffs
        os.replace(tmp_path, output_path)
        log.debug("Profile written: %s", output_path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


# ---------------------------------------------------------------------------
# Quality summary
# ---------------------------------------------------------------------------

def compute_quality_summary(profile: FileProfile) -> QualitySummary:
    """
    Derive aggregate quality metrics from the column profiles.

    Called immediately before serialisation so the summary always reflects
    the final state of the profiles (including structural checker flags).
    """
    cols = profile.columns

    columns_with_issues   = sum(1 for c in cols if c.quality_flags)
    null_heavy_columns    = sum(
        1 for c in cols
        if QualityFlag.HIGH_NULL_RATIO in c.quality_flags
        or QualityFlag.FULLY_NULL      in c.quality_flags
    )
    type_conflict_columns = sum(
        1 for c in cols if QualityFlag.TYPE_CONFLICT in c.quality_flags
    )

    return QualitySummary(
        columns_profiled      = len(cols),
        columns_with_issues   = columns_with_issues,
        null_heavy_columns    = null_heavy_columns,
        type_conflict_columns = type_conflict_columns,
        corrupt_rows_detected = profile.corrupt_row_count,
    )


# ---------------------------------------------------------------------------
# Serialisation
# ---------------------------------------------------------------------------

def serialise(obj) -> object:
    """
    Recursively convert an object to a JSON-serialisable form.

    Rules:
    - None, bool, int, float, str  → unchanged
    - Enum                         → .value  (string)
    - dataclass instance           → dict of {field_name: serialised_value}
    - list                         → [serialised_item, ...]
    - Anything else                → unchanged (let json.dump raise if needed)
    """
    if obj is None:
        return None
    if isinstance(obj, bool):          # must precede int (bool subclasses int)
        return obj
    if isinstance(obj, (int, float, str)):
        return obj
    if isinstance(obj, Enum):
        return obj.value
    if isinstance(obj, list):
        return [serialise(item) for item in obj]
    if is_dataclass(obj):
        return {f.name: serialise(getattr(obj, f.name)) for f in fields(obj)}
    return obj
