"""
Relationship Report Writer

Serialises a RelationshipReport to JSON.
Reuses serialise() from profile_writer for consistent output formatting.

Entry point:
  write(report: RelationshipReport, output_path: str | Path) -> None
"""

from __future__ import annotations

import json
import logging
import os
import tempfile
from pathlib import Path

from file_profiler.models.relationships import RelationshipReport
from file_profiler.output.profile_writer import serialise

log = logging.getLogger(__name__)


def write(report: RelationshipReport, output_path: str | Path) -> None:
    """
    Serialise a RelationshipReport to JSON and write atomically to output_path.

    Creates parent directories if they do not exist.

    Args:
        report:      Completed RelationshipReport from relationship_detector.detect().
        output_path: Destination path for the JSON file.

    Raises:
        OSError — if the file cannot be written (permissions, disk full, etc.).
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    data = serialise(report)

    tmp_fd, tmp_path = tempfile.mkstemp(
        dir=output_path.parent, suffix=".tmp", prefix=output_path.stem + "_"
    )
    try:
        with os.fdopen(tmp_fd, "w", encoding="utf-8") as fh:
            json.dump(data, fh, indent=2, ensure_ascii=False)
            fh.write("\n")
        os.replace(tmp_path, output_path)
        log.debug("Relationship report written: %s", output_path)
    except Exception:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise
