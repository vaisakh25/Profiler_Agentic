"""
Layer 8 — Structural Quality Checker

Runs after column profiling. Examines the completed ColumnProfiles for
file-specific structural issues that do not exist in well-managed databases.

Per-column quality flags emitted (appended to ColumnProfile.quality_flags):
  DUPLICATE_COLUMN_NAME  — two or more columns share the same header name
  FULLY_NULL             — every sampled value in the column is null
  CONSTANT_COLUMN        — only one distinct non-null value across all samples
  HIGH_NULL_RATIO        — null_count / total_count > NULL_HEAVY_THRESHOLD

File-level structural issues (returned as plain strings for
FileProfile.structural_issues):
  Column shift description  — when corrupt_row_count > 0
  Encoding inconsistency    — when intake fell back to latin-1

Entry point:
  check(profiles, corrupt_row_count, encoding)
      -> tuple[list[ColumnProfile], list[str]]
"""

from __future__ import annotations

import logging
from collections import Counter

from file_profiler.models.enums import QualityFlag
from file_profiler.models.file_profile import ColumnProfile
from file_profiler.observability.langsmith import compact_text_output, traceable

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

@traceable(
    name="layer.quality.check",
    run_type="chain",
    process_outputs=compact_text_output,
)
def check(
    profiles: list[ColumnProfile],
    corrupt_row_count: int = 0,
    encoding: str = "utf-8",
) -> tuple[list[ColumnProfile], list[str]]:
    """
    Audit all column profiles for structural quality issues.

    Args:
        profiles:          ColumnProfiles from the column profiling engine.
        corrupt_row_count: Rows whose field count did not match the header
                           (reported by the format engine; 0 for non-CSV).
        encoding:          File encoding from intake. "latin-1" signals that
                           UTF-8 decoding failed and a fallback was used.

    Returns:
        (profiles, structural_issues)
        profiles          — same list, quality_flags populated in-place.
        structural_issues — file-level issue strings for FileProfile.structural_issues.
    """
    structural_issues: list[str] = []

    _flag_duplicate_names(profiles)
    _flag_column_nullness(profiles)
    _report_shift_errors(corrupt_row_count, structural_issues)
    _report_encoding_issues(encoding, structural_issues)

    return profiles, structural_issues


# ---------------------------------------------------------------------------
# Per-column checks
# ---------------------------------------------------------------------------

def _flag_duplicate_names(profiles: list[ColumnProfile]) -> None:
    """
    Add DUPLICATE_COLUMN_NAME to every column whose name appears more than once.

    Duplicate names are a structural problem — downstream joins and exports
    that reference columns by name will silently pick the wrong one.
    """
    name_counts = Counter(p.name for p in profiles)
    for p in profiles:
        if name_counts[p.name] > 1:
            _add_flag(p, QualityFlag.DUPLICATE_COLUMN_NAME)
            log.debug("Duplicate column name detected: %r", p.name)


def _flag_column_nullness(profiles: list[ColumnProfile]) -> None:
    """
    Add per-column quality flags derivable from the ColumnProfile statistics.

    The column profiler already computes is_constant and is_sparse as boolean
    fields.  This function promotes them into quality_flags so they surface
    in the unified JSON output alongside flags from type inference.
    """
    for p in profiles:
        # FULLY_NULL: no non-null values were seen in the sample.
        # distinct_count == 0 with null_count > 0 is the reliable indicator
        # (inferred_type == NULL_ONLY is equivalent but couples us to the enum).
        if p.distinct_count == 0 and p.null_count > 0:
            _add_flag(p, QualityFlag.FULLY_NULL)
            log.debug("Fully null column: %r (%d nulls)", p.name, p.null_count)

        # CONSTANT_COLUMN: computed by column_profiler (distinct_count == 1
        # and at least one non-null value).
        if p.is_constant:
            _add_flag(p, QualityFlag.CONSTANT_COLUMN)
            log.debug("Constant column: %r", p.name)

        # HIGH_NULL_RATIO: computed by column_profiler
        # (null_count / total_count > NULL_HEAVY_THRESHOLD).
        if p.is_sparse:
            _add_flag(p, QualityFlag.HIGH_NULL_RATIO)
            log.debug(
                "High null ratio: %r (%d nulls)", p.name, p.null_count
            )


# ---------------------------------------------------------------------------
# File-level checks
# ---------------------------------------------------------------------------

def _report_shift_errors(corrupt_row_count: int, issues: list[str]) -> None:
    """
    Report column shift errors at the file level.

    A shift error means a row had fewer or more fields than the header, so
    values in that row may have been assigned to the wrong columns.  The exact
    set of affected columns cannot be determined without re-scanning, so this
    is a file-level issue rather than a per-column flag.
    """
    if corrupt_row_count > 0:
        msg = (
            f"COLUMN_SHIFT_ERROR: {corrupt_row_count} row(s) had a field count "
            f"that did not match the header. Values in those rows may be "
            f"misaligned across columns."
        )
        issues.append(msg)
        log.warning(msg)


def _report_encoding_issues(encoding: str, issues: list[str]) -> None:
    """
    Report a suspected encoding inconsistency.

    In this system latin-1 is the final-resort fallback codec — it is only
    used when UTF-8 decoding failed on the sniff sample.  That failure is a
    strong signal that the file contains mixed or misdeclared encodings.
    """
    if encoding == "binary":
        return  # Binary formats (Parquet, Excel) — encoding check not applicable
    normalised = encoding.lower().replace("-", "").replace("_", "")
    if normalised in ("latin1", "iso88591"):
        msg = (
            "ENCODING_INCONSISTENCY: file was decoded as latin-1 because UTF-8 "
            "decoding failed. Column values may contain mojibake characters."
        )
        issues.append(msg)
        log.warning(msg)


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------

def _add_flag(profile: ColumnProfile, flag: QualityFlag) -> None:
    """Append flag only if not already present (idempotent)."""
    if flag not in profile.quality_flags:
        profile.quality_flags.append(flag)
