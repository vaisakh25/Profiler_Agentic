"""
Standardization Layer — Column Name & Value Normalization

Entry point:  standardize(columns) -> tuple[list[RawColumnData], StandardizationReport]

Sits between format engines (Layer 4/5) and the column profiler (Layer 7).
Produces NEW RawColumnData objects with cleaned names and values; originals
are never mutated.

Operations (applied in order per value):
  1. Column name normalization  (lowercase snake_case, dedup)
  2. Whitespace trimming        (leading/trailing)
  3. Null sentinel normalization (various null-like strings -> None)
  4. Boolean token normalization (Yes/No/Y/N/… -> true/false)
     — only when >= 90 % of non-null values are boolean tokens
  5. Numeric surface cleaning    (strip $, €, £, ¥, %, grouping commas)

Idempotent: running standardize() twice produces identical output.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Optional

from file_profiler.config import settings
from file_profiler.models.file_profile import RawColumnData

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Report dataclasses
# ---------------------------------------------------------------------------

@dataclass
class ColumnStandardizationDetail:
    """Per-column record of what the standardizer changed."""
    original_name:        str
    standardized_name:    str
    name_changed:         bool
    nulls_normalized:     int = 0   # sentinel values converted to None
    whitespace_trimmed:   int = 0   # values that had leading/trailing whitespace
    booleans_normalized:  int = 0   # boolean variants normalised to true/false
    numerics_cleaned:     int = 0   # values with currency/percent/comma stripped


@dataclass
class StandardizationReport:
    """Summary of all standardization actions for quality reporting."""
    columns_renamed:          int = 0
    total_nulls_normalized:   int = 0
    total_whitespace_trimmed: int = 0
    total_booleans_normalized: int = 0
    total_numerics_cleaned:   int = 0
    details: list[ColumnStandardizationDetail] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Regex helpers for name normalization
_RE_NON_ALNUM       = re.compile(r"[^a-z0-9_]")
_RE_MULTI_UNDERSCORE = re.compile(r"_{2,}")
_RE_LEADING_DIGIT    = re.compile(r"^(\d)")

# Boolean normalisation map (lowercase key -> canonical form).
# Kept in sync with type_inference._BOOL_TOKENS.
_BOOL_NORMALIZE: dict[str, str] = {
    "true": "true", "false": "false",
    "yes": "true",  "no": "false",
    "y": "true",    "n": "false",
    "t": "true",    "f": "false",
    "1": "true",    "0": "false",
}

# Minimum fraction of non-null values that must be boolean tokens
# before boolean normalization is applied to a column.
_BOOL_COLUMN_THRESHOLD: float = 0.90

# Numeric cleaning patterns
_RE_CURRENCY       = re.compile(r"^[\$\u20ac\u00a3\u00a5]")   # $ € £ ¥
_RE_PERCENT        = re.compile(r"%$")
_RE_GROUPING_COMMA = re.compile(r"(?<=\d),(?=\d{3})")

# Null sentinels are loaded from settings at call time (not module level)
# so that tests can override settings.NULL_SENTINEL_VALUES.


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def standardize(
    columns: list[RawColumnData],
) -> tuple[list[RawColumnData], StandardizationReport]:
    """
    Standardize column names and values across all columns.

    Returns a NEW list of RawColumnData (original list is not mutated)
    and a StandardizationReport summarizing all changes.

    Args:
        columns: Raw column data from a format engine.

    Returns:
        (standardized_columns, report)
    """
    report = StandardizationReport()

    if not columns:
        return [], report

    # Phase 1 — standardize column names
    name_map, collision_set = _build_name_map([col.name for col in columns])

    # Phase 2 — standardize values per column
    result: list[RawColumnData] = []
    for col in columns:
        new_name = name_map[col.name]
        new_values, detail = _standardize_values(
            col.values,
            original_name=col.name,
            standardized_name=new_name,
        )

        # Adjust null_count: original + newly normalised nulls
        new_null_count = col.null_count + detail.nulls_normalized

        result.append(RawColumnData(
            name=new_name,
            declared_type=col.declared_type,
            values=new_values,
            total_count=col.total_count,
            null_count=new_null_count,
            type_inference=col.type_inference,
        ))

        report.details.append(detail)
        if detail.name_changed:
            report.columns_renamed += 1
        report.total_nulls_normalized   += detail.nulls_normalized
        report.total_whitespace_trimmed += detail.whitespace_trimmed
        report.total_booleans_normalized += detail.booleans_normalized
        report.total_numerics_cleaned   += detail.numerics_cleaned

    log.debug(
        "Standardization complete: %d col(s) renamed, %d null(s) normalised, "
        "%d ws trimmed, %d bool(s), %d numeric(s).",
        report.columns_renamed,
        report.total_nulls_normalized,
        report.total_whitespace_trimmed,
        report.total_booleans_normalized,
        report.total_numerics_cleaned,
    )

    return result, report


# ---------------------------------------------------------------------------
# Column name helpers
# ---------------------------------------------------------------------------

def _normalize_name(name: str) -> str:
    """
    Convert a column name to lowercase snake_case.

    Rules:
      1. Strip leading/trailing whitespace
      2. Lowercase
      3. Replace non-alphanumeric chars with underscore
      4. Collapse consecutive underscores
      5. Strip leading/trailing underscores
      6. If name starts with a digit, prefix with ``col_``
      7. If empty after normalization, return ``unnamed``
    """
    result = name.strip().lower()
    result = _RE_NON_ALNUM.sub("_", result)
    result = _RE_MULTI_UNDERSCORE.sub("_", result)
    result = result.strip("_")
    if not result:
        return "unnamed"
    if _RE_LEADING_DIGIT.match(result):
        result = f"col_{result}"
    return result


def _build_name_map(
    original_names: list[str],
) -> tuple[dict[str, str], set[str]]:
    """
    Build a mapping from original name -> standardized name, handling
    duplicates by appending ``_2``, ``_3``, etc.

    Returns:
        (mapping, collision_set)

        mapping:       {original_name: standardized_name}
        collision_set: set of normalised names that had collisions
    """
    seen: dict[str, int] = {}          # normalised -> count
    collision_set: set[str] = set()
    mapping: dict[str, str] = {}

    for orig in original_names:
        norm = _normalize_name(orig)
        count = seen.get(norm, 0) + 1
        seen[norm] = count
        if count == 1:
            mapping[orig] = norm
        else:
            mapping[orig] = f"{norm}_{count}"
            collision_set.add(norm)

    return mapping, collision_set


# ---------------------------------------------------------------------------
# Value helpers
# ---------------------------------------------------------------------------

def _standardize_values(
    values: list[Optional[str]],
    *,
    original_name: str,
    standardized_name: str,
) -> tuple[list[Optional[str]], ColumnStandardizationDetail]:
    """
    Apply value-level standardization to a single column's sampled values.

    Returns (new_values, detail).
    """
    null_sentinels = frozenset(settings.NULL_SENTINEL_VALUES)

    # Pre-scan: determine if this column is predominantly boolean
    is_boolean_column = _is_boolean_column(values)

    new_values: list[Optional[str]] = []
    nulls_normalized    = 0
    whitespace_trimmed  = 0
    booleans_normalized = 0
    numerics_cleaned    = 0

    for val in values:
        if val is None:
            new_values.append(None)
            continue

        original_val = val

        # Step 1 — whitespace trimming (always)
        val = val.strip()
        if val != original_val:
            whitespace_trimmed += 1

        # Step 2 — empty after trim → None
        if val == "":
            new_values.append(None)
            nulls_normalized += 1
            continue

        # Step 3 — null sentinel check
        if val.lower() in null_sentinels:
            new_values.append(None)
            nulls_normalized += 1
            continue

        # Step 4 — boolean normalization (only for boolean-dominant columns)
        if is_boolean_column:
            lower = val.lower()
            canonical = _BOOL_NORMALIZE.get(lower)
            if canonical is not None and canonical != val:
                booleans_normalized += 1
                val = canonical

        # Step 5 — numeric surface cleaning
        cleaned = _clean_numeric(val)
        if cleaned != val:
            numerics_cleaned += 1
            val = cleaned

        new_values.append(val)

    detail = ColumnStandardizationDetail(
        original_name=original_name,
        standardized_name=standardized_name,
        name_changed=(original_name != standardized_name),
        nulls_normalized=nulls_normalized,
        whitespace_trimmed=whitespace_trimmed,
        booleans_normalized=booleans_normalized,
        numerics_cleaned=numerics_cleaned,
    )

    return new_values, detail


def _is_boolean_column(values: list[Optional[str]]) -> bool:
    """
    Return True if >= 90 % of non-null, non-empty values are boolean tokens.
    """
    non_null = [v.strip().lower() for v in values if v is not None and v.strip()]
    if not non_null:
        return False
    bool_count = sum(1 for v in non_null if v in _BOOL_NORMALIZE)
    return (bool_count / len(non_null)) >= _BOOL_COLUMN_THRESHOLD


def _clean_numeric(val: str) -> str:
    """
    Strip currency symbols, grouping commas, and percent signs from a value
    that appears to be numeric.

    Only accepts the cleaning if the result still contains at least one digit
    (avoids mangling non-numeric strings like ``$ales``).
    """
    candidate = val
    candidate = _RE_CURRENCY.sub("", candidate)
    candidate = _RE_PERCENT.sub("", candidate)
    candidate = _RE_GROUPING_COMMA.sub("", candidate)
    candidate = candidate.strip()

    if candidate != val and any(c.isdigit() for c in candidate):
        return candidate
    return val
