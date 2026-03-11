"""
Type Inference Engine

Entry point:  infer(values) -> TypeInferenceResult

Determines the most specific type that fits a column's sampled values.
Used by the CSV engine (Step E) and JSON engine (schema discovery).
Parquet and Excel engines skip this — they have declared types.

Detection order (most specific to least):
  1.  NULL_ONLY   — every value is null / empty
  2.  BOOLEAN     — all non-null values are true/false/yes/no/1/0
                    (checked before INTEGER so 1/0 columns are not mistyped)
  3.  DATE        — all non-null values parse as calendar dates
                    (checked before INTEGER so YYYYMMDD is not mistyped as integer)
  4.  TIMESTAMP   — all non-null values parse as datetime with time component
  5.  INTEGER     — all non-null values are whole numbers (leading-zero guard)
  6.  FLOAT       — all non-null values are decimals or scientific notation
  7.  UUID        — all non-null values match UUID 8-4-4-4-12 hex format
  8.  CATEGORICAL — distinct value count is below CATEGORICAL_MAX_DISTINCT
  9.  FREE_TEXT   — average string length exceeds FREE_TEXT_MIN_AVG_LENGTH
  10. STRING      — default fallback

A type is accepted when at least _CONFIDENCE_THRESHOLD of non-null values match.
The returned confidence_score is the exact match ratio (e.g. 0.97).
"""

from __future__ import annotations

import re
import logging
from datetime import datetime
from typing import Optional

from file_profiler.config import settings
from file_profiler.models.enums import InferredType, QualityFlag
from file_profiler.models.file_profile import TypeInferenceResult

log = logging.getLogger(__name__)

# Minimum fraction of non-null values that must match a candidate type.
# Values below this cause the detector to fall through to the next type.
_CONFIDENCE_THRESHOLD = 0.90


# ---------------------------------------------------------------------------
# Compiled patterns
# ---------------------------------------------------------------------------

# INTEGER: optional leading minus, then either "0" or a non-zero digit followed
# by more digits.  Deliberately excludes leading zeros (00123 = zip code / ID).
_RE_INT = re.compile(r"^-?(?:0|[1-9]\d*)$")

# FLOAT: decimal point OR scientific notation (with or without decimal point).
# Comma-stripped values (1,000 → 1000) are checked before this pattern.
# Matches: 3.14 | .5 | 1e10 | 2.5e-3 | 1E10 (but NOT plain integers like 42)
_RE_FLOAT = re.compile(
    r"^-?(?:"
    r"(?:\d+\.\d*|\.\d+)(?:[eE][+-]?\d+)?"   # decimal, optional sci-notation
    r"|"
    r"\d+[eE][+-]?\d+"                         # integer base with sci-notation (e.g. 1e10)
    r")$"
)

# UUID: standard 8-4-4-4-12 hex, case-insensitive.
_RE_UUID = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}"
    r"-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)

# Boolean accepted tokens (compare after lower-casing the value).
_BOOL_TOKENS = frozenset({"true", "false", "yes", "no", "1", "0", "t", "f", "y", "n"})

# Date formats: (strptime_format, human_readable_label)
# Checked in order — more specific formats first.
_DATE_FORMATS: list[tuple[str, str]] = [
    ("%Y-%m-%d",  "YYYY-MM-DD"),
    ("%Y%m%d",    "YYYYMMDD"),
    ("%d/%m/%Y",  "DD/MM/YYYY"),
    ("%m/%d/%Y",  "MM/DD/YYYY"),
    ("%d-%m-%Y",  "DD-MM-YYYY"),
    ("%m-%d-%Y",  "MM-DD-YYYY"),
]

# Timestamp formats: (strptime_format, human_readable_label, is_tz_aware)
# ISO-8601 variants first, then locale variants (DD/MM, MM/DD, DD-MM, MM-DD).
_TIMESTAMP_FORMATS: list[tuple[str, str, bool]] = [
    # ISO-8601 with timezone
    ("%Y-%m-%dT%H:%M:%S%z",       "YYYY-MM-DDTHH:MM:SS+TZ",        True),
    ("%Y-%m-%dT%H:%M:%S.%f%z",    "YYYY-MM-DDTHH:MM:SS.ffffff+TZ", True),
    ("%Y-%m-%dT%H:%M:%SZ",        "YYYY-MM-DDTHH:MM:SSZ",          True),
    # ISO-8601 naive
    ("%Y-%m-%dT%H:%M:%S",         "YYYY-MM-DDTHH:MM:SS",           False),
    ("%Y-%m-%dT%H:%M:%S.%f",      "YYYY-MM-DDTHH:MM:SS.ffffff",    False),
    ("%Y-%m-%d %H:%M:%S",         "YYYY-MM-DD HH:MM:SS",           False),
    ("%Y-%m-%d %H:%M:%S.%f",      "YYYY-MM-DD HH:MM:SS.ffffff",    False),
    # Slash-separated
    ("%d/%m/%Y %H:%M:%S",         "DD/MM/YYYY HH:MM:SS",           False),
    ("%m/%d/%Y %H:%M:%S",         "MM/DD/YYYY HH:MM:SS",           False),
    ("%d/%m/%Y %H:%M:%S.%f",      "DD/MM/YYYY HH:MM:SS.ffffff",    False),
    ("%m/%d/%Y %H:%M:%S.%f",      "MM/DD/YYYY HH:MM:SS.ffffff",    False),
    # Dash-separated (non-ISO order)
    ("%d-%m-%Y %H:%M:%S",         "DD-MM-YYYY HH:MM:SS",           False),
    ("%m-%d-%Y %H:%M:%S",         "MM-DD-YYYY HH:MM:SS",           False),
    ("%d-%m-%Y %H:%M:%S.%f",      "DD-MM-YYYY HH:MM:SS.ffffff",    False),
    ("%m-%d-%Y %H:%M:%S.%f",      "MM-DD-YYYY HH:MM:SS.ffffff",    False),
]


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def infer(values: list[Optional[str]]) -> TypeInferenceResult:
    """
    Infer the most specific type for a column from its sampled values.

    Args:
        values: Raw string values from the column sample. None and empty
                strings are both treated as null and excluded from type checks.

    Returns:
        TypeInferenceResult with inferred_type, confidence_score,
        format_variants (MIXED_DATE), and quality_flags (MIXED_TIMEZONES,
        MIXED_DATE_FORMATS).
    """
    non_null = [v.strip() for v in values if v is not None and str(v).strip() != ""]

    if not non_null:
        return TypeInferenceResult(InferredType.NULL_ONLY, 1.0)

    total = len(non_null)

    for checker in (
        _check_boolean,     # before INTEGER: 1/0 are boolean tokens, not integers
        _check_date,        # before INTEGER: YYYYMMDD (20240115) is a valid integer
        _check_timestamp,   # before INTEGER: same guard; timestamps have separators so no clash with DATE
        _check_integer,
        _check_float,
        _check_uuid,
        _check_categorical,
        _check_free_text,
    ):
        result = checker(non_null, total)
        if result is not None:
            return result

    return TypeInferenceResult(InferredType.STRING, 1.0)


# ---------------------------------------------------------------------------
# Type checkers  (each returns TypeInferenceResult | None)
# ---------------------------------------------------------------------------

def _check_integer(values: list[str], total: int) -> Optional[TypeInferenceResult]:
    """
    INTEGER if non-null values are whole numbers.

    Leading-zero values (e.g. '00123') are NOT integers — they are likely
    zip codes, account IDs, or padded legacy codes and must stay as STRING.

    Commas are stripped before matching so '1,000' is treated as 1000.
    """
    matched = 0
    for val in values:
        clean = val.replace(",", "")
        if _RE_INT.match(clean):
            matched += 1

    ratio = matched / total
    if ratio >= _CONFIDENCE_THRESHOLD:
        return TypeInferenceResult(InferredType.INTEGER, ratio)
    return None


def _check_float(values: list[str], total: int) -> Optional[TypeInferenceResult]:
    """
    FLOAT if non-null values are decimals or scientific-notation numbers.
    Commas stripped before check ('1,000.50' → '1000.50').
    """
    matched = 0
    for val in values:
        clean = val.replace(",", "")
        if _RE_FLOAT.match(clean):
            matched += 1

    ratio = matched / total
    if ratio >= _CONFIDENCE_THRESHOLD:
        return TypeInferenceResult(InferredType.FLOAT, ratio)
    return None


def _check_boolean(values: list[str], total: int) -> Optional[TypeInferenceResult]:
    """BOOLEAN if non-null values are all recognised boolean tokens."""
    matched = sum(1 for v in values if v.lower() in _BOOL_TOKENS)
    ratio = matched / total
    if ratio >= _CONFIDENCE_THRESHOLD:
        return TypeInferenceResult(InferredType.BOOLEAN, ratio)
    return None


def _check_date(values: list[str], total: int) -> Optional[TypeInferenceResult]:
    """
    DATE if non-null values are calendar dates.
    MIXED_DATE if they parse as dates but using multiple format patterns.
    """
    matched_labels: list[str] = []

    for val in values:
        label = _match_date(val)
        if label is not None:
            matched_labels.append(label)

    ratio = len(matched_labels) / total
    if ratio < _CONFIDENCE_THRESHOLD:
        return None

    unique_formats = sorted(set(matched_labels))
    if len(unique_formats) > 1:
        return TypeInferenceResult(
            inferred_type=InferredType.MIXED_DATE,
            confidence_score=ratio,
            format_variants=unique_formats,
            quality_flags=[QualityFlag.MIXED_DATE_FORMATS],
        )

    return TypeInferenceResult(InferredType.DATE, ratio)


def _check_timestamp(values: list[str], total: int) -> Optional[TypeInferenceResult]:
    """
    TIMESTAMP if non-null values include a time component.
    Adds MIXED_TIMEZONES flag when some values carry timezone info and others do not.
    """
    tz_aware_count   = 0
    tz_naive_count   = 0
    matched          = 0

    for val in values:
        result = _match_timestamp(val)
        if result is not None:
            matched += 1
            _, is_tz = result
            if is_tz:
                tz_aware_count += 1
            else:
                tz_naive_count += 1

    ratio = matched / total
    if ratio < _CONFIDENCE_THRESHOLD:
        return None

    flags: list[QualityFlag] = []
    if tz_aware_count > 0 and tz_naive_count > 0:
        flags.append(QualityFlag.MIXED_TIMEZONES)
        log.debug(
            "MIXED_TIMEZONES: %d tz-aware, %d tz-naive values in column",
            tz_aware_count,
            tz_naive_count,
        )

    return TypeInferenceResult(
        inferred_type=InferredType.TIMESTAMP,
        confidence_score=ratio,
        quality_flags=flags,
    )


def _check_uuid(values: list[str], total: int) -> Optional[TypeInferenceResult]:
    """UUID if non-null values all match the standard 8-4-4-4-12 hex format."""
    matched = sum(1 for v in values if _RE_UUID.match(v))
    ratio = matched / total
    if ratio >= _CONFIDENCE_THRESHOLD:
        return TypeInferenceResult(InferredType.UUID, ratio)
    return None


def _check_categorical(values: list[str], total: int) -> Optional[TypeInferenceResult]:
    """
    CATEGORICAL if the number of distinct values is below CATEGORICAL_MAX_DISTINCT.
    Confidence is 1.0 — cardinality is an exact count, not a ratio.
    """
    if len(set(values)) < settings.CATEGORICAL_MAX_DISTINCT:
        return TypeInferenceResult(InferredType.CATEGORICAL, 1.0)
    return None


def _check_free_text(values: list[str], total: int) -> Optional[TypeInferenceResult]:
    """
    FREE_TEXT if the average string length exceeds FREE_TEXT_MIN_AVG_LENGTH.
    Confidence is 1.0 — average length is a deterministic metric.
    """
    avg_len = sum(len(v) for v in values) / total
    if avg_len >= settings.FREE_TEXT_MIN_AVG_LENGTH:
        return TypeInferenceResult(InferredType.FREE_TEXT, 1.0)
    return None


# ---------------------------------------------------------------------------
# Format-matching helpers
# ---------------------------------------------------------------------------

def _match_date(val: str) -> Optional[str]:
    """
    Try each date format against val.
    Returns the human-readable format label on the first match, else None.
    """
    for fmt, label in _DATE_FORMATS:
        try:
            datetime.strptime(val, fmt)
            return label
        except ValueError:
            continue
    return None


def _match_timestamp(val: str) -> Optional[tuple[str, bool]]:
    """
    Try each timestamp format against val.
    Returns (label, is_tz_aware) on the first match, else None.
    """
    for fmt, label, is_tz in _TIMESTAMP_FORMATS:
        try:
            datetime.strptime(val, fmt)
            return label, is_tz
        except ValueError:
            continue
    return None
