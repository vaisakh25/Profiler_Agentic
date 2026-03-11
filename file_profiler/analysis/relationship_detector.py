"""
Cross-Table Relationship Detector

Entry point:  detect(profiles: list[FileProfile]) -> RelationshipReport

Detects likely foreign-key relationships between tables by scoring column
pairs on four additive signals derived entirely from already-computed
column profiles — no file re-scanning required.

Scoring signals (all additive, total capped at 1.0):
  1. Name    (max 0.50) — naming convention patterns
  2. Type    (max 0.20) — inferred-type compatibility
  3. Cardinality (max 0.25) — PK uniqueness + FK ⊆ PK cardinality
  4. Overlap (max 0.15) — top-10 value set intersection

Only candidates with confidence >= MIN_CONFIDENCE (0.30) are returned.
"""

from __future__ import annotations

import logging
from typing import Optional

from file_profiler.models.enums import InferredType, QualityFlag
from file_profiler.models.file_profile import ColumnProfile, FileProfile, TopValue
from file_profiler.models.relationships import (
    ColumnRef,
    ForeignKeyCandidate,
    RelationshipReport,
)

log = logging.getLogger(__name__)

# Minimum confidence to include a candidate in the report.
MIN_CONFIDENCE: float = 0.50

# Minimum length of the stripped pk_table name before attempting name patterns.
# Guards against degenerate short table names ("a", "ab") causing false positives.
_MIN_TABLE_NAME_LEN: int = 3

# Quality flags that disqualify a column from being a PK or FK candidate.
_DISQUALIFYING_FLAGS = frozenset({
    QualityFlag.FULLY_NULL,
    QualityFlag.STRUCTURAL_CORRUPTION,
})

# InferredTypes that are incompatible with being an FK (semantic mismatch).
_NON_FK_TYPES = frozenset({
    InferredType.BOOLEAN,
    InferredType.NULL_ONLY,
    InferredType.FREE_TEXT,
})

# Types that form compatible numeric pairs.
_NUMERIC_TYPES = frozenset({InferredType.INTEGER, InferredType.FLOAT})

# Types compatible as string-encoded identifiers.
_STRING_ID_TYPES = frozenset({InferredType.STRING, InferredType.INTEGER, InferredType.UUID})


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def detect(profiles: list[FileProfile]) -> RelationshipReport:
    """
    Detect foreign-key candidates across a set of table profiles.

    Args:
        profiles: List of FileProfile objects (typically from profile_directory()).

    Returns:
        RelationshipReport with all candidates sorted by confidence descending.
    """
    if len(profiles) < 2:
        log.debug(
            "Relationship detection requires >= 2 tables; got %d.", len(profiles)
        )
        total_cols = sum(len(fp.columns) for fp in profiles)
        return RelationshipReport(
            tables_analyzed  = len(profiles),
            columns_analyzed = total_cols,
            candidates       = [],
        )

    # Step 1 — build PK index: table_name → list of PK-eligible ColumnProfiles
    pk_index: dict[str, list[ColumnProfile]] = {}
    for fp in profiles:
        pk_candidates = [c for c in fp.columns if _is_pk_eligible(c)]
        if pk_candidates:
            pk_index[fp.table_name] = pk_candidates
            log.debug(
                "  %s: %d PK candidate(s): %s",
                fp.table_name,
                len(pk_candidates),
                [c.name for c in pk_candidates],
            )

    if not pk_index:
        log.debug("No PK candidates found — no relationships to report.")
        total_cols = sum(len(fp.columns) for fp in profiles)
        return RelationshipReport(
            tables_analyzed  = len(profiles),
            columns_analyzed = total_cols,
            candidates       = [],
        )

    # Step 2 & 3 — generate pairs and score each one
    candidates: list[ForeignKeyCandidate] = []

    for fk_fp in profiles:
        fk_table = fk_fp.table_name
        for fk_col in fk_fp.columns:
            # FK column must pass basic eligibility
            if not _is_fk_eligible(fk_col):
                continue

            for pk_table, pk_cols in pk_index.items():
                if pk_table == fk_table:
                    continue   # no self-joins

                for pk_col in pk_cols:
                    confidence, evidence = _score_pair(
                        fk_col, fk_table, fk_fp.row_count,
                        pk_col, pk_table,
                    )
                    if confidence < MIN_CONFIDENCE:
                        continue

                    overlap_pct = _value_overlap(fk_col.top_values, pk_col.top_values)
                    fk_null_ratio = (
                        fk_col.null_count / fk_fp.row_count
                        if fk_fp.row_count > 0 else 0.0
                    )

                    candidates.append(ForeignKeyCandidate(
                        fk                    = ColumnRef(fk_table, fk_col.name),
                        pk                    = ColumnRef(pk_table, pk_col.name),
                        confidence            = round(confidence, 4),
                        evidence              = evidence,
                        fk_null_ratio         = round(fk_null_ratio, 4),
                        fk_distinct_count     = fk_col.distinct_count,
                        pk_distinct_count     = pk_col.distinct_count,
                        top_value_overlap_pct = (
                            round(overlap_pct, 4) if overlap_pct is not None else None
                        ),
                    ))

    # Step 4 — sort by confidence descending
    candidates.sort(key=lambda c: c.confidence, reverse=True)

    total_cols = sum(len(fp.columns) for fp in profiles)
    log.debug(
        "Relationship detection complete: %d candidate(s) from %d table(s), %d column(s).",
        len(candidates), len(profiles), total_cols,
    )

    return RelationshipReport(
        tables_analyzed  = len(profiles),
        columns_analyzed = total_cols,
        candidates       = candidates,
    )


# ---------------------------------------------------------------------------
# Eligibility checks
# ---------------------------------------------------------------------------

def _is_pk_eligible(col: ColumnProfile) -> bool:
    """
    A column qualifies as a PK candidate if it is effectively unique and clean.

    Criteria:
      - No disqualifying quality flags (FULLY_NULL, STRUCTURAL_CORRUPTION)
      - Not a non-key type (FREE_TEXT, BOOLEAN, NULL_ONLY)
      - is_key_candidate == True  OR  (unique_ratio >= 0.95 AND null_count == 0)
    """
    if _has_disqualifying_flag(col):
        return False
    if col.inferred_type in _NON_FK_TYPES:
        return False
    if col.is_key_candidate:
        return True
    return col.unique_ratio >= 0.95 and col.null_count == 0


def _is_fk_eligible(col: ColumnProfile) -> bool:
    """
    A column qualifies as an FK candidate if it is not itself a PK and is clean.

    Criteria:
      - No disqualifying quality flags
      - Not a non-FK type (FREE_TEXT, BOOLEAN, NULL_ONLY)
      - Not itself a PK candidate (unique, non-null columns are likely PKs)
    """
    if _has_disqualifying_flag(col):
        return False
    if col.inferred_type in _NON_FK_TYPES:
        return False
    # A column that is itself a unique key is more likely a PK than an FK
    if col.is_key_candidate:
        return False
    return True


def _has_disqualifying_flag(col: ColumnProfile) -> bool:
    return bool(_DISQUALIFYING_FLAGS & set(col.quality_flags))


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------

def _score_pair(
    fk_col: ColumnProfile,
    fk_table: str,
    fk_row_count: int,
    pk_col: ColumnProfile,
    pk_table: str,
) -> tuple[float, list[str]]:
    """
    Compute the composite confidence score for one FK→PK pair.

    Returns (confidence, evidence_list).
    """
    evidence: list[str] = []
    total = 0.0

    name_s, name_ev = _name_score(fk_col.name, pk_col.name, pk_table)
    if name_s > 0:
        total += name_s
        evidence.append(name_ev)

    type_s, type_ev = _type_score(fk_col.inferred_type, pk_col.inferred_type)
    if type_s > 0:
        total += type_s
        evidence.append(type_ev)

    card_s, card_ev = _cardinality_score(fk_col, pk_col)
    if card_s > 0:
        total += card_s
        evidence.extend(card_ev)

    overlap_pct = _value_overlap(fk_col.top_values, pk_col.top_values)
    over_s, over_ev = _overlap_score_from_pct(overlap_pct)
    if over_s > 0:
        total += over_s
        evidence.append(over_ev)

    return min(1.0, total), evidence


def _name_score(fk_name: str, pk_name: str, pk_table: str) -> tuple[float, str]:
    """
    Score the naming-convention relationship between an FK column name and
    a PK column name / table name.  Returns (score, evidence_code).

    Patterns checked (highest → lowest, first match wins):
      direct_prefix   : fk_name == f"{pk_table}_{pk_name}"
                        e.g. fk="customers_id", pk_table="customers", pk_col="id"
      singular_prefix : fk_name == f"{pk_table.rstrip('s')}_{pk_name}"
                        e.g. fk="customer_id", pk_table="customers", pk_col="id"
      exact           : fk_name == pk_name
                        e.g. both columns are named "customer_id"
      embedded        : fk_name ends with f"_{pk_name}" and the stripped pk_table
                        name appears anywhere in fk_name
                        e.g. fk="ref_customer_id", pk_table="customers", pk_col="id"
    """
    stripped = pk_table.rstrip("s")

    # Guard: avoid matching against degenerate short table names
    if len(stripped) < _MIN_TABLE_NAME_LEN:
        # Still allow exact name match regardless of table name length
        if fk_name == pk_name:
            return 0.40, "name:exact"
        return 0.0, ""

    # Direct prefix: e.g. "customers_id"
    if fk_name == f"{pk_table}_{pk_name}":
        return 0.50, "name:direct_prefix"

    # Singular prefix: e.g. "customer_id" (strips trailing 's')
    if fk_name == f"{stripped}_{pk_name}":
        return 0.45, "name:singular_prefix"

    # Exact column name match
    if fk_name == pk_name:
        return 0.40, "name:exact"

    # Embedded: e.g. "ref_customer_id" where pk_table="customers", pk_col="id"
    if fk_name.endswith(f"_{pk_name}") and stripped in fk_name:
        return 0.35, "name:embedded"

    return 0.0, ""


def _type_score(
    fk_type: InferredType,
    pk_type: InferredType,
) -> tuple[float, str]:
    """
    Score type compatibility between an FK column and a PK column.

    Exact match is strongest.  Numeric pairs and string/integer/UUID
    combinations are common for IDs stored in different representations.
    """
    if fk_type == pk_type:
        return 0.20, "type:exact"
    if fk_type in _NUMERIC_TYPES and pk_type in _NUMERIC_TYPES:
        return 0.10, "type:numeric_compat"
    if fk_type in _STRING_ID_TYPES and pk_type in _STRING_ID_TYPES:
        return 0.05, "type:string_compat"
    return 0.0, ""


def _cardinality_score(
    fk_col: ColumnProfile,
    pk_col: ColumnProfile,
) -> tuple[float, list[str]]:
    """
    Score based on the PK column's uniqueness and the FK ⊆ PK cardinality rule.

    PK uniqueness (pick best, do not stack):
      is_key_candidate → 0.20
      unique_ratio >= 0.95 → 0.15

    Subset signal (additive with uniqueness):
      fk distinct_count <= pk distinct_count → 0.05
    """
    evidence: list[str] = []
    total = 0.0

    if pk_col.is_key_candidate:
        total += 0.20
        evidence.append("pk:key_candidate")
    elif pk_col.unique_ratio >= 0.95:
        total += 0.15
        evidence.append("pk:high_unique")

    if fk_col.distinct_count <= pk_col.distinct_count:
        total += 0.05
        evidence.append("cardinality:fk_subset")

    return total, evidence


def _value_overlap(
    fk_top: list[TopValue],
    pk_top: list[TopValue],
) -> Optional[float]:
    """
    Estimate what fraction of the FK column's top values appear in the PK
    column's top values.

    Returns None if either top-value list is empty.
    Returns a float in [0.0, 1.0] otherwise.

    Note: this is only an approximation.  It compares the top-10 most
    frequent values in each column — not all distinct values.
    """
    fk_vals = {tv.value for tv in fk_top}
    pk_vals = {tv.value for tv in pk_top}

    if not fk_vals or not pk_vals:
        return None

    return len(fk_vals & pk_vals) / len(fk_vals)


def _overlap_score_from_pct(
    overlap_pct: Optional[float],
) -> tuple[float, str]:
    """Convert an overlap percentage to a score + evidence code."""
    if overlap_pct is None:
        return 0.0, ""
    if overlap_pct >= 0.80:
        return 0.15, "overlap:high"
    if overlap_pct >= 0.50:
        return 0.10, "overlap:medium"
    return 0.0, ""
