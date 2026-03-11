"""
Data models for the cross-table relationship analysis layer.

Produced by analysis/relationship_detector.py and consumed by
output/relationship_writer.py and the Column Intelligence Layer.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class ColumnRef:
    """Identifies a column within a named table."""
    table_name:  str
    column_name: str


@dataclass
class ForeignKeyCandidate:
    """
    A candidate foreign-key relationship detected from column profiles.

    fk  → the many-side column  (e.g. orders.customer_id)
    pk  → the one-side column   (e.g. customers.id)

    Confidence is the sum of four additive signals (name, type, cardinality,
    value overlap), capped at 1.0 and rounded to 4 decimal places.

    evidence lists the human-readable signal codes that contributed:
        e.g. ["name:singular_prefix", "type:exact", "pk:key_candidate",
               "cardinality:fk_subset", "overlap:high"]

    top_value_overlap_pct is an estimate derived from the top-10 most
    frequent values stored in each column profile.  It is None when either
    column has no top_values recorded.  It is NOT a true join-coverage figure
    (full coverage requires re-scanning the files).
    """
    fk:                    ColumnRef
    pk:                    ColumnRef
    confidence:            float
    evidence:              list[str]
    fk_null_ratio:         float           # fk.null_count / fk table row_count
    fk_distinct_count:     int
    pk_distinct_count:     int
    top_value_overlap_pct: Optional[float]  # |FK∩PK top vals| / |FK top vals|


@dataclass
class RelationshipReport:
    """
    Output of the relationship detector for a set of profiled tables.

    candidates is sorted by confidence descending.
    Only candidates with confidence >= MIN_CONFIDENCE (0.30) are included.
    Callers wanting only high-confidence results can filter on confidence >= 0.70.
    """
    tables_analyzed:  int
    columns_analyzed: int
    candidates:       list[ForeignKeyCandidate] = field(default_factory=list)
