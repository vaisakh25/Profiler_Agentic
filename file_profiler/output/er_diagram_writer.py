"""
ER Diagram Writer — Mermaid format.

Generates a Mermaid erDiagram from profiled tables and detected relationships.

Entry point:
  write(profiles, report, output_path, min_confidence) -> None
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

from file_profiler.models.enums import InferredType
from file_profiler.models.file_profile import FileProfile
from file_profiler.models.relationships import RelationshipReport

log = logging.getLogger(__name__)

# Map InferredType to short Mermaid-friendly type labels.
_TYPE_LABEL = {
    InferredType.INTEGER:    "int",
    InferredType.FLOAT:      "float",
    InferredType.BOOLEAN:    "bool",
    InferredType.DATE:       "date",
    InferredType.TIMESTAMP:  "timestamp",
    InferredType.UUID:       "uuid",
    InferredType.STRING:     "string",
    InferredType.FREE_TEXT:  "text",
    InferredType.CATEGORICAL: "categorical",
    InferredType.MIXED_DATE: "mixed_date",
    InferredType.NULL_ONLY:  "null",
}


def write(
    profiles: list[FileProfile],
    report: RelationshipReport,
    output_path: str | Path,
    min_confidence: float = 0.70,
) -> None:
    """
    Write a Mermaid erDiagram to a markdown file.

    Args:
        profiles:       List of FileProfile objects (tables and their columns).
        report:         RelationshipReport from relationship_detector.detect().
        output_path:    Destination .md file.
        min_confidence: Only include relationships at or above this confidence.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    lines = generate(profiles, report, min_confidence)

    output_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    log.info("ER diagram written → %s", output_path)


def generate(
    profiles: list[FileProfile],
    report: RelationshipReport,
    min_confidence: float = 0.70,
) -> list[str]:
    """
    Generate Mermaid erDiagram lines (without writing to disk).

    Returns a list of strings — one per line.
    """
    # Build lookup: table_name → FileProfile
    table_map = {p.table_name: p for p in profiles}

    # Filter relationships by confidence.
    rels = [c for c in report.candidates if c.confidence >= min_confidence]

    # Collect tables that participate in at least one relationship.
    participating_tables: set[str] = set()
    for r in rels:
        participating_tables.add(r.pk.table_name)
        participating_tables.add(r.fk.table_name)

    # Also collect PK/FK column names per table for annotation.
    pk_columns: dict[str, set[str]] = {}  # table → {col_name, ...}
    fk_columns: dict[str, set[str]] = {}
    for r in rels:
        pk_columns.setdefault(r.pk.table_name, set()).add(r.pk.column_name)
        fk_columns.setdefault(r.fk.table_name, set()).add(r.fk.column_name)

    lines: list[str] = [
        "```mermaid",
        "erDiagram",
    ]

    # ── Entity definitions ──────────────────────────────────────────────
    for tname in sorted(participating_tables):
        profile = table_map.get(tname)
        if profile is None or not profile.columns:
            lines.append(f"    {_safe(tname)} {{")
            lines.append("    }")
            continue

        lines.append(f"    {_safe(tname)} {{")
        for col in profile.columns:
            type_label = _TYPE_LABEL.get(col.inferred_type, "string")
            marker = ""
            if col.name in pk_columns.get(tname, set()):
                marker = " PK"
            elif col.name in fk_columns.get(tname, set()):
                marker = " FK"
            lines.append(f"        {type_label} {_safe(col.name)}{marker}")
        lines.append("    }")

    # ── Relationship lines ──────────────────────────────────────────────
    lines.append("")
    for r in sorted(rels, key=lambda x: (-x.confidence, x.pk.table_name)):
        pk_t = _safe(r.pk.table_name)
        fk_t = _safe(r.fk.table_name)
        label = f"{r.fk.column_name} -> {r.pk.column_name}"
        # ||--o{ = one(exact)-to-many(zero or more)
        lines.append(f"    {pk_t} ||--o{{ {fk_t} : \"{label}\"")

    lines.append("```")
    return lines


def _safe(name: str) -> str:
    """Sanitise a name for Mermaid (replace non-alphanumeric with underscore)."""
    return "".join(c if c.isalnum() or c == "_" else "_" for c in name)
