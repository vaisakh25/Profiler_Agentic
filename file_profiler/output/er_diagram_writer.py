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
from file_profiler.observability.langsmith import compact_text_output, traceable

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

# Common audit/metadata FK columns that create noise in ER diagrams.
# These are valid FKs but overwhelm the diagram when a single table
# (e.g. people/users) is referenced by every other table.
_AUDIT_FK_PATTERNS = {
    "lasteditedby", "last_edited_by",
    "createdby", "created_by",
    "modifiedby", "modified_by",
    "updatedby", "updated_by",
}


@traceable(
    name="output.er_diagram_writer.write",
    run_type="chain",
    process_outputs=compact_text_output,
)
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


def _deduplicate_relationships(
    rels: list,
) -> list:
    """Keep only the best (highest-confidence) relationship per FK column.

    When `condition_era.person_id` has candidates pointing to both
    `person.person_id` and `observation_period.person_id`, only the
    highest-confidence one is retained.  On ties, prefer the candidate
    whose PK table name is a prefix/substring of the FK column name
    (e.g. `person_id` → `person` table).

    Also removes bidirectional duplicates: if both A.x→B.y and B.y→A.x
    exist, keeps only the higher-confidence one.
    """
    best: dict[tuple[str, str], object] = {}  # (fk_table, fk_col) → best candidate

    for r in sorted(rels, key=lambda x: -x.confidence):
        key = (r.fk.table_name, r.fk.column_name)
        if key not in best:
            best[key] = r
        else:
            existing = best[key]
            if r.confidence > existing.confidence:
                best[key] = r
            elif r.confidence == existing.confidence:
                # Tie-break: prefer PK table whose name matches the FK column
                # e.g. person_id → person table over observation_period table
                fk_col_lower = r.fk.column_name.lower().replace("_id", "").replace("id", "")
                r_match = fk_col_lower == r.pk.table_name.lower()
                e_match = fk_col_lower == existing.pk.table_name.lower()
                if r_match and not e_match:
                    best[key] = r

    deduped = list(best.values())

    # Remove bidirectional duplicates: if A.x→B.y and B.y→A.x both
    # exist, keep only the higher-confidence edge.
    seen_pairs: dict[tuple[str, str, str, str], object] = {}
    for r in sorted(deduped, key=lambda x: -x.confidence):
        forward = (r.fk.table_name, r.fk.column_name, r.pk.table_name, r.pk.column_name)
        reverse = (r.pk.table_name, r.pk.column_name, r.fk.table_name, r.fk.column_name)
        if forward not in seen_pairs and reverse not in seen_pairs:
            seen_pairs[forward] = r
    return list(seen_pairs.values())


def _is_audit_fk(fk_col_name: str) -> bool:
    """Return True if the FK column name matches a common audit pattern."""
    return fk_col_name.lower() in _AUDIT_FK_PATTERNS


def generate(
    profiles: list[FileProfile],
    report: RelationshipReport,
    min_confidence: float = 0.70,
) -> list[str]:
    """
    Generate Mermaid erDiagram lines (without writing to disk).

    Audit FK columns (e.g. lasteditedby) are separated into a compact
    summary section so they don't overwhelm the main diagram.

    Returns a list of strings — one per line.
    """
    # Build lookup: table_name → FileProfile
    table_map = {p.table_name: p for p in profiles}

    # Separate audit relationships BEFORE confidence filtering so they
    # appear in the summary section even at lower confidence levels.
    all_candidates = report.candidates
    audit_rels_raw = [c for c in all_candidates if _is_audit_fk(c.fk.column_name) and c.confidence >= 0.50]
    domain_candidates = [c for c in all_candidates if not _is_audit_fk(c.fk.column_name)]

    # Filter domain relationships by confidence, then deduplicate.
    rels = [c for c in domain_candidates if c.confidence >= min_confidence]
    rels = _deduplicate_relationships(rels)
    domain_rels = rels

    # Deduplicate audit rels for the summary section.
    audit_rels = _deduplicate_relationships(audit_rels_raw)

    if audit_rels:
        log.info(
            "ER diagram: separated %d audit FK edges (e.g. lasteditedby) "
            "from %d domain edges",
            len(audit_rels), len(domain_rels),
        )

    # Collect tables that participate in at least one relationship.
    participating_tables: set[str] = set()
    for r in rels:  # include audit tables in entity definitions
        participating_tables.add(r.pk.table_name)
        participating_tables.add(r.fk.table_name)

    # Determine PK columns from the profile's own key_candidate flag —
    # NOT from being on the pk side of a relationship.  A column is only
    # marked PK if the profiler already identified it as a key candidate
    # (high uniqueness, no nulls).
    #
    # FK columns are derived from the deduplicated relationship set.
    fk_columns: dict[str, set[str]] = {}
    for r in rels:
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
            if col.is_key_candidate and col.name not in fk_columns.get(tname, set()):
                marker = " PK"
            elif col.name in fk_columns.get(tname, set()):
                marker = " FK"
            lines.append(f"        {type_label} {_safe(col.name)}{marker}")
        lines.append("    }")

    # ── Domain relationship lines ──────────────────────────────────────
    lines.append("")
    for r in sorted(domain_rels, key=lambda x: (-x.confidence, x.pk.table_name)):
        pk_t = _safe(r.pk.table_name)
        fk_t = _safe(r.fk.table_name)
        label = f"{r.fk.column_name} -> {r.pk.column_name}"
        # ||--o{ = one(exact)-to-many(zero or more)
        lines.append(f"    {pk_t} ||--o{{ {fk_t} : \"{label}\"")

    lines.append("```")

    # ── Audit FK summary (outside the diagram for clarity) ─────────────
    if audit_rels:
        # Group by PK table (the referenced table, e.g. application_people)
        audit_by_pk: dict[str, list[str]] = {}
        for r in audit_rels:
            pk_label = f"{r.pk.table_name}.{r.pk.column_name}"
            fk_label = f"{r.fk.table_name}.{r.fk.column_name}"
            audit_by_pk.setdefault(pk_label, []).append(fk_label)

        lines.append("")
        lines.append("### Audit FK Relationships")
        lines.append("")
        lines.append("The following audit/tracking columns reference a shared "
                      "lookup table. They are valid FKs but excluded from the "
                      "diagram to reduce visual noise.")
        lines.append("")
        for pk_label, fk_list in sorted(audit_by_pk.items()):
            lines.append(f"**{pk_label}** ← {len(fk_list)} tables:")
            for fk in sorted(fk_list):
                lines.append(f"  - `{fk}`")
            lines.append("")

    return lines


def _safe(name: str) -> str:
    """Sanitise a name for Mermaid (replace non-alphanumeric with underscore)."""
    return "".join(c if c.isalnum() or c == "_" else "_" for c in name)
