"""LLM-enriched relationship analysis via RAG over profiling results.

Builds a ChromaDB vector store from profiling output (column profiles,
sample rows, low-cardinality values, detected relationships), then uses
an LLM to produce:
  - Semantic descriptions for tables and columns
  - PK/FK confidence reassessment and join recommendations
  - An enriched ER diagram with descriptions

This acts as a "second opinion" layer on top of the deterministic pipeline.

Usage (standalone):
    profiles, report = ...  # from the profiler pipeline
    result = await enrich(profiles, report, dir_path, provider="groq")

Usage (via MCP tool / chatbot):
    Automatically invoked by the ``enrich_relationships`` MCP tool or
    by the chatbot after ``detect_relationships``.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

from file_profiler.models.file_profile import FileProfile
from file_profiler.models.relationships import RelationshipReport

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SAMPLE_ROWS_COUNT = 10  # rows to extract from each source file
ENRICHMENT_COLLECTION = "profiler_enrichment"

# ---------------------------------------------------------------------------
# 1. Sample row extractor — reads actual rows from source files
# ---------------------------------------------------------------------------


def extract_sample_rows(file_path: str, n: int = SAMPLE_ROWS_COUNT) -> list[dict]:
    """Read up to *n* rows from a data file as list-of-dicts.

    Supports Parquet and CSV.  Returns an empty list on error.
    """
    path = Path(file_path)
    try:
        if path.suffix.lower() == ".parquet":
            return _read_parquet_rows(path, n)
        elif path.suffix.lower() in (".csv", ".tsv"):
            return _read_csv_rows(path, n)
        else:
            log.debug("Unsupported format for sample rows: %s", path.suffix)
            return []
    except Exception as exc:
        log.warning("Could not extract sample rows from %s: %s", path, exc)
        return []


def _read_parquet_rows(path: Path, n: int) -> list[dict]:
    import pyarrow.parquet as pq

    table = pq.read_table(path)
    df = table.slice(0, min(n, table.num_rows)).to_pandas()
    # Convert to string representations for embedding
    return [
        {col: str(val) for col, val in row.items()}
        for row in df.to_dict(orient="records")
    ]


def _read_csv_rows(path: Path, n: int) -> list[dict]:
    import csv

    rows: list[dict] = []
    with open(path, "r", encoding="utf-8", errors="replace") as fh:
        reader = csv.DictReader(fh)
        for i, row in enumerate(reader):
            if i >= n:
                break
            rows.append({k: str(v) for k, v in row.items()})
    return rows


# ---------------------------------------------------------------------------
# 2. Document builder — creates LangChain Documents for embedding
# ---------------------------------------------------------------------------


def build_documents(
    profiles: list[FileProfile],
    report: RelationshipReport,
    dir_path: str,
) -> list:
    """Build LangChain Documents from profiling results.

    Creates three categories of documents:
      - **Table schema docs** (one per table): column names, types, stats,
        quality flags, low-cardinality values with samples, sample rows.
      - **Relationship doc** (one): all detected FK candidates with evidence.
      - **Quality overview doc** (one): aggregate quality across all tables.

    Returns:
        List of LangChain Document objects ready for embedding.
    """
    from langchain_core.documents import Document

    docs: list[Document] = []

    for profile in profiles:
        # --- Table schema document ---
        col_descriptions = []
        low_card_details = []

        for col in profile.columns:
            flags_str = ", ".join(f.value for f in col.quality_flags) if col.quality_flags else "none"
            desc = (
                f"  - {col.name}: type={col.inferred_type.value}, "
                f"nulls={col.null_count}, distinct={col.distinct_count}, "
                f"cardinality={col.cardinality.value}, "
                f"key_candidate={col.is_key_candidate}, "
                f"quality_flags=[{flags_str}]"
            )
            if col.sample_values:
                desc += f", samples={col.sample_values[:5]}"
            col_descriptions.append(desc)

            # Low cardinality columns — include all top values
            if col.is_low_cardinality and col.top_values:
                values_str = ", ".join(
                    f"{tv.value} ({tv.count})" for tv in col.top_values[:15]
                )
                low_card_details.append(
                    f"  - {col.name}: {col.distinct_count} distinct values: [{values_str}]"
                )

        schema_text = (
            f"Table: {profile.table_name}\n"
            f"Format: {profile.file_format.value}\n"
            f"Rows: {profile.row_count}\n"
            f"Columns ({len(profile.columns)}):\n"
            + "\n".join(col_descriptions)
        )

        if low_card_details:
            schema_text += "\n\nLow cardinality columns (categorical/enum-like):\n"
            schema_text += "\n".join(low_card_details)

        # Extract and add sample rows
        sample_rows = extract_sample_rows(profile.file_path)
        if sample_rows:
            rows_str = "\n".join(
                f"  Row {i+1}: {json.dumps(row)}"
                for i, row in enumerate(sample_rows[:SAMPLE_ROWS_COUNT])
            )
            schema_text += f"\n\nSample rows ({len(sample_rows)}):\n{rows_str}"

        if profile.structural_issues:
            schema_text += f"\n\nStructural issues: {profile.structural_issues}"

        docs.append(Document(
            page_content=schema_text,
            metadata={
                "doc_type": "table_schema",
                "table_name": profile.table_name,
                "row_count": profile.row_count,
                "column_count": len(profile.columns),
            },
        ))

    # --- Relationships document ---
    if report.candidates:
        rel_lines = []
        for c in report.candidates:
            rel_lines.append(
                f"  {c.fk.table_name}.{c.fk.column_name} -> "
                f"{c.pk.table_name}.{c.pk.column_name} "
                f"(confidence={c.confidence:.2f}, "
                f"evidence=[{', '.join(c.evidence)}], "
                f"fk_nulls={c.fk_null_ratio:.2f}, "
                f"fk_distinct={c.fk_distinct_count}, "
                f"pk_distinct={c.pk_distinct_count}, "
                f"overlap={c.top_value_overlap_pct})"
            )

        rel_text = (
            f"Detected relationships across {report.tables_analyzed} tables, "
            f"{report.columns_analyzed} columns:\n"
            + "\n".join(rel_lines)
        )

        docs.append(Document(
            page_content=rel_text,
            metadata={
                "doc_type": "relationships",
                "candidate_count": len(report.candidates),
                "tables_analyzed": report.tables_analyzed,
            },
        ))

    # --- Quality overview document ---
    quality_lines = []
    for profile in profiles:
        qs = profile.quality_summary
        quality_lines.append(
            f"  {profile.table_name}: "
            f"profiled={qs.columns_profiled}, "
            f"issues={qs.columns_with_issues}, "
            f"null_heavy={qs.null_heavy_columns}, "
            f"type_conflicts={qs.type_conflict_columns}, "
            f"corrupt_rows={qs.corrupt_rows_detected}"
        )

    quality_text = (
        f"Quality overview for {len(profiles)} tables "
        f"in {dir_path}:\n"
        + "\n".join(quality_lines)
    )

    docs.append(Document(
        page_content=quality_text,
        metadata={"doc_type": "quality_overview", "table_count": len(profiles)},
    ))

    log.info(
        "Built %d documents (%d tables + relationships + quality)",
        len(docs), len(profiles),
    )
    return docs


# ---------------------------------------------------------------------------
# 3. Vector store — embed documents into ChromaDB
# ---------------------------------------------------------------------------


def create_vector_store(
    documents: list,
    collection_name: str = ENRICHMENT_COLLECTION,
):
    """Embed documents into a transient ChromaDB collection.

    Uses Jina ``jina-embeddings-v3`` via API (requires JINA_API_KEY).

    Args:
        documents:       LangChain Document objects.
        collection_name: ChromaDB collection name.

    Returns:
        A LangChain Chroma vector store instance.
    """
    from langchain_chroma import Chroma
    from file_profiler.agent.vector_store import get_embeddings

    embeddings = get_embeddings()

    vector_store = Chroma.from_documents(
        documents=documents,
        embedding=embeddings,
        collection_name=collection_name,
    )

    log.info(
        "Vector store created: %d documents in '%s'",
        len(documents), collection_name,
    )
    return vector_store


# ---------------------------------------------------------------------------
# 4. LLM enrichment chain — RAG over the vector store
# ---------------------------------------------------------------------------

ENRICHMENT_PROMPT = """\
You are a senior data engineer analysing a set of profiled data tables.

You have been given:
1. **Table schemas** with column types, cardinality, quality flags, sample values, \
and actual sample rows from each table.
2. **Detected relationships** — foreign key candidates found by a deterministic \
algorithm (name matching, type compatibility, cardinality checks, value overlap).
3. **Quality overview** — aggregate quality metrics per table.

## Your task

Analyse all the context and produce:

### 1. Table Descriptions
For each table, write a 1–2 sentence semantic description of what the table \
represents in the domain. Use column names, sample values, and relationships \
to infer meaning.

### 2. Column Descriptions
For each table, describe the key columns (especially PKs, FKs, and columns \
with notable patterns). Focus on semantic meaning, not just type info.

### 3. Primary Key Assessment
For each table, confirm or revise the primary key candidates. Explain your \
reasoning based on uniqueness, naming, and domain knowledge.

### 4. Foreign Key Assessment
Review each detected FK relationship. For each:
- **Confirm** or **reject** the relationship with reasoning
- **Suggest new relationships** the deterministic algorithm may have missed

### 5. Join Path Recommendations
Recommend optimal **JOIN types** (INNER, LEFT, etc.) with reasoning. \
Suggest the most useful join paths for common analytical queries. \
For example: "To analyse conditions per patient, join person → visit_occurrence \
→ condition_occurrence on person_id and visit_occurrence_id."

### 6. Enriched ER Diagram
Generate a Mermaid erDiagram that includes:
- All tables with their columns and types
- PK and FK annotations
- Relationship lines with descriptive labels
- Use comments to add table descriptions

Format the diagram inside a ```mermaid code block.

### 7. Data Quality Recommendations
Based on quality flags, null ratios, and structural issues, provide \
actionable recommendations for data cleanup or migration preparation. \
useful data quality checks recommendations.

---

## Context

{context}

---

Produce a comprehensive analysis. Be specific — reference actual column \
names, sample values, and confidence scores.
"""


async def enrich(
    profiles: list[FileProfile],
    report: RelationshipReport,
    dir_path: str,
    provider: str = "groq",
    model: Optional[str] = None,
) -> dict:
    """Run the full enrichment pipeline.

    Delegates to the map-reduce implementation in
    ``enrichment_mapreduce.enrich()``.  This wrapper exists for
    backward compatibility.

    Args:
        profiles: List of FileProfile objects.
        report:   RelationshipReport from the deterministic detector.
        dir_path: Path to the data directory (for metadata).
        provider: LLM provider (``"groq"``, ``"google"``, ``"openai"``).
        model:    LLM model override.

    Returns:
        Dict with ``enrichment`` (full LLM analysis text) and metadata.
    """
    from file_profiler.agent.enrichment_mapreduce import enrich as mr_enrich

    return await mr_enrich(
        profiles=profiles,
        report=report,
        dir_path=dir_path,
        provider=provider,
        model=model,
        incremental=True,
    )
