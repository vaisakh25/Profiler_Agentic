"""Map-reduce LLM enrichment pipeline.

Replaces the monolithic enrichment approach with a three-phase pipeline:

Phase 1 (MAP):    For each table, send a small per-table prompt to the LLM
                  asking for a concise semantic summary.  Parallelizable.
Phase 2 (EMBED):  Store all table summaries in a persistent ChromaDB vector
                  store.  Only new/changed tables are re-summarized.
Phase 3 (REDUCE): Query the vector store for semantically related tables,
                  then send a focused prompt to the LLM for cross-table
                  relationship analysis and join recommendations.

Usage:
    result = await enrich(profiles, report, dir_path, provider="google")
"""

from __future__ import annotations

import asyncio
import json
import logging
import math
from pathlib import Path
from typing import Optional

from file_profiler.models.file_profile import FileProfile
from file_profiler.models.relationships import RelationshipReport

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------

MAP_PROMPT = """\
You are a Senior Data and AI Engineer. Analyse this single table profile and produce a \
concise summary (max 200 words).

Include:
1. What the table likely represents (infer from column names, sample values)
2. Primary key column(s) and their types
3. Notable columns (FKs, categoricals, dates, high-null)
4. Data quality issues worth noting
5. Row count and column count

Table profile:
{profile_context}

Respond with ONLY the summary paragraph — no headers, no markdown.
"""

REDUCE_PROMPT = """\
You are a Senior Data and AI Engineer analysing a set of profiled data tables.

You have:
1. **Table summaries** — LLM-generated descriptions of each table.
2. **Detected relationships** — FK candidates from a deterministic algorithm \
(name matching, type compatibility, cardinality checks, value overlap).

## Your task

Produce:

### 1. Table Descriptions
Confirm or revise the table descriptions below.

### 2. Column Descriptions
For each table, describe key columns (PKs, FKs, notable patterns).

### 3. Primary Key Assessment
Confirm or revise PK candidates with reasoning.

### 4. Foreign Key Assessment
Review each detected FK. Confirm/reject with reasoning. Suggest missed ones.

### 5. Join Path Recommendations
Recommend JOIN types and useful join paths for analytics.

### 6. Enriched ER Diagram
Generate a Mermaid erDiagram with all tables, columns, PK/FK annotations, \
and relationship lines with descriptive labels. Format inside ```mermaid and share a .png/.svg link if possible.

### 7. Data Quality Recommendations
Actionable recommendations based on quality flags and null ratios.

---

## Table Summaries

{table_summaries}

## Detected Relationships

{relationships}

---

Be specific — reference column names, sample values, confidence scores.
"""


CLUSTER_REDUCE_PROMPT = """\
You are a Senior Data and AI Engineer analysing a cluster of semantically related tables \
that share a common domain or functional area.

## Table Summaries

{table_summaries}

## Detected Relationships (within this cluster)

{relationships}

---

## Your task

### 1. Cluster Theme
One sentence: what domain or functional area do these tables represent?

### 2. Primary Key Assessment
Identify PK candidates for each table with reasoning.

### 3. Foreign Key Assessment
Identify FK relationships within this cluster. Confirm or reject detected ones. \
Suggest missed ones.

### 4. Join Paths
Recommended join types and paths between tables in this cluster.

### 5. Cluster ER Diagram
```mermaid
erDiagram
    (tables in this cluster only)
```

### 6. Data Quality Notes
Key quality issues specific to these tables.

Be specific — reference column names, sample values, confidence scores.
"""

META_REDUCE_PROMPT = """\
You are a Senior Data and AI Engineer. You have received per-cluster analyses of a \
large multi-table database schema.

## Cluster Analyses

{cluster_analyses}

## Cross-Cluster Detected Relationships

{cross_cluster_relationships}

---

## Your task

Produce the comprehensive final analysis:

### 1. Table Descriptions
For each table (grouped by cluster), confirm or revise its description.

### 2. Column Descriptions
For each table, describe key columns (PKs, FKs, notable patterns).

### 3. Primary Key Assessment
Final PK confirmation across all tables.

### 4. Foreign Key Assessment
All FK relationships — intra-cluster and cross-cluster. Suggest missed ones.

### 5. Join Path Recommendations
Full join paths for analytics, including cross-cluster joins.

### 6. Complete ER Diagram
```mermaid
erDiagram
    (all tables, all relationships)
```

### 7. Data Quality Recommendations
Actionable recommendations across all tables.

Be specific — reference column names, sample values, confidence scores.
"""


# ---------------------------------------------------------------------------
# Profile context builder
# ---------------------------------------------------------------------------

def _build_table_context(profile: FileProfile, token_budget: int = 2000) -> str:
    """Build a compact profile context string for one table.

    Includes column metadata, low-cardinality values, and sample rows.
    Truncates to approximately *token_budget* characters.
    """
    col_lines = []
    for col in profile.columns:
        flags = ", ".join(f.value for f in col.quality_flags) if col.quality_flags else "none"
        line = (
            f"  - {col.name}: type={col.inferred_type.value}, "
            f"nulls={col.null_count}, distinct={col.distinct_count}, "
            f"key_candidate={col.is_key_candidate}, "
            f"quality=[{flags}]"
        )
        if col.sample_values:
            line += f", samples={col.sample_values[:3]}"
        col_lines.append(line)

    text = (
        f"Table: {profile.table_name}\n"
        f"Rows: {profile.row_count}, Columns: {len(profile.columns)}\n"
        f"Format: {profile.file_format.value}\n"
        f"Columns:\n" + "\n".join(col_lines)
    )

    # Add sample rows (compact — max 5 rows)
    from file_profiler.agent.enrichment import extract_sample_rows
    rows = extract_sample_rows(profile.file_path, n=5)
    if rows:
        rows_str = "\n".join(f"  {json.dumps(r)}" for r in rows[:5])
        text += f"\n\nSample rows:\n{rows_str}"

    # Truncate to budget
    if len(text) > token_budget:
        text = text[:token_budget - 20] + "\n... (truncated)"

    return text


def _build_relationships_context(report: RelationshipReport) -> str:
    """Format the deterministic relationship report for the REDUCE prompt."""
    if not report.candidates:
        return "No relationships detected by the deterministic algorithm."

    lines = []
    for c in report.candidates:
        lines.append(
            f"  {c.fk.table_name}.{c.fk.column_name} -> "
            f"{c.pk.table_name}.{c.pk.column_name} "
            f"(confidence={c.confidence:.2f}, "
            f"evidence=[{', '.join(c.evidence)}], "
            f"overlap={c.top_value_overlap_pct})"
        )
    return (
        f"Detected {len(report.candidates)} relationships "
        f"across {report.tables_analyzed} tables:\n"
        + "\n".join(lines)
    )


# ---------------------------------------------------------------------------
# Phase 1: MAP — per-table LLM summarization
# ---------------------------------------------------------------------------

async def _summarize_one_table(
    profile: FileProfile,
    llm,
    token_budget: int = 2000,
    semaphore: Optional[asyncio.Semaphore] = None,
) -> tuple[str, str]:
    """Summarize a single table using a small LLM prompt.

    Returns:
        Tuple of (table_name, summary_text).
        On error, summary_text is a fallback description.
    """
    context = _build_table_context(profile, token_budget)
    prompt = MAP_PROMPT.format(profile_context=context)

    async def _invoke():
        response = await llm.ainvoke(prompt)
        content = response.content
        if isinstance(content, list):
            content = " ".join(
                item.get("text", str(item)) if isinstance(item, dict) else str(item)
                for item in content
            )
        return content

    try:
        if semaphore:
            async with semaphore:
                summary = await _invoke()
        else:
            summary = await _invoke()

        log.info("MAP: summarized %s (%d chars)", profile.table_name, len(summary))
        return profile.table_name, summary

    except Exception as exc:
        log.warning("MAP: failed for %s: %s — using fallback", profile.table_name, exc)
        # Fallback: use the raw profile context as the summary
        fallback = (
            f"Table {profile.table_name} has {profile.row_count} rows and "
            f"{len(profile.columns)} columns. "
            f"Columns: {', '.join(c.name for c in profile.columns)}."
        )
        return profile.table_name, fallback


async def map_phase(
    profiles: list[FileProfile],
    llm,
    max_workers: int = 4,
    token_budget: int = 2000,
    existing_fingerprints: Optional[dict[str, str]] = None,
) -> dict[str, str]:
    """Run the MAP phase: summarize each table in parallel.

    Args:
        profiles: All FileProfile objects.
        llm: LangChain chat model.
        max_workers: Max concurrent LLM calls.
        token_budget: Per-table context budget in chars.
        existing_fingerprints: {table_name: fingerprint} already in store.
                               Tables with matching fingerprints are skipped.

    Returns:
        Dict mapping table_name -> summary_text for newly summarized tables.
    """
    from file_profiler.agent.vector_store import _table_fingerprint

    existing_fingerprints = existing_fingerprints or {}

    # Determine which tables need (re-)summarization
    to_summarize = []
    for p in profiles:
        fp = _table_fingerprint(p.table_name, p.row_count, len(p.columns))
        if existing_fingerprints.get(p.table_name) == fp:
            log.debug("MAP: skipping %s (fingerprint match)", p.table_name)
            continue
        to_summarize.append(p)

    if not to_summarize:
        log.info("MAP: all %d tables already summarized", len(profiles))
        return {}

    log.info("MAP: summarizing %d/%d tables (max_workers=%d)",
             len(to_summarize), len(profiles), max_workers)

    semaphore = asyncio.Semaphore(max_workers)
    tasks = [
        _summarize_one_table(p, llm, token_budget, semaphore)
        for p in to_summarize
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    summaries = {}
    for result in results:
        if isinstance(result, Exception):
            log.error("MAP: unexpected error: %s", result)
            continue
        table_name, summary = result
        summaries[table_name] = summary

    log.info("MAP: completed %d summaries", len(summaries))
    return summaries


# ---------------------------------------------------------------------------
# Phase 2: EMBED — store summaries in persistent vector DB
# ---------------------------------------------------------------------------

def embed_phase(
    summaries: dict[str, str],
    profiles: list[FileProfile],
    report: RelationshipReport,
    persist_dir: Path,
):
    """Store table summaries and relationship doc in persistent ChromaDB.

    Upserts each summary (idempotent).  Also stores the deterministic
    relationship report as a separate document.

    Returns:
        The Chroma vector store instance.
    """
    from file_profiler.agent.vector_store import (
        _table_fingerprint,
        get_or_create_store,
        upsert_relationship_doc,
        upsert_table_summary,
    )

    store = get_or_create_store(persist_dir)

    # Build a lookup for profile metadata
    profile_map = {p.table_name: p for p in profiles}

    for table_name, summary in summaries.items():
        p = profile_map.get(table_name)
        meta = {}
        if p:
            meta = {
                "row_count": p.row_count,
                "column_count": len(p.columns),
                "fingerprint": _table_fingerprint(
                    p.table_name, p.row_count, len(p.columns),
                ),
            }
        upsert_table_summary(store, table_name, summary, meta)

    # Store the deterministic relationship report
    rel_text = _build_relationships_context(report)
    upsert_relationship_doc(store, rel_text, {
        "candidate_count": len(report.candidates),
        "tables_analyzed": report.tables_analyzed,
    })

    log.info("EMBED: upserted %d summaries + relationship doc", len(summaries))
    return store


# ---------------------------------------------------------------------------
# Phase 3: REDUCE — cross-table LLM analysis
# ---------------------------------------------------------------------------

async def reduce_phase(
    store,
    report: RelationshipReport,
    profiles: list[FileProfile],
    llm,
    top_k: int = 15,
    token_budget: int = 12000,
) -> str:
    """Run the REDUCE phase: cross-table relationship analysis.

    For small datasets (<= top_k tables), retrieves all summaries.
    For larger datasets, uses semantic search for the most relevant subset.

    Returns:
        Full LLM analysis text (markdown with ER diagram, etc.).
    """
    from file_profiler.agent.vector_store import (
        get_all_summaries,
        query_similar_tables,
    )

    # Retrieve table summaries
    if len(profiles) <= top_k:
        summary_docs = get_all_summaries(store)
    else:
        # Build a query from relationship candidates + table names
        query_parts = [p.table_name for p in profiles[:10]]
        for c in report.candidates[:10]:
            query_parts.append(
                f"{c.fk.table_name}.{c.fk.column_name} relates to "
                f"{c.pk.table_name}.{c.pk.column_name}"
            )
        query = " | ".join(query_parts)
        summary_docs = query_similar_tables(store, query, k=top_k)

    if not summary_docs:
        return "No table summaries available for analysis."

    # Assemble context with budget
    summaries_text = ""
    for doc in summary_docs:
        table_name = doc.metadata.get("table_name", "unknown")
        entry = f"### {table_name}\n{doc.page_content}\n\n"
        if len(summaries_text) + len(entry) > token_budget:
            summaries_text += "... (remaining tables omitted for token budget)\n"
            break
        summaries_text += entry

    relationships_text = _build_relationships_context(report)

    prompt = REDUCE_PROMPT.format(
        table_summaries=summaries_text,
        relationships=relationships_text,
    )

    log.info(
        "REDUCE: sending prompt (%d chars summaries, %d chars relationships)",
        len(summaries_text), len(relationships_text),
    )

    response = await llm.ainvoke(prompt)

    content = response.content
    if isinstance(content, list):
        content = " ".join(
            item.get("text", str(item)) if isinstance(item, dict) else str(item)
            for item in content
        )

    log.info("REDUCE: complete (%d chars)", len(content))
    return content


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _chunk_tables(table_names: list[str], chunk_size: int) -> dict[int, list[str]]:
    """Fallback: split table names into fixed-size sequential chunks."""
    clusters: dict[int, list[str]] = {}
    for i, name in enumerate(table_names):
        clusters.setdefault(i // chunk_size, []).append(name)
    return clusters


# ---------------------------------------------------------------------------
# Phase 3 (large path): CLUSTER — semantic grouping
# ---------------------------------------------------------------------------

def cluster_phase(
    store,
    profiles: list["FileProfile"],
    target_cluster_size: int = 15,
) -> dict[int, list[str]]:
    """Group table summaries into semantic clusters.

    Uses AgglomerativeClustering on stored ChromaDB embedding vectors.
    Falls back to sequential chunking when sklearn is unavailable or
    embeddings cannot be retrieved.

    The cluster count is derived automatically:
        n_clusters = max(2, ceil(n_tables / target_cluster_size))

    Args:
        store: Chroma vector store (after embed_phase).
        profiles: All FileProfile objects (used for count + fallback names).
        target_cluster_size: Desired average tables per cluster.

    Returns:
        Dict mapping cluster_id (int) -> list of table names.
    """
    from file_profiler.agent.vector_store import get_table_embeddings

    n_tables = len(profiles)

    if n_tables <= target_cluster_size:
        log.info("CLUSTER: %d tables ≤ target size %d — single cluster",
                 n_tables, target_cluster_size)
        return {0: [p.table_name for p in profiles]}

    n_clusters = max(2, math.ceil(n_tables / target_cluster_size))
    log.info("CLUSTER: %d tables → %d clusters (target_size=%d)",
             n_tables, n_clusters, target_cluster_size)

    table_names, vectors = get_table_embeddings(store)

    if not table_names or not vectors:
        log.warning("CLUSTER: no embeddings retrieved — falling back to chunking")
        return _chunk_tables([p.table_name for p in profiles], target_cluster_size)

    try:
        import numpy as np
        from sklearn.cluster import AgglomerativeClustering

        X = np.array(vectors, dtype=float)
        norms = np.linalg.norm(X, axis=1, keepdims=True)
        norms[norms == 0] = 1.0
        X_norm = X / norms

        clustering = AgglomerativeClustering(
            n_clusters=min(n_clusters, len(table_names)),
            metric="cosine",
            linkage="average",
        )
        labels = clustering.fit_predict(X_norm)

        clusters: dict[int, list[str]] = {}
        for name, label in zip(table_names, labels):
            clusters.setdefault(int(label), []).append(name)

        # Include any profiles missing from the store in the smallest cluster
        stored_set = set(table_names)
        missing = [p.table_name for p in profiles if p.table_name not in stored_set]
        if missing:
            smallest = min(clusters, key=lambda k: len(clusters[k]))
            clusters[smallest].extend(missing)
            log.debug("CLUSTER: appended %d missing tables to cluster %d",
                      len(missing), smallest)

        log.info("CLUSTER: formed %d clusters from %d tables", len(clusters), n_tables)
        return clusters

    except ImportError:
        log.warning("CLUSTER: sklearn not available — falling back to chunking")
        return _chunk_tables(table_names, target_cluster_size)
    except Exception as exc:
        log.warning("CLUSTER: failed (%s) — falling back to chunking", exc)
        return _chunk_tables(table_names, target_cluster_size)


# ---------------------------------------------------------------------------
# Phase 4 (large path): REDUCE per cluster
# ---------------------------------------------------------------------------

async def reduce_cluster_phase(
    clusters: dict[int, list[str]],
    store,
    report: "RelationshipReport",
    llm,
    token_budget: int = 6000,
    max_workers: int = 4,
) -> dict[int, str]:
    """Run a focused REDUCE prompt for each cluster in parallel.

    Each cluster gets its own LLM call covering only the tables in that
    cluster, which keeps prompt sizes manageable regardless of total table
    count.

    Args:
        clusters: Output of cluster_phase — {cluster_id: [table_names]}.
        store: Chroma vector store (summaries already embedded).
        report: Deterministic relationship report (used to filter intra-cluster FKs).
        llm: LangChain chat model.
        token_budget: Max chars for summaries section in each cluster prompt.
        max_workers: Max concurrent LLM calls across clusters.

    Returns:
        Dict mapping cluster_id -> cluster analysis text.
    """
    from file_profiler.agent.vector_store import get_all_summaries

    # Build lookup: table_name -> summary text
    summary_map: dict[str, str] = {
        doc.metadata.get("table_name", ""): doc.page_content
        for doc in get_all_summaries(store)
        if doc.metadata.get("table_name")
    }

    def _intra_cluster_rels(cluster_tables: list[str]) -> str:
        """Format only the FK candidates whose both sides are in this cluster."""
        cluster_set = set(cluster_tables)
        relevant = [
            c for c in report.candidates
            if c.fk.table_name in cluster_set and c.pk.table_name in cluster_set
        ]
        if not relevant:
            return "No intra-cluster relationships detected."
        lines = [
            f"  {c.fk.table_name}.{c.fk.column_name} → "
            f"{c.pk.table_name}.{c.pk.column_name} "
            f"(confidence={c.confidence:.2f}, overlap={c.top_value_overlap_pct})"
            for c in relevant
        ]
        return "\n".join(lines)

    semaphore = asyncio.Semaphore(max_workers)

    async def _analyze_cluster(cluster_id: int, table_names: list[str]) -> tuple[int, str]:
        summaries_text = ""
        for name in table_names:
            summary = summary_map.get(name, f"Table {name}: no summary available.")
            entry = f"### {name}\n{summary}\n\n"
            if len(summaries_text) + len(entry) > token_budget:
                summaries_text += "... (truncated for token budget)\n"
                break
            summaries_text += entry

        prompt = CLUSTER_REDUCE_PROMPT.format(
            table_summaries=summaries_text,
            relationships=_intra_cluster_rels(table_names),
        )

        try:
            async with semaphore:
                response = await llm.ainvoke(prompt)
            content = response.content
            if isinstance(content, list):
                content = " ".join(
                    item.get("text", str(item)) if isinstance(item, dict) else str(item)
                    for item in content
                )
            log.info("REDUCE cluster %d: %d tables → %d chars",
                     cluster_id, len(table_names), len(content))
            return cluster_id, content
        except Exception as exc:
            log.error("REDUCE cluster %d: failed — %s", cluster_id, exc)
            return cluster_id, f"Cluster {cluster_id} analysis failed: {exc}"

    results = await asyncio.gather(*[
        _analyze_cluster(cid, tables) for cid, tables in clusters.items()
    ])
    return dict(results)


# ---------------------------------------------------------------------------
# Phase 5 (large path): META-REDUCE — cross-cluster synthesis
# ---------------------------------------------------------------------------

async def meta_reduce_phase(
    clusters: dict[int, list[str]],
    cluster_analyses: dict[int, str],
    report: "RelationshipReport",
    llm,
    token_budget: int = 8000,
) -> str:
    """Synthesize all cluster analyses into one comprehensive final report.

    Identifies cross-cluster join paths and produces the full 7-part
    analysis including a complete ER diagram spanning all tables.

    Args:
        clusters: {cluster_id: [table_names]} from cluster_phase.
        cluster_analyses: {cluster_id: analysis_text} from reduce_cluster_phase.
        report: Deterministic relationship report (for cross-cluster FKs).
        llm: LangChain chat model.
        token_budget: Max chars for cluster analyses section of the prompt.

    Returns:
        Final comprehensive enrichment analysis (markdown).
    """
    # Assemble cluster analyses with budget
    analyses_text = ""
    for cid in sorted(clusters):
        tables = clusters[cid]
        analysis = cluster_analyses.get(cid, "No analysis available.")
        preview = ", ".join(tables[:5]) + ("…" if len(tables) > 5 else "")
        entry = (
            f"## Cluster {cid}  ({len(tables)} tables: {preview})\n\n"
            f"{analysis}\n\n---\n\n"
        )
        if len(analyses_text) + len(entry) > token_budget:
            analyses_text += "… (remaining clusters omitted for token budget)\n"
            break
        analyses_text += entry

    # Identify cross-cluster FK candidates
    cluster_map: dict[str, int] = {
        name: cid
        for cid, tables in clusters.items()
        for name in tables
    }
    cross = [
        c for c in report.candidates
        if cluster_map.get(c.fk.table_name, -1) != cluster_map.get(c.pk.table_name, -2)
    ]
    if cross:
        cc_lines = [
            f"  {c.fk.table_name}.{c.fk.column_name} → "
            f"{c.pk.table_name}.{c.pk.column_name} "
            f"(clusters {cluster_map.get(c.fk.table_name, '?')} → "
            f"{cluster_map.get(c.pk.table_name, '?')}, "
            f"confidence={c.confidence:.2f})"
            for c in cross
        ]
        cross_text = f"{len(cross)} cross-cluster relationships:\n" + "\n".join(cc_lines)
    else:
        cross_text = "No cross-cluster relationships detected by the deterministic algorithm."

    prompt = META_REDUCE_PROMPT.format(
        cluster_analyses=analyses_text,
        cross_cluster_relationships=cross_text,
    )

    log.info(
        "META-REDUCE: %d clusters, %d cross-cluster rels, prompt=%d chars",
        len(clusters), len(cross), len(prompt),
    )

    response = await llm.ainvoke(prompt)
    content = response.content
    if isinstance(content, list):
        content = " ".join(
            item.get("text", str(item)) if isinstance(item, dict) else str(item)
            for item in content
        )

    log.info("META-REDUCE: complete (%d chars)", len(content))
    return content


# ---------------------------------------------------------------------------
# Orchestrator — public entry point
# ---------------------------------------------------------------------------

async def enrich(
    profiles: list[FileProfile],
    report: RelationshipReport,
    dir_path: str,
    provider: str = "google",
    model: Optional[str] = None,
    persist_dir: Optional[Path] = None,
    incremental: bool = True,
) -> dict:
    """Run the full enrichment pipeline, auto-scaling to any number of tables.

    **Small datasets** (n_tables ≤ CLUSTER_TARGET_SIZE):
        MAP → EMBED → REDUCE  (single focused prompt, original behaviour)

    **Large datasets** (n_tables > CLUSTER_TARGET_SIZE):
        MAP → EMBED → CLUSTER → REDUCE-per-cluster → META-REDUCE

    The CLUSTER phase groups tables by semantic similarity using
    AgglomerativeClustering on their stored embedding vectors.  Each cluster
    gets its own focused LLM call, and a final META-REDUCE synthesises the
    cross-cluster picture.

    Args:
        profiles: List of FileProfile objects.
        report: RelationshipReport from the deterministic detector.
        dir_path: Path to the data directory (for metadata/logging).
        provider: LLM provider ("google", "groq", "openai", "anthropic").
        model: LLM model override.
        persist_dir: ChromaDB directory. Defaults to config.VECTOR_STORE_DIR.
        incremental: If True, skip tables already summarized in the store.

    Returns:
        Dict with enrichment text and metadata.  Large-dataset results also
        include ``clusters_formed`` and ``cluster_breakdown``.
    """
    from file_profiler.agent.llm_factory import get_llm_with_fallback
    from file_profiler.agent.vector_store import get_or_create_store, get_stored_fingerprints
    from file_profiler.config.env import (
        CLUSTER_TARGET_SIZE,
        MAP_MAX_WORKERS,
        MAP_TOKEN_BUDGET,
        META_REDUCE_TOKEN_BUDGET,
        PER_CLUSTER_TOKEN_BUDGET,
        REDUCE_TOKEN_BUDGET,
        REDUCE_TOP_K,
        VECTOR_STORE_DIR,
    )

    persist_dir = persist_dir or VECTOR_STORE_DIR
    llm = get_llm_with_fallback(provider=provider, model=model)

    # Check what's already in the store (incremental caching)
    existing_fingerprints: dict[str, str] = {}
    if incremental:
        try:
            store = get_or_create_store(persist_dir)
            existing_fingerprints = get_stored_fingerprints(store)
        except Exception:
            existing_fingerprints = {}

    # Phase 1: MAP — per-table LLM summaries
    log.info("=== Phase 1: MAP (%d tables) ===", len(profiles))
    new_summaries = await map_phase(
        profiles, llm,
        max_workers=MAP_MAX_WORKERS,
        token_budget=MAP_TOKEN_BUDGET,
        existing_fingerprints=existing_fingerprints if incremental else None,
    )

    # Phase 2: EMBED — store summaries in ChromaDB
    log.info("=== Phase 2: EMBED ===")
    store = embed_phase(new_summaries, profiles, report, persist_dir)

    cached_count = len(profiles) - len(new_summaries)
    n_tables = len(profiles)

    if n_tables <= CLUSTER_TARGET_SIZE:
        # ── Small dataset path ────────────────────────────────────────────
        log.info("=== Phase 3: REDUCE (direct, %d tables) ===", n_tables)
        enrichment_text = await reduce_phase(
            store, report, profiles, llm,
            top_k=REDUCE_TOP_K,
            token_budget=REDUCE_TOKEN_BUDGET,
        )
        return {
            "enrichment": enrichment_text,
            "documents_embedded": len(new_summaries) + 1,
            "tables_analyzed": n_tables,
            "tables_summarized": len(new_summaries),
            "tables_cached": cached_count,
            "relationships_analyzed": len(report.candidates),
            "clusters_formed": 1,
        }

    else:
        # ── Large dataset path ────────────────────────────────────────────
        log.info("=== Phase 3: CLUSTER (%d tables) ===", n_tables)
        clusters = cluster_phase(store, profiles, target_cluster_size=CLUSTER_TARGET_SIZE)

        log.info("=== Phase 4: REDUCE per cluster (%d clusters) ===", len(clusters))
        cluster_analyses = await reduce_cluster_phase(
            clusters, store, report, llm,
            token_budget=PER_CLUSTER_TOKEN_BUDGET,
            max_workers=MAP_MAX_WORKERS,
        )

        log.info("=== Phase 5: META-REDUCE ===")
        enrichment_text = await meta_reduce_phase(
            clusters, cluster_analyses, report, llm,
            token_budget=META_REDUCE_TOKEN_BUDGET,
        )
        return {
            "enrichment": enrichment_text,
            "documents_embedded": len(new_summaries) + 1,
            "tables_analyzed": n_tables,
            "tables_summarized": len(new_summaries),
            "tables_cached": cached_count,
            "relationships_analyzed": len(report.candidates),
            "clusters_formed": len(clusters),
            "cluster_breakdown": {str(cid): tables for cid, tables in clusters.items()},
        }
