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
import collections
import json
import logging
import math
import tempfile
import time
from pathlib import Path
from typing import Callable, Optional

from file_profiler.models.file_profile import FileProfile
from file_profiler.models.relationships import RelationshipReport
from file_profiler.observability.langsmith import (
    compact_text_output,
    describe_profiles,
    extract_llm_usage,
    resolve_prompt,
    safe_name,
    traceable,
)

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# LLM retry helper — exponential backoff for transient API errors
# ---------------------------------------------------------------------------

_LLM_MAX_RETRIES = 3
_LLM_RETRY_BASE_DELAY = 2.0  # seconds


def _trace_llm_invoke_inputs(inputs: dict) -> dict:
    prompt = inputs.get("prompt", "")
    return {
        "prompt_chars": len(prompt) if isinstance(prompt, str) else 0,
        "estimated_tokens": _estimate_token_count(prompt) if isinstance(prompt, str) else 0,
        "max_retries": inputs.get("max_retries", _LLM_MAX_RETRIES),
        "fallback_provider": inputs.get("fallback_provider"),
        "fallback_model": inputs.get("fallback_model"),
        "fallback_timeout": inputs.get("fallback_timeout", 0),
    }


def _trace_map_table_inputs(inputs: dict) -> dict:
    profile = inputs.get("profile")
    columns = getattr(profile, "columns", []) or []
    return {
        "table": safe_name(getattr(profile, "table_name", ""), kind="table"),
        "row_count": getattr(profile, "row_count", 0),
        "column_count": len(columns),
        "token_budget": inputs.get("token_budget"),
        "fallback_provider": inputs.get("fallback_provider"),
        "fallback_model": inputs.get("fallback_model"),
    }


def _trace_map_table_output(output) -> dict:
    try:
        table_name, summary, col_descs = output
    except Exception:
        return compact_text_output(output)
    return {
        "table": safe_name(table_name, kind="table"),
        "summary_chars": len(summary or ""),
        "columns_described": len(col_descs or {}),
    }


def _trace_map_phase_inputs(inputs: dict) -> dict:
    profiles = inputs.get("profiles") or []
    existing_fingerprints = inputs.get("existing_fingerprints") or {}
    return {
        **describe_profiles(profiles),
        "max_workers": inputs.get("max_workers"),
        "token_budget": inputs.get("token_budget"),
        "provider": inputs.get("provider"),
        "existing_fingerprints": len(existing_fingerprints),
    }


def _trace_map_phase_output(output) -> dict:
    try:
        summaries, column_descriptions = output
    except Exception:
        return compact_text_output(output)
    return {
        "tables_summarized": len(summaries or {}),
        "tables_with_column_descriptions": len(column_descriptions or {}),
        "columns_described": sum(len(cols) for cols in (column_descriptions or {}).values()),
    }


def _trace_reduce_inputs(inputs: dict) -> dict:
    profiles = inputs.get("profiles") or []
    report = inputs.get("report")
    return {
        **describe_profiles(profiles),
        "relationship_candidates": len(getattr(report, "candidates", []) or []),
        "top_k": inputs.get("top_k"),
        "token_budget": inputs.get("token_budget"),
        "provider": inputs.get("provider"),
        "model": inputs.get("model"),
        "discovered_relationships_chars": len(inputs.get("discovered_relationships") or ""),
    }


def _trace_cluster_reduce_inputs(inputs: dict) -> dict:
    clusters = inputs.get("clusters") or {}
    report = inputs.get("report")
    return {
        "cluster_count": len(clusters),
        "largest_cluster": max((len(tables) for tables in clusters.values()), default=0),
        "relationship_candidates": len(getattr(report, "candidates", []) or []),
        "token_budget": inputs.get("token_budget"),
        "max_workers": inputs.get("max_workers"),
        "provider": inputs.get("provider"),
        "model": inputs.get("model"),
    }


def _trace_cluster_reduce_output(output) -> dict:
    if not isinstance(output, dict):
        return compact_text_output(output)
    return {
        "cluster_count": len(output),
        "analysis_chars": sum(len(text or "") for text in output.values()),
    }


def _trace_meta_reduce_inputs(inputs: dict) -> dict:
    clusters = inputs.get("clusters") or {}
    cluster_analyses = inputs.get("cluster_analyses") or {}
    report = inputs.get("report")
    return {
        "cluster_count": len(clusters),
        "cluster_analyses": len(cluster_analyses),
        "analysis_chars": sum(len(text or "") for text in cluster_analyses.values()),
        "relationship_candidates": len(getattr(report, "candidates", []) or []),
        "token_budget": inputs.get("token_budget"),
        "provider": inputs.get("provider"),
        "model": inputs.get("model"),
    }


def _trace_batch_enrich_inputs(inputs: dict) -> dict:
    profiles = inputs.get("profiles") or []
    report = inputs.get("report")
    return {
        **describe_profiles(profiles),
        "relationship_candidates": len(getattr(report, "candidates", []) or []),
        "provider": inputs.get("provider"),
        "model": inputs.get("model"),
        "incremental": inputs.get("incremental"),
    }


def _trace_discover_reduce_inputs(inputs: dict) -> dict:
    profiles = inputs.get("profiles") or []
    report = inputs.get("report")
    return {
        **describe_profiles(profiles),
        "relationship_candidates": len(getattr(report, "candidates", []) or []),
        "provider": inputs.get("provider"),
        "model": inputs.get("model"),
        "skip_reduce": inputs.get("skip_reduce"),
    }


def _fallback_provider(provider: str | None) -> str | None:
    """Return the next provider to try when the current provider/model fails."""
    chain = {
        "google": "groq",
        "groq": "openai",
        "openai": "anthropic",
    }
    if not provider:
        return None
    return chain.get(provider.lower())


def _looks_like_model_availability_error(exc_str: str) -> bool:
    """Return True when an exception looks like a model/deprecation failure."""
    terms = (
        "decommission",
        "deprecated",
        "model not found",
        "unknown model",
        "unsupported model",
        "no such model",
        "does not exist",
        "not available",
        "not found",
    )
    return any(term in exc_str for term in terms)


@traceable(
    name="enrichment.llm_invoke",
    run_type="llm",
    process_inputs=_trace_llm_invoke_inputs,
    process_outputs=compact_text_output,
)
async def _invoke_with_retry(
    llm,
    prompt: str,
    max_retries: int = _LLM_MAX_RETRIES,
    *,
    fallback_provider: str | None = None,
    fallback_model: str | None = None,
    fallback_temperature: float = 0.0,
    fallback_timeout: int = 0,
) -> str:
    """Invoke an LLM with exponential backoff on transient failures.

    Returns the text content of the response.

    Retries on: rate limits (429), server errors (5xx), timeouts,
    and connection errors.  Does NOT retry on auth errors (401/403)
    or malformed request errors (400).
    """
    from file_profiler.agent.llm_factory import get_llm

    last_exc = None
    fallback_used = False
    for attempt in range(max_retries + 1):
        try:
            response = await llm.ainvoke(prompt)
            content = response.content
            if isinstance(content, list):
                content = " ".join(
                    item.get("text", str(item)) if isinstance(item, dict) else str(item)
                    for item in content
                )
            return content
        except Exception as exc:
            last_exc = exc
            exc_str = str(exc).lower()

            # Don't retry on auth errors.
            if any(code in exc_str for code in ("401", "403")):
                raise

            # If the current provider/model is unavailable, switch once to a
            # configured fallback provider/model instead of retrying the same one.
            if (
                fallback_provider
                and not fallback_used
                and _looks_like_model_availability_error(exc_str)
            ):
                log.warning(
                    "LLM model/provider failed (%s); falling back to '%s'",
                    exc, fallback_provider,
                )
                llm = get_llm(
                    provider=fallback_provider,
                    model=fallback_model,
                    temperature=fallback_temperature,
                    timeout=fallback_timeout,
                )
                fallback_used = True
                continue

            # Treat generic invalid request errors as non-recoverable unless the
            # message looks like a model availability problem.
            if any(code in exc_str for code in ("400", "invalid")) and not _looks_like_model_availability_error(exc_str):
                raise

            if attempt < max_retries:
                delay = _LLM_RETRY_BASE_DELAY * (2 ** attempt)
                log.warning(
                    "LLM call failed (attempt %d/%d): %s — retrying in %.1fs",
                    attempt + 1, max_retries + 1, exc, delay,
                )
                await asyncio.sleep(delay)
            else:
                log.error("LLM call failed after %d attempts: %s", max_retries + 1, exc)
                raise last_exc
    
    # Should never reach here, but satisfy type checker
    raise last_exc if last_exc else RuntimeError("LLM invocation failed with no exception recorded")


# ---------------------------------------------------------------------------
# Token counting utility
# ---------------------------------------------------------------------------

def _estimate_token_count(text: str) -> int:
    """Estimate token count for text using a simple approximation.
    
    Uses ~4 characters per token as a rough estimate based on OpenAI's
    guidelines. This is conservative (actual ratio is often 3-3.5) to
    ensure we stay well below limits.
    
    For more accurate counting, install tiktoken and use it directly.
    """
    try:
        # Try using tiktoken if available for accurate token counting
        import tiktoken
        encoding = tiktoken.get_encoding("cl100k_base")  # GPT-4, GPT-3.5-turbo
        return len(encoding.encode(text))
    except (ImportError, Exception):
        # Fallback: use conservative character-based estimate
        # Assume 4 chars per token (conservative; actual is often 3-3.5)
        return len(text) // 4


def _truncate_to_token_limit(text: str, max_tokens: int) -> str:
    """Truncate text to fit within the specified token limit.
    
    Returns the truncated text with a clear indicator if truncation occurred.
    """
    current_tokens = _estimate_token_count(text)
    if current_tokens <= max_tokens:
        return text
    
    # Calculate how much to trim, targeting 95% of limit to add truncation marker
    target_tokens = int(max_tokens * 0.95)
    chars_per_token = len(text) // current_tokens if current_tokens > 0 else 4
    target_chars = target_tokens * chars_per_token
    
    suffix = "\n\n... [CONTENT TRUNCATED DUE TO TOKEN LIMIT] ..."
    if target_chars > len(suffix):
        truncated = text[:target_chars - len(suffix)] + suffix
        log.warning(
            "Prompt truncated from %d to %d tokens (limit: %d)",
            current_tokens, _estimate_token_count(truncated), max_tokens
        )
        return truncated
    
    # If even the suffix won't fit, return what we can
    return text[:target_chars]


# ---------------------------------------------------------------------------
# Rate-limit-aware concurrency
# ---------------------------------------------------------------------------

class _RateLimitedSemaphore:
    """Semaphore + sliding-window RPM limiter.

    Combines asyncio.Semaphore (max concurrent tasks) with a per-minute
    request cap.  When RPM is 0, behaves like a plain Semaphore.
    """

    def __init__(self, max_concurrent: int, rpm: int = 0):
        self._sem = asyncio.Semaphore(max_concurrent)
        self._rpm = rpm
        self._timestamps: collections.deque = collections.deque()
        self._lock = asyncio.Lock()

    async def __aenter__(self):
        await self._sem.acquire()
        if self._rpm > 0:
            async with self._lock:
                now = time.monotonic()
                # Evict timestamps outside the 60s window
                while self._timestamps and now - self._timestamps[0] > 60.0:
                    self._timestamps.popleft()
                # If at RPM limit, wait until the oldest timestamp expires
                if len(self._timestamps) >= self._rpm:
                    sleep_until = self._timestamps[0] + 60.0
                    delay = max(0, sleep_until - now)
                    if delay > 0:
                        log.debug("RPM limit (%d): throttling %.1fs", self._rpm, delay)
                        await asyncio.sleep(delay)
                self._timestamps.append(time.monotonic())
        return self

    async def __aexit__(self, *args):
        self._sem.release()


# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------

MAP_PROMPT = """\
You are a Senior Data and AI Engineer. Analyse this single table profile and produce \
a structured JSON response.

Table profile:
{profile_context}

Respond with ONLY valid JSON in the following format — no markdown fences, no extra text:

{{
  "summary": "<A concise 250-300 word summary covering: what the table likely represents \
(infer from column names and sample values), primary key column(s), notable columns \
(FKs, categoricals, dates, high-null), data quality issues, row count and column count.>",
  "column_descriptions": {{
    "<column_name>": {{
      "type": "<data type: int, string, float, date, timestamp, boolean, uuid, categorical>",
      "role": "<PK | FK | regular>",
      "description": "<1 sentence describing what this column represents, inferred from \
its name, type, sample values, and position in the table>"
    }}
  }}
}}

IMPORTANT:
- Include EVERY column from the table profile in column_descriptions — do not skip any.
- Infer the semantic meaning from column names, sample values, and context.
- For the "role" field: mark as PK if key_candidate=True with high distinctness, \
FK if the name suggests a foreign reference (e.g. ends in _id and is not the PK), \
otherwise "regular". Give preference to columns ending with _id for PK/FK roles, but use your judgment based on the profile
and discard false positives (e.g. created_at should not be a PK just because it has distinct values).
"""

REDUCE_PROMPT = """\
You are a Senior Data and AI Engineer analysing a set of profiled data tables.

You have three relationship signals, listed in **priority order**:
1. **Vector-discovered column similarities** (HIGHEST PRIORITY) — columns that are \
semantically similar across tables based on their descriptions, types, and names. \
These are the most scalable and reliable signal for discovering relationships. \
Always include these in your analysis, even if the deterministic detector missed them.
2. **Column descriptions** — LLM-generated per-column semantic descriptions from the MAP phase.
3. **Deterministic FK candidates** (SUPPORTING SIGNAL) — rule-based candidates from \
name matching, type compatibility, cardinality checks, and value overlap. Use these \
to confirm or supplement the vector-discovered relationships, but do NOT rely on them \
as the sole source of truth — they miss semantic relationships and produce false positives.

## Your task

Produce:

### 1. Table Descriptions
For EVERY table, write a 2-3 sentence semantic description of what the table \
represents in the domain. Infer meaning from column names, sample values, \
and relationships. Do NOT skip any table.

### 2. Column Descriptions
For EVERY table, list ALL columns with:
- Column name
- Data type (e.g. int, string, float, date, timestamp, boolean, uuid)
- Whether it is a PK, FK, or regular column
- A brief description of what the column represents

### 3. Primary Key Assessment
For EVERY table, confirm or revise PK candidates with reasoning. \
If no PK is detected, explain why and suggest candidates.

### 4. Foreign Key Assessment
Review each detected FK. Confirm/reject with reasoning. Suggest missed ones. \
For each FK, clearly state: source_table.column → target_table.column.

### 5. Join Path Recommendations
Recommend JOIN types (INNER, LEFT, RIGHT, FULL) and useful join paths for analytics. \
Show the complete join chain: TableA.col → TableB.col → TableC.col.

### 6. Enriched ER Diagram

Generate a **complete** Mermaid erDiagram. MANDATORY rules:
- Include **EVERY table** — even tables with no detected relationships.
- List **EVERY column** in each table with its data type.
- Annotate PK columns with `PK` and FK columns with `FK`.
- Draw relationship lines between ALL related tables using proper Mermaid cardinality:
  - `||--||` one-to-one
  - `||--o{{` one-to-many
  - `o{{--o{{` many-to-many
- Each relationship line MUST have a descriptive label showing the join columns \
  (e.g. `"order_id → id"`).
- **Audit/tracking columns** (e.g. `LastEditedBy`, `CreatedBy`, `ModifiedBy`) that \
reference a shared person/user table should be listed separately at the end as a \
comment, NOT drawn as relationship lines. They are valid FKs but create visual noise.
- **Do NOT draw bidirectional edges.** Each FK relationship should appear exactly once: \
from the FK side to the PK side.
- Use clean, readable formatting with consistent indentation.

Example format:
```mermaid
erDiagram
    customers {{
        int id PK
        string name
        string email
        timestamp created_at
    }}
    orders {{
        int order_id PK
        int customer_id FK
        float total_amount
        date order_date
    }}
    customers ||--o{{ orders : "customer_id → id"
```

### 7. Data Quality Recommendations
Actionable recommendations based on quality flags and null ratios.

---

## Table Summaries

{table_summaries}

## Column Descriptions (from per-table analysis)

{column_descriptions}

## Discovered Column Relationships (vector similarity — PRIMARY SOURCE)

{discovered_relationships}

## Detected Relationships (deterministic — SUPPORTING SOURCE)

{relationships}

---

IMPORTANT PRIORITY RULES:
- **Vector-discovered similarities are your primary relationship source.** They scale \
across large schemas and capture semantic connections that rule-based detection misses.
- Use deterministic FK candidates to **confirm** vector-discovered relationships and \
to fill gaps, but if they contradict the vector signal, prefer the vector signal.
- When a vector-discovered pair has high similarity (≥ 0.80), treat it as a strong FK \
candidate even if the deterministic detector did not flag it.
- In the ER diagram, include relationships from BOTH sources but label vector-discovered \
ones that the deterministic detector missed.

Be specific — reference column names, sample values, confidence scores.
"""


CLUSTER_REDUCE_PROMPT = """\
You are a Senior Data and AI Engineer analysing a cluster of semantically related tables \
that share a common domain or functional area.

## Table Summaries

{table_summaries}

## Column Descriptions (from per-table analysis)

{column_descriptions}

## Detected Relationships (within this cluster)

{relationships}

---

## Your task

### 1. Cluster Theme
Two sentence: what domain or functional area do these tables represent?

### 2. Primary Key Assessment
For EVERY table in this cluster, identify PK candidates with reasoning.

### 3. Foreign Key Assessment
Identify FK relationships within this cluster. Confirm or reject detected ones. \
Suggest missed ones. For each FK, clearly state: source_table.column → target_table.column.

### 4. Join Paths
Recommended join types (INNER, LEFT, etc.) and paths between tables in this cluster.

### 5. Cluster ER Diagram

Generate a Mermaid erDiagram for this cluster. MANDATORY rules:
- Include **EVERY table** in this cluster — even tables with no relationships.
- List **EVERY column** in each table with its data type.
- Annotate PK columns with `PK` and FK columns with `FK`.
- Draw relationship lines with proper cardinality (`||--||`, `||--o{{`, `o{{--o{{`).
- Each relationship line MUST have a descriptive label (e.g. `"fk_col → pk_col"`).

```mermaid
erDiagram
    (tables in this cluster only, ALL columns, ALL types, PK/FK annotated)
```

### 6. Data Quality Notes
Key quality issues specific to these tables.

Be specific — reference column names, sample values, confidence scores.
"""

META_REDUCE_PROMPT = """\
You are a Senior Data and AI Engineer. You have received per-cluster analyses of a \
large multi-table database schema.

You have relationship signals in **priority order**:
1. **Vector-discovered column similarities** (HIGHEST PRIORITY) — semantically similar \
columns across tables. These scale across large schemas and capture connections that \
rule-based detection misses. Always include these, even if the deterministic detector \
did not flag them.
2. **Cross-cluster deterministic FK candidates** (SUPPORTING) — rule-based candidates. \
Use to confirm or supplement, not as sole source of truth.

## Cluster Analyses

{cluster_analyses}

## Discovered Column Relationships (vector similarity — PRIMARY SOURCE)

{discovered_relationships}

## Cross-Cluster Detected Relationships (deterministic — SUPPORTING SOURCE)

{cross_cluster_relationships}

---

## Your task

Produce the comprehensive final analysis:

### 1. Table Descriptions
For EVERY table (grouped by cluster), write a 2-3 sentence semantic description. \
Do NOT skip any table.

### 2. Column Descriptions
For EVERY table, list ALL columns with their data type and a brief 2 line description. \
Mark PK and FK columns explicitly.

### 3. Primary Key Assessment
Final PK confirmation across ALL tables with reasoning.

### 4. Foreign Key Assessment
ALL FK relationships — intra-cluster and cross-cluster. \
For each FK, clearly state: source_table.column → target_table.column. \
**Prioritise vector-discovered similarities** — if a high-similarity pair (≥ 0.80) \
was not flagged by the deterministic detector, include it as a likely FK. \
Suggest missed ones.

### 5. Join Path Recommendations
Full join paths for analytics, including cross-cluster joins. \
Show the complete chain: TableA.col → TableB.col → TableC.col with JOIN type. \
Prefer join paths backed by vector similarity over purely deterministic ones.

### 6. Complete ER Diagram

Generate a **complete** Mermaid erDiagram. MANDATORY rules:
- Include **EVERY table** from ALL clusters — even tables with no relationships.
- List **EVERY column** in each table with its data type.
- Annotate PK columns with `PK` and FK columns with `FK`.
- Draw relationship lines between ALL related tables using proper cardinality:
  - `||--||` one-to-one
  - `||--o{{` one-to-many
  - `o{{--o{{` many-to-many
- Each relationship line MUST have a descriptive label (e.g. `"fk_col → pk_col"`).
- Include BOTH intra-cluster AND cross-cluster relationships.
- Include vector-discovered relationships even if not in deterministic candidates.

```mermaid
erDiagram
    (ALL tables, ALL columns with types, ALL PK/FK annotations, ALL relationships)
```

### 7. Data Quality Recommendations
Actionable recommendations across all tables.

Be specific — reference column names, sample values, confidence scores.
"""


# ---------------------------------------------------------------------------
# Profile context builder
# ---------------------------------------------------------------------------

def _column_priority(col) -> int:
    """Rank column importance for context budget allocation.

    Lower = higher priority (rendered first, never truncated).
    """
    if col.is_key_candidate:
        return 0  # PK candidates — always include full detail
    name_lower = col.name.lower()
    if name_lower.endswith("_id") or name_lower.endswith("id"):
        return 1  # FK-like columns
    if col.quality_flags:
        return 2  # columns with quality issues are analytically interesting
    return 3  # regular columns


def _compute_adaptive_budget(profile: FileProfile, base_budget: int) -> int:
    """Scale token budget based on column count.

    A 5-column table stays near base_budget.  A 50-column table gets
    enough room to include all columns.  Capped at MAP_TOKEN_BUDGET_MAX.
    """
    from file_profiler.config.env import MAP_TOKEN_BUDGET_MAX
    per_col_chars = 120  # ~120 chars per column line
    adaptive = base_budget + (len(profile.columns) * per_col_chars)
    return min(adaptive, MAP_TOKEN_BUDGET_MAX)


def _render_column_full(col) -> str:
    """Full-detail rendering for priority columns (PK/FK candidates)."""
    flags = ", ".join(f.value for f in col.quality_flags) if col.quality_flags else "none"
    line = (
        f"  - {col.name}: type={col.inferred_type.value}, "
        f"nulls={col.null_count}, distinct={col.distinct_count}, "
        f"key_candidate={col.is_key_candidate}, "
        f"quality=[{flags}]"
    )
    if col.sample_values:
        line += f", samples={col.sample_values[:5]}"
    if col.top_values:
        top = [tv.value for tv in col.top_values[:3]]
        line += f", top_values={top}"
    return line


def _render_column_compact(col) -> str:
    """Compact rendering for regular columns — preserves key signals only."""
    parts = [f"  - {col.name}: {col.inferred_type.value}"]
    if col.null_count:
        parts.append(f"nulls={col.null_count}")
    parts.append(f"distinct={col.distinct_count}")
    if col.sample_values:
        parts.append(f"samples={col.sample_values[:3]}")
    return ", ".join(parts)


def _truncate_text_to_budget(text: str, budget: int) -> str:
    """Hard-cap text length to the supplied character budget."""
    if budget <= 0:
        return ""
    if len(text) <= budget:
        return text
    suffix = "... (truncated)"
    if budget <= len(suffix):
        return text[:budget]
    return text[: budget - len(suffix)] + suffix


def _build_table_context(profile: FileProfile, token_budget: int = 2000) -> str:
    """Build a compact profile context string for one table.

    Uses priority-based column rendering:
    - Tier 1 (PK/FK candidates): full detail — always included
    - Tier 2 (regular columns): compact form — truncated if over budget

    The token_budget is adaptive when called from map_phase (scales with
    column count).  Includes sample values from profiling — no file re-read.
    """
    # Partition columns by priority
    sorted_cols = sorted(profile.columns, key=_column_priority)
    priority_cutoff = 2  # priorities 0-1 get full detail
    priority_cols = [c for c in sorted_cols if _column_priority(c) <= priority_cutoff]
    regular_cols = [c for c in sorted_cols if _column_priority(c) > priority_cutoff]

    # Render priority columns (full detail — never truncated)
    priority_lines = [_render_column_full(c) for c in priority_cols]
    # Render regular columns (compact — may be truncated)
    regular_lines = [_render_column_compact(c) for c in regular_cols]

    header = (
        f"Table: {profile.table_name}\n"
        f"Rows: {profile.row_count}, Columns: {len(profile.columns)}\n"
        f"Format: {profile.file_format.value}\n"
    )

    text = header + "Key columns (PK/FK candidates):\n" + "\n".join(priority_lines) if priority_lines else header
    if regular_lines:
        regular_section = "\nOther columns:\n" + "\n".join(regular_lines)
        # Only truncate the regular section
        remaining_budget = token_budget - len(text) - 100  # reserve for sample rows
        if len(regular_section) > remaining_budget > 0:
            regular_section = regular_section[:remaining_budget - 30] + "\n  ... (truncated)"
        if remaining_budget > 0:
            text += regular_section

    # Build synthetic sample rows from per-column sample_values (no file I/O)
    max_samples = 3
    cols_with_samples = [c for c in profile.columns if c.sample_values]
    if cols_with_samples:
        sample_rows = []
        for row_idx in range(min(max_samples, max(len(c.sample_values) for c in cols_with_samples))):
            row = {}
            for col in cols_with_samples:
                if row_idx < len(col.sample_values):
                    row[col.name] = col.sample_values[row_idx]
            if row:
                sample_rows.append(row)
        if sample_rows:
            rows_str = "\n".join(f"  {json.dumps(r)}" for r in sample_rows)
            sample_section = f"\n\nSample rows (reconstructed):\n{rows_str}"
            # Only add if within budget
            if len(text) + len(sample_section) <= token_budget:
                text += sample_section

    return _truncate_text_to_budget(text, token_budget)


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


def _build_column_descriptions_context(
    all_column_descriptions: dict[str, dict],
) -> str:
    """Format column descriptions for inclusion in the REDUCE prompt context."""
    if not all_column_descriptions:
        return ""

    sections = []
    for table_name in sorted(all_column_descriptions):
        cols = all_column_descriptions[table_name]
        if not cols:
            continue
        lines = [f"### {table_name} — Column Details"]
        for col_name, info in cols.items():
            col_type = info.get("type", "unknown")
            role = info.get("role", "regular")
            desc = info.get("description", "")
            role_tag = f" [{role}]" if role != "regular" else ""
            lines.append(f"  - **{col_name}** ({col_type}){role_tag}: {desc}")
        sections.append("\n".join(lines))

    return "\n\n".join(sections)


def _resolve_writable_output_path(output_path: Path) -> Path:
    """Return a writable output path, falling back to tmp when needed."""
    output_path = Path(output_path)
    try:
        output_path.parent.mkdir(parents=True, exist_ok=True)
        return output_path
    except OSError:
        fallback_dir = Path(tempfile.gettempdir()) / "file_profiler_output"
        fallback_dir.mkdir(parents=True, exist_ok=True)
        fallback_path = fallback_dir / output_path.name
        log.warning(
            "Output path %s unavailable; using fallback %s",
            output_path.parent,
            fallback_dir,
        )
        return fallback_path


def save_enriched_profiles_json(
    profiles: list[FileProfile],
    summaries: dict[str, str],
    all_column_descriptions: dict[str, dict],
    report: RelationshipReport,
    output_path: Path,
) -> None:
    """Save enriched profiles with table summaries and column descriptions to JSON.

    Produces a comprehensive JSON file with per-table structure:
    - table_name, row_count, column_count
    - LLM-generated table summary
    - Per-column: name, inferred_type, role (PK/FK/regular), semantic description
    - Detected relationships involving this table

    Args:
        profiles: List of FileProfile objects.
        summaries: {table_name: summary_text} from MAP phase.
        all_column_descriptions: {table_name: {col_name: {type, role, description}}}.
        report: Deterministic RelationshipReport.
        output_path: Destination JSON file.
    """
    output_path = _resolve_writable_output_path(Path(output_path))

    # Build relationship index: table_name -> list of relationships
    rel_index: dict[str, list[dict]] = {}
    for c in report.candidates:
        for tname in (c.fk.table_name, c.pk.table_name):
            rel_index.setdefault(tname, [])
        rel_index[c.fk.table_name].append({
            "direction": "outgoing_fk",
            "this_column": c.fk.column_name,
            "references_table": c.pk.table_name,
            "references_column": c.pk.column_name,
            "confidence": round(c.confidence, 4),
        })
        rel_index[c.pk.table_name].append({
            "direction": "incoming_fk",
            "from_table": c.fk.table_name,
            "from_column": c.fk.column_name,
            "this_column": c.pk.column_name,
            "confidence": round(c.confidence, 4),
        })

    tables = []
    for p in profiles:
        col_descs = all_column_descriptions.get(p.table_name, {})

        columns = []
        for col in p.columns:
            desc_info = col_descs.get(col.name, {})
            columns.append({
                "name": col.name,
                "inferred_type": col.inferred_type.value,
                "declared_type": col.declared_type,
                "role": desc_info.get("role", "regular"),
                "description": desc_info.get("description", ""),
                "null_count": col.null_count,
                "distinct_count": col.distinct_count,
                "is_key_candidate": col.is_key_candidate,
                "sample_values": col.sample_values[:5] if col.sample_values else [],
            })

        tables.append({
            "table_name": p.table_name,
            "file_path": p.file_path,
            "row_count": p.row_count,
            "column_count": len(p.columns),
            "summary": summaries.get(p.table_name, ""),
            "columns": columns,
            "relationships": rel_index.get(p.table_name, []),
        })

    output_data = {
        "tables_profiled": len(tables),
        "total_columns": sum(len(t["columns"]) for t in tables),
        "total_relationships": len(report.candidates),
        "tables": tables,
    }

    output_path.write_text(
        json.dumps(output_data, indent=2, ensure_ascii=False) + "\n",
        encoding="utf-8",
    )
    log.info("Enriched profiles saved → %s (%d tables, %d columns described)",
             output_path, len(tables),
             sum(1 for t in tables for c in t["columns"] if c["description"]))


# ---------------------------------------------------------------------------
# Phase 1: MAP — per-table LLM summarization
# ---------------------------------------------------------------------------

def _parse_map_response(
    raw: str,
    profile: FileProfile,
) -> tuple[str, dict]:
    """Parse the MAP phase LLM response into (summary, column_descriptions).

    Attempts to extract JSON from the response.  Falls back to treating the
    entire response as a summary with empty column descriptions.

    Returns:
        (summary_text, column_descriptions_dict)
    """
    # Strip markdown fences if the LLM wrapped the JSON
    text = raw.strip()
    if text.startswith("```"):
        # Remove first line (```json or ```) and last line (```)
        lines = text.split("\n")
        text = "\n".join(lines[1:-1]).strip()

    try:
        parsed = json.loads(text)
        summary = parsed.get("summary", "")
        col_descs = parsed.get("column_descriptions", {})
        if not isinstance(col_descs, dict):
            col_descs = {}
        return summary, col_descs
    except (json.JSONDecodeError, AttributeError):
        log.debug("MAP: could not parse JSON from response, treating as plain summary")
        # Fallback: entire response is the summary, generate basic column descriptions
        col_descs = {}
        for col in profile.columns:
            col_descs[col.name] = {
                "type": col.inferred_type.value,
                "role": "PK" if col.is_key_candidate else "regular",
                "description": f"Column '{col.name}' of type {col.inferred_type.value}",
            }
        return text, col_descs


@traceable(
    name="enrichment.map_table",
    run_type="chain",
    process_inputs=_trace_map_table_inputs,
    process_outputs=extract_llm_usage,
)
async def _summarize_one_table(
    profile: FileProfile,
    llm,
    token_budget: int = 2000,
    semaphore: Optional[asyncio.Semaphore | _RateLimitedSemaphore] = None,
    fallback_provider: str | None = None,
    fallback_model: str | None = None,
) -> tuple[str, str, dict]:
    """Summarize a single table using a small LLM prompt.

    Returns:
        Tuple of (table_name, summary_text, column_descriptions).
        On error, summary_text is a fallback description with basic column info.
    """
    adaptive_budget = _compute_adaptive_budget(profile, token_budget)
    context = _build_table_context(profile, adaptive_budget)
    prompt = resolve_prompt("file-profiler/map", MAP_PROMPT).format(
        profile_context=context
    )

    try:
        if semaphore:
            async with semaphore:
                raw = await _invoke_with_retry(
                    llm,
                    prompt,
                    fallback_provider=fallback_provider,
                    fallback_model=fallback_model,
                )
        else:
            raw = await _invoke_with_retry(
                llm,
                prompt,
                fallback_provider=fallback_provider,
                fallback_model=fallback_model,
            )

        summary, col_descs = _parse_map_response(raw, profile)
        log.info("MAP: summarized %s (%d chars, %d columns described)",
                 profile.table_name, len(summary), len(col_descs))
        return profile.table_name, summary, col_descs

    except Exception as exc:
        log.warning("MAP: failed for %s: %s — using fallback", profile.table_name, exc)
        fallback = (
            f"Table {profile.table_name} has {profile.row_count} rows and "
            f"{len(profile.columns)} columns. "
            f"Columns: {', '.join(c.name for c in profile.columns)}."
        )
        col_descs = {
            col.name: {
                "type": col.inferred_type.value,
                "role": "PK" if col.is_key_candidate else "regular",
                "description": f"Column '{col.name}' of type {col.inferred_type.value}",
            }
            for col in profile.columns
        }
        return profile.table_name, fallback, col_descs


@traceable(
    name="enrichment.map_phase",
    run_type="chain",
    process_inputs=_trace_map_phase_inputs,
    process_outputs=_trace_map_phase_output,
)
async def map_phase(
    profiles: list[FileProfile],
    llm,
    max_workers: int = 4,
    token_budget: int = 2000,
    existing_fingerprints: Optional[dict[str, str]] = None,
    on_table_done: Optional[Callable] = None,
    provider: str = "google",
) -> tuple[dict[str, str], dict[str, dict]]:
    """Run the MAP phase: summarize each table in parallel.

    Args:
        profiles: All FileProfile objects.
        llm: LangChain chat model.
        max_workers: Max concurrent LLM calls.
        token_budget: Base per-table context budget in chars (auto-scales
                      with column count via _compute_adaptive_budget).
        existing_fingerprints: {table_name: fingerprint} already in store.
                               Tables with matching fingerprints are skipped.
        on_table_done: Optional async callback(done_count, total_count, table_name)
                       called after each table completes.  Used to send progress
                       updates that keep SSE connections alive during long MAP runs.
        provider: LLM provider name — used to apply provider-specific RPM limits.

    Returns:
        Tuple of:
          - Dict mapping table_name -> summary_text for newly summarized tables.
          - Dict mapping table_name -> column_descriptions for all summarized tables.
            Each column_descriptions is {col_name: {"type", "role", "description"}}.
    """
    from file_profiler.agent.vector_store import _table_fingerprint
    from file_profiler.config.env import PROVIDER_RPM

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
        return {}, {}

    # Scale concurrency: use configured max but cap at table count to avoid
    # idle semaphore slots, and ensure at least 2 for small batches.
    effective_workers = max(2, min(max_workers, len(to_summarize)))
    rpm = PROVIDER_RPM.get(provider.lower(), 0)
    log.info("MAP: summarizing %d/%d tables (workers=%d, rpm_limit=%s)",
             len(to_summarize), len(profiles), effective_workers,
             rpm if rpm > 0 else "unlimited")

    semaphore = _RateLimitedSemaphore(effective_workers, rpm)
    fallback_provider = _fallback_provider(provider)
    summaries = {}
    all_column_descriptions: dict[str, dict] = {}
    done_count = 0
    total = len(to_summarize)

    async def _summarize_and_track(profile: FileProfile):
        nonlocal done_count
        result = await _summarize_one_table(
            profile,
            llm,
            token_budget,
            semaphore,
            fallback_provider=fallback_provider,
        )
        done_count += 1
        if not isinstance(result, Exception):
            table_name, summary, col_descs = result
            summaries[table_name] = summary
            all_column_descriptions[table_name] = col_descs
            if on_table_done:
                await on_table_done(done_count, total, table_name)
        else:
            log.error("MAP: unexpected error: %s", result)
            if on_table_done:
                await on_table_done(done_count, total, f"error: {result}")
        return result

    # --- Memory-efficient streaming: bounded in-flight tasks ---
    # Instead of creating all coroutines upfront via asyncio.gather,
    # limit in-flight tasks to avoid memory pressure with 200+ tables.
    inflight_limit = effective_workers * 2
    pending: set[asyncio.Task] = set()

    for profile in to_summarize:
        # If at the inflight limit, wait for one to finish before adding more
        if len(pending) >= inflight_limit:
            done, pending = await asyncio.wait(
                pending, return_when=asyncio.FIRST_COMPLETED
            )
        pending.add(asyncio.create_task(_summarize_and_track(profile)))

    # Drain remaining tasks
    if pending:
        await asyncio.wait(pending)

    log.info("MAP: completed %d summaries with column descriptions", len(summaries))
    return summaries, all_column_descriptions


# ---------------------------------------------------------------------------
# Phase 2: EMBED — store summaries in persistent vector DB
# ---------------------------------------------------------------------------

def embed_phase(
    summaries: dict[str, str],
    profiles: list[FileProfile],
    report: RelationshipReport,
    all_column_descriptions: Optional[dict[str, dict]] = None,
    persist_dir: Optional[Path] = None,
):
    """Store table summaries, column descriptions, and relationship doc in ChromaDB.

    Upserts each summary and column description (idempotent).  Also stores
    the deterministic relationship report as a separate document.

    Returns:
        Tuple of (table_store, column_store).

    Backward compatibility:
        Legacy callers may pass only four args where the 4th arg is persist_dir.
        In that mode, returns only table_store to preserve legacy behavior.
    """
    from file_profiler.agent.vector_store import (
        _table_fingerprint,
        batch_upsert_column_descriptions,
        batch_upsert_table_summaries,
        get_or_create_store,
        get_or_create_column_store,
        upsert_relationship_candidates,
        upsert_relationship_doc,
    )

    legacy_return_store_only = False
    if persist_dir is None:
        if isinstance(all_column_descriptions, (Path, str)):
            persist_dir = Path(all_column_descriptions)
            all_column_descriptions = {}
            legacy_return_store_only = True
        else:
            raise TypeError("persist_dir is required")

    if all_column_descriptions is None:
        all_column_descriptions = {}

    store = get_or_create_store(persist_dir)

    # Build a lookup for profile metadata
    profile_map = {p.table_name: p for p in profiles}

    # Batch-upsert all table summaries in one embedding + insert call
    metadata_map = {}
    for table_name in summaries:
        p = profile_map.get(table_name)
        if p:
            metadata_map[table_name] = {
                "row_count": p.row_count,
                "column_count": len(p.columns),
                "fingerprint": _table_fingerprint(
                    p.table_name, p.row_count, len(p.columns),
                ),
            }
    n_summaries = batch_upsert_table_summaries(store, summaries, metadata_map)

    # Store the deterministic relationship report
    rel_text = _build_relationships_context(report)
    upsert_relationship_doc(store, rel_text, {
        "candidate_count": len(report.candidates),
        "tables_analyzed": report.tables_analyzed,
    })

    # Store per-candidate structured documents with column profile context
    n_rel_docs = upsert_relationship_candidates(store, report, profiles)
    log.info(
        "EMBED: batch-upserted %d summaries + relationship doc + %d per-candidate docs",
        n_summaries, n_rel_docs,
    )

    # Batch-upsert all column descriptions across all tables in one call
    column_store = get_or_create_column_store(persist_dir)
    total_cols = batch_upsert_column_descriptions(
        column_store, all_column_descriptions, profile_map,
    )

    log.info("EMBED: batch-upserted %d column descriptions across %d tables",
             total_cols, len(all_column_descriptions))

    if legacy_return_store_only:
        return store
    return store, column_store


# ---------------------------------------------------------------------------
# Phase 3: REDUCE — cross-table LLM analysis
# ---------------------------------------------------------------------------

@traceable(
    name="enrichment.reduce",
    run_type="chain",
    process_inputs=_trace_reduce_inputs,
    process_outputs=extract_llm_usage,
)
async def reduce_phase(
    store,
    report: RelationshipReport,
    profiles: list[FileProfile],
    llm,
    all_column_descriptions: Optional[dict[str, dict]] = None,
    discovered_relationships: str = "",
    top_k: int = 15,
    token_budget: int = 12000,
    provider: str | None = None,
    model: str | None = None,
) -> str:
    """Run the REDUCE phase: cross-table relationship analysis.

    For small datasets (<= top_k tables), retrieves all summaries.
    For larger datasets, uses semantic search for the most relevant subset.

    Args:
        all_column_descriptions: {table_name: {col_name: {type, role, description}}}
            from the MAP phase — included in the REDUCE prompt for richer context.
        discovered_relationships: Formatted string of vector-discovered column pairs.

    Returns:
        Full LLM analysis text (markdown with ER diagram, etc.).
    """
    from file_profiler.agent.vector_store import (
        get_all_summaries,
        query_similar_tables,
    )

    # Auto-scale budget based on table count
    token_budget = _scale_budget(token_budget, len(profiles))

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
    col_desc_text = _build_column_descriptions_context(all_column_descriptions or {})

    prompt = resolve_prompt("file-profiler/reduce", REDUCE_PROMPT).format(
        table_summaries=summaries_text,
        column_descriptions=col_desc_text or "No column descriptions available.",
        relationships=relationships_text,
        discovered_relationships=discovered_relationships or "None discovered.",
    )

    # Enforce hard token limit to prevent context window overflow
    from file_profiler.config.env import MAX_INPUT_TOKENS
    estimated_tokens = _estimate_token_count(prompt)
    
    log.info(
        "REDUCE: sending prompt (%d chars summaries, %d chars col_descs, %d chars relationships, %d estimated tokens)",
        len(summaries_text), len(col_desc_text), len(relationships_text), estimated_tokens,
    )
    
    # Truncate if exceeds max input tokens
    if estimated_tokens > MAX_INPUT_TOKENS:
        log.warning(
            "REDUCE prompt exceeds MAX_INPUT_TOKENS (%d > %d), truncating...",
            estimated_tokens, MAX_INPUT_TOKENS
        )
        prompt = _truncate_to_token_limit(prompt, MAX_INPUT_TOKENS)

    content = await _invoke_with_retry(
        llm,
        prompt,
        fallback_provider=_fallback_provider(provider),
        fallback_model=model,
    )

    log.info("REDUCE: complete (%d chars)", len(content))
    return content


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _scale_budget(base: int, n_items: int, min_per_item: int = 400) -> int:
    """Scale token budget based on item count, with floor and ceiling.

    Ensures each table/cluster gets at least min_per_item chars in the
    prompt.  The base value (from env config) acts as a floor.
    Capped at 80K to stay within LLM context limits.
    """
    needed = n_items * min_per_item
    return min(max(base, needed), 80_000)


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

@traceable(
    name="enrichment.cluster_reduce",
    run_type="chain",
    process_inputs=_trace_cluster_reduce_inputs,
    process_outputs=_trace_cluster_reduce_output,
)
async def reduce_cluster_phase(
    clusters: dict[int, list[str]],
    store,
    report: "RelationshipReport",
    llm,
    all_column_descriptions: Optional[dict[str, dict]] = None,
    token_budget: int = 6000,
    max_workers: int = 4,
    provider: str | None = None,
    model: str | None = None,
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
        all_column_descriptions: {table_name: {col_name: {type, role, description}}}
            from the MAP phase.
        token_budget: Max chars for summaries section in each cluster prompt.
        max_workers: Max concurrent LLM calls across clusters.

    Returns:
        Dict mapping cluster_id -> cluster analysis text.
    """
    from file_profiler.agent.vector_store import get_all_summaries

    all_column_descriptions = all_column_descriptions or {}

    # Auto-scale budget by the largest cluster size
    max_cluster_size = max(len(t) for t in clusters.values()) if clusters else 1
    token_budget = _scale_budget(token_budget, max_cluster_size)

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

        # Build column descriptions for just this cluster's tables
        cluster_col_descs = {
            t: all_column_descriptions[t]
            for t in table_names if t in all_column_descriptions
        }
        col_desc_text = _build_column_descriptions_context(cluster_col_descs)

        prompt = resolve_prompt(
            "file-profiler/cluster_reduce", CLUSTER_REDUCE_PROMPT
        ).format(
            table_summaries=summaries_text,
            column_descriptions=col_desc_text or "No column descriptions available.",
            relationships=_intra_cluster_rels(table_names),
        )

        # Enforce hard token limit
        from file_profiler.config.env import MAX_INPUT_TOKENS
        estimated_tokens = _estimate_token_count(prompt)
        if estimated_tokens > MAX_INPUT_TOKENS:
            log.warning(
                "CLUSTER REDUCE cluster %d prompt exceeds MAX_INPUT_TOKENS (%d > %d), truncating...",
                cluster_id, estimated_tokens, MAX_INPUT_TOKENS
            )
            prompt = _truncate_to_token_limit(prompt, MAX_INPUT_TOKENS)

        try:
            async with semaphore:
                content = await _invoke_with_retry(
                    llm,
                    prompt,
                    fallback_provider=_fallback_provider(provider),
                    fallback_model=model,
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

@traceable(
    name="enrichment.meta_reduce",
    run_type="chain",
    process_inputs=_trace_meta_reduce_inputs,
    process_outputs=compact_text_output,
)
async def meta_reduce_phase(
    clusters: dict[int, list[str]],
    cluster_analyses: dict[int, str],
    report: "RelationshipReport",
    llm,
    discovered_relationships: str = "",
    token_budget: int = 8000,
    provider: str | None = None,
    model: str | None = None,
) -> str:
    """Synthesize all cluster analyses into one comprehensive final report.

    Identifies cross-cluster join paths and produces the full 7-part
    analysis including a complete ER diagram spanning all tables.

    Args:
        clusters: {cluster_id: [table_names]} from cluster_phase.
        cluster_analyses: {cluster_id: analysis_text} from reduce_cluster_phase.
        report: Deterministic relationship report (for cross-cluster FKs).
        llm: LangChain chat model.
        discovered_relationships: Formatted string of vector-discovered column pairs.
        token_budget: Max chars for cluster analyses section of the prompt.

    Returns:
        Final comprehensive enrichment analysis (markdown).
    """
    # Auto-scale budget: each cluster analysis can be ~1500 chars
    token_budget = _scale_budget(token_budget, len(clusters), min_per_item=1500)

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

    prompt = resolve_prompt("file-profiler/meta_reduce", META_REDUCE_PROMPT).format(
        cluster_analyses=analyses_text,
        cross_cluster_relationships=cross_text,
        discovered_relationships=discovered_relationships or "None discovered.",
    )

    # Enforce hard token limit
    from file_profiler.config.env import MAX_INPUT_TOKENS
    estimated_tokens = _estimate_token_count(prompt)
    
    log.info(
        "META-REDUCE: %d clusters, %d cross-cluster rels, prompt=%d chars, %d estimated tokens",
        len(clusters), len(cross), len(prompt), estimated_tokens,
    )
    
    # Truncate if exceeds max input tokens
    if estimated_tokens > MAX_INPUT_TOKENS:
        log.warning(
            "META-REDUCE prompt exceeds MAX_INPUT_TOKENS (%d > %d), truncating...",
            estimated_tokens, MAX_INPUT_TOKENS
        )
        prompt = _truncate_to_token_limit(prompt, MAX_INPUT_TOKENS)

    content = await _invoke_with_retry(
        llm,
        prompt,
        fallback_provider=_fallback_provider(provider),
        fallback_model=model,
    )

    log.info("META-REDUCE: complete (%d chars)", len(content))
    return content


# ---------------------------------------------------------------------------
# Enriched ER diagram extraction & persistence
# ---------------------------------------------------------------------------

import re

_MERMAID_BLOCK_RE = re.compile(
    r"```mermaid\s*\n(.*?)```",
    re.DOTALL,
)


def extract_enriched_er_diagram(enrichment_text: str) -> str | None:
    """Extract the last (most complete) Mermaid erDiagram block from enrichment text.

    The enrichment markdown may contain multiple mermaid blocks (e.g. per-cluster
    diagrams followed by a complete one).  We return the *last* block that contains
    ``erDiagram``, which is typically the final comprehensive diagram.

    Returns:
        The mermaid block content (without the ``` fences), or None if not found.
    """
    blocks = _MERMAID_BLOCK_RE.findall(enrichment_text)
    # Filter to only erDiagram blocks (skip sequence diagrams, etc.)
    er_blocks = [b.strip() for b in blocks if "erDiagram" in b]
    if not er_blocks:
        return None
    # Return the last one — it's the most complete (meta-reduce / final reduce)
    return er_blocks[-1]


def save_enriched_er_diagram(enrichment_text: str, output_path: Path) -> Path | None:
    """Extract the enriched ER diagram and write it to a markdown file.

    Args:
        enrichment_text: Full enrichment markdown from the LLM.
        output_path: Destination file path (e.g. data/output/enriched_er_diagram.md).

    Returns:
        The output path if saved successfully, None if no diagram found.
    """
    diagram = extract_enriched_er_diagram(enrichment_text)
    if not diagram:
        log.warning("No enriched ER diagram found in enrichment text")
        return None

    output_path = _resolve_writable_output_path(Path(output_path))

    content = f"# Enriched ER Diagram\n\n```mermaid\n{diagram}\n```\n"
    output_path.write_text(content, encoding="utf-8")
    log.info("Enriched ER diagram saved → %s", output_path)
    return output_path


# ---------------------------------------------------------------------------
# Write descriptions back to FileProfile objects and re-save JSONs
# ---------------------------------------------------------------------------

def _apply_descriptions_to_profiles(
    profiles: list[FileProfile],
    summaries: dict[str, str],
    all_column_descriptions: dict[str, dict],
    output_dir: Path,
) -> None:
    """Write LLM-generated descriptions back into FileProfile objects and re-save JSONs.

    Mutates profiles in place: sets FileProfile.description and
    ColumnProfile.description for every column that has a description.
    Then re-writes the per-table JSON files in output_dir.
    """
    from file_profiler.output.profile_writer import write as write_profile

    for profile in profiles:
        # Table-level description
        if profile.table_name in summaries:
            profile.description = summaries[profile.table_name]

        # Column-level descriptions
        col_descs = all_column_descriptions.get(profile.table_name, {})
        for col in profile.columns:
            desc_info = col_descs.get(col.name, {})
            if desc_info.get("description"):
                col.description = desc_info["description"]

        # Re-save the profile JSON with descriptions included
        if output_dir:
            output_path = output_dir / f"{profile.table_name}_profile.json"
            try:
                write_profile(profile, output_path)
                log.debug("Re-saved profile with descriptions: %s", output_path)
            except Exception as exc:
                log.warning("Could not re-save profile %s: %s",
                            profile.table_name, exc)


def _build_discovered_relationships_context(
    discovered: list[dict],
) -> str:
    """Format vector-discovered column relationships for the REDUCE prompt."""
    if not discovered:
        return "No additional column relationships discovered via semantic similarity."

    lines = [
        f"Discovered {len(discovered)} semantically similar column pairs "
        f"via vector embeddings:"
    ]
    for d in discovered[:50]:  # Cap at 50 to avoid prompt bloat
        lines.append(
            f"  {d['source_table']}.{d['source_column']} ↔ "
            f"{d['target_table']}.{d['target_column']} "
            f"(similarity={d['similarity_score']:.2f})"
        )
    return "\n".join(lines)


def summarize_column_clusters(
    column_clusters: dict[int, list[dict]],
    cluster_derived_rels: list[dict],
    profiles: list[FileProfile],
) -> list:
    """Generate natural-language summaries for each column cluster (Phase 4).

    Template-based — no LLM required. Uses PK/FK assignments from
    derive_relationships_from_clusters() plus profile metadata to produce
    a human-readable description of each cluster's semantic role.

    Args:
        column_clusters: {cluster_id: [col_info_dicts]} from cluster_columns_dbscan().
        cluster_derived_rels: list of PK/FK dicts from derive_relationships_from_clusters().
        profiles: FileProfile list for column metadata (unique_ratio, is_key_candidate).

    Returns:
        List of ClusterSummary objects, one per cluster.
    """
    from file_profiler.models.file_profile import ClusterSummary

    # Build (table, column) → ColumnProfile lookup
    col_lookup: dict[tuple[str, str], object] = {}
    for p in profiles:
        for col in p.columns:
            col_lookup[(p.table_name, col.name)] = col

    # Build cluster_id → {pk, fks} from derived relationships
    rel_by_cluster: dict[int, dict] = {}
    for rel in cluster_derived_rels:
        cid = rel["cluster_id"]
        if cid not in rel_by_cluster:
            rel_by_cluster[cid] = {"pk": None, "fks": []}
        if rel_by_cluster[cid]["pk"] is None:
            rel_by_cluster[cid]["pk"] = {
                "table_name": rel["pk_table"],
                "column_name": rel["pk_column"],
            }
        rel_by_cluster[cid]["fks"].append({
            "table_name": rel["fk_table"],
            "column_name": rel["fk_column"],
            "confidence": rel["confidence"],
        })

    summaries: list = []

    for cluster_id, members in column_clusters.items():
        cluster_rels = rel_by_cluster.get(cluster_id)

        if cluster_rels and cluster_rels["pk"]:
            # pk_fk cluster — has a clear primary key with FK references
            pk = cluster_rels["pk"]
            fks = cluster_rels["fks"]
            pk_col = col_lookup.get((pk["table_name"], pk["column_name"]))

            pk_details = []
            if pk_col:
                if getattr(pk_col, "is_key_candidate", False):
                    pk_details.append("key_candidate=True")
                ur = getattr(pk_col, "unique_ratio", None)
                if ur is not None:
                    pk_details.append(f"{ur * 100:.0f}% distinct")
            pk_detail_str = f" ({', '.join(pk_details)})" if pk_details else ""

            entity = pk["table_name"].rstrip("s").replace("_", " ")
            fk_list = ", ".join(
                f"{fk['table_name']}.{fk['column_name']}" for fk in fks[:10]
            )
            summary_text = (
                f"{entity.title()} identifier cluster. "
                f"{pk['table_name']}.{pk['column_name']} is the PK{pk_detail_str}. "
                f"FK references: {fk_list}."
            )
            summaries.append(ClusterSummary(
                cluster_id=cluster_id,
                cluster_type="pk_fk",
                summary_text=summary_text,
                pk_member=pk,
                fk_members=[
                    {"table_name": fk["table_name"], "column_name": fk["column_name"]}
                    for fk in fks
                ],
            ))
        else:
            # No PK/FK found — shared attribute domain (dates, statuses, metrics)
            tables_in_cluster = list({m["table_name"] for m in members})
            col_names = [m["column_name"] for m in members]
            col_types = list({m.get("column_type", "") for m in members if m.get("column_type")})

            tables_str = ", ".join(tables_in_cluster[:5])
            col_str = ", ".join(col_names[:5])
            types_str = ", ".join(col_types) if col_types else "mixed"
            summary_text = (
                f"Shared attribute domain across [{tables_str}]. "
                f"Columns [{col_str}] represent the same concept ({types_str}) "
                f"but are not FK relationships."
            )
            summaries.append(ClusterSummary(
                cluster_id=cluster_id,
                cluster_type="shared_attribute",
                summary_text=summary_text,
                pk_member=None,
                fk_members=[],
            ))

    n_pk_fk = sum(1 for s in summaries if s.cluster_type == "pk_fk")
    n_shared = sum(1 for s in summaries if s.cluster_type == "shared_attribute")
    log.info(
        "PHASE 4 (SUMMARIZE): %d clusters → %d summaries (%d pk_fk, %d shared_attribute)",
        len(column_clusters), len(summaries), n_pk_fk, n_shared,
    )
    return summaries


def _build_cluster_context(cluster_summaries: list) -> str:
    """Format column cluster summaries for inclusion in REDUCE prompts.

    Returns a compact, newline-delimited block listing each cluster's
    semantic role — ready to be appended to the discovered_relationships context.
    """
    if not cluster_summaries:
        return ""

    lines = [f"\nColumn cluster summaries ({len(cluster_summaries)} clusters):"]
    for cs in cluster_summaries:
        lines.append(
            f"  [Cluster {cs.cluster_id} / {cs.cluster_type}] {cs.summary_text}"
        )
    return "\n".join(lines)


def _build_cluster_derived_relationships_context(
    cluster_relationships: list[dict],
) -> str:
    """Format PK/FK relationships derived from column clustering for prompts.

    These are higher-signal than raw column pairs because they include
    PK/FK directionality and confidence scores.
    """
    if not cluster_relationships:
        return ""

    lines = [
        f"\nCluster-derived PK/FK relationships ({len(cluster_relationships)} found):"
    ]
    for r in cluster_relationships[:50]:
        lines.append(
            f"  {r['fk_table']}.{r['fk_column']} -> "
            f"{r['pk_table']}.{r['pk_column']} "
            f"(confidence={r['confidence']:.2f}, cluster={r['cluster_id']})"
        )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Batch enrichment — MAP + APPLY + EMBED only (no DISCOVER/REDUCE)
# ---------------------------------------------------------------------------

@traceable(
    name="enrichment.batch",
    run_type="chain",
    process_inputs=_trace_batch_enrich_inputs,
    process_outputs=compact_text_output,
)
async def batch_enrich(
    profiles: list[FileProfile],
    report: RelationshipReport,
    dir_path: str,
    provider: str = "google",
    model: Optional[str] = None,
    persist_dir: Optional[Path] = None,
    incremental: bool = True,
    on_table_done: Optional[Callable] = None,
) -> dict:
    """Run MAP + APPLY + EMBED phases for a batch of profiles.

    Designed to be called in batches to avoid SSE timeouts on large datasets.
    After all batches complete, call discover_and_reduce_pipeline() for final synthesis.

    Args:
        on_table_done: Optional async callback(done_count, total, table_name)
                       forwarded to map_phase to keep SSE alive during LLM calls.

    Returns:
        Dict with batch status metadata.
    """
    from file_profiler.agent.llm_factory import get_llm_with_fallback
    from file_profiler.agent.vector_store import (
        get_or_create_store,
        get_stored_fingerprints,
    )
    from file_profiler.config.env import (
        MAP_MAX_WORKERS,
        MAP_TOKEN_BUDGET,
        OUTPUT_DIR,
        VECTOR_STORE_DIR,
    )

    persist_dir = persist_dir or VECTOR_STORE_DIR
    llm = get_llm_with_fallback(provider=provider, model=model)

    existing_fingerprints: dict[str, str] = {}
    if incremental:
        try:
            store = get_or_create_store(persist_dir)
            existing_fingerprints = get_stored_fingerprints(store)
        except Exception as exc:
            log.warning("Could not load fingerprints for incremental mode: %s", exc)
            existing_fingerprints = {}

    # Phase 1: MAP
    log.info("=== BATCH MAP (%d tables) ===", len(profiles))
    new_summaries, all_column_descriptions = await map_phase(
        profiles, llm,
        max_workers=MAP_MAX_WORKERS,
        token_budget=MAP_TOKEN_BUDGET,
        existing_fingerprints=existing_fingerprints if incremental else None,
        on_table_done=on_table_done,
        provider=provider,
    )

    # Phase 2: APPLY — deferred to caller for batched mode; only update
    # in-memory profile objects here (no JSON re-write per batch).
    for profile in profiles:
        if profile.table_name in new_summaries:
            profile.description = new_summaries[profile.table_name]
        col_descs = all_column_descriptions.get(profile.table_name, {})
        for col in profile.columns:
            desc_info = col_descs.get(col.name, {})
            if desc_info.get("description"):
                col.description = desc_info["description"]

    # Phase 3: EMBED (run in thread to avoid blocking the event loop)
    log.info("=== BATCH EMBED ===")
    result = await asyncio.to_thread(
        embed_phase,
        new_summaries, profiles, report, all_column_descriptions, persist_dir,
    )
    # embed_phase returns tuple when all params provided (non-legacy mode)
    if isinstance(result, tuple):
        store, column_store = result
    else:
        # Legacy return mode (should not happen in batch_enrich, but handle gracefully)
        store = result
        column_store = None

    return {
        "batch_tables": len(profiles),
        "tables_summarized": len(new_summaries),
        "tables_cached": len(profiles) - len(new_summaries),
        "columns_described": sum(len(v) for v in all_column_descriptions.values()),
        "column_descriptions": all_column_descriptions,
        "status": "embedded",
    }


# ---------------------------------------------------------------------------
# Discover + Reduce — final synthesis after all batches embedded
# ---------------------------------------------------------------------------

@traceable(
    name="enrichment.discover_reduce_pipeline",
    run_type="chain",
    process_inputs=_trace_discover_reduce_inputs,
    process_outputs=compact_text_output,
)
async def discover_and_reduce_pipeline(
    profiles: list[FileProfile],
    report: RelationshipReport,
    dir_path: str,
    provider: str = "google",
    model: Optional[str] = None,
    persist_dir: Optional[Path] = None,
    output_dir: Optional[Path] = None,
    on_phase_done: Optional[Callable] = None,
    skip_reduce: bool = False,
) -> dict:
    """Run DISCOVER + REDUCE phases after all batches have been embedded.

    Call this once after all batch_enrich() calls have completed.

    Args:
        on_phase_done: Optional async callback(step_index: int, step_name: str, detail: str).
            Called when each pipeline phase actually completes, enabling
            real-time progress tracking tied to actual work.
        skip_reduce: If True and enriched_er_diagram.md already exists on disk,
            skip the expensive REDUCE LLM call and reuse existing output.

    Returns:
        Dict with enrichment analysis and metadata.
    """
    from file_profiler.agent.llm_factory import get_reduce_llm
    from file_profiler.agent.vector_store import (
        cluster_by_column_affinity,
        cluster_columns_dbscan,
        derive_relationships_from_clusters,
        get_or_create_cluster_store,
        get_or_create_column_store,
        get_or_create_store,
        get_all_summaries,
        upsert_cluster_summary,
    )
    from file_profiler.config.env import (
        CLUSTER_TARGET_SIZE,
        COLUMN_AFFINITY_THRESHOLD,
        MAP_MAX_WORKERS,
        META_REDUCE_TOKEN_BUDGET,
        OUTPUT_DIR,
        PER_CLUSTER_TOKEN_BUDGET,
        REDUCE_TOKEN_BUDGET,
        REDUCE_TOP_K,
        VECTOR_STORE_DIR,
    )

    persist_dir = persist_dir or VECTOR_STORE_DIR
    output_dir = output_dir or OUTPUT_DIR
    llm = get_reduce_llm(provider=provider, model=model)

    store = get_or_create_store(persist_dir)
    column_store = get_or_create_column_store(persist_dir)

    table_names = [p.table_name for p in profiles]

    # Helper: fire the progress callback if provided
    async def _phase(step: int, name: str, detail: str = "") -> None:
        if on_phase_done:
            await on_phase_done(step, name, detail)

    # --- Phase 3: Column-level DBSCAN clustering ---
    # Cluster individual columns across tables by semantic similarity.
    # Columns describing the same concept (e.g. customer IDs) land in
    # one cluster regardless of table, enabling PK/FK discovery.
    log.info("=== COLUMN CLUSTER (DBSCAN, %d tables) ===", len(table_names))
    column_clusters, column_singletons = await asyncio.to_thread(
        cluster_columns_dbscan,
        column_store, table_names,
        1.0 - COLUMN_AFFINITY_THRESHOLD,  # convert similarity threshold to distance
        2,  # min_samples
    )
    await _phase(5, "COLUMN CLUSTER: DBSCAN grouping",
                 f"{len(column_clusters)} clusters, {len(column_singletons)} singletons")

    # --- Phase 5: Derive PK/FK relationships from column clusters ---
    cluster_derived_rels: list[dict] = []
    if column_clusters:
        log.info("=== DERIVE RELATIONSHIPS (%d column clusters) ===", len(column_clusters))
        cluster_derived_rels = await asyncio.to_thread(
            derive_relationships_from_clusters,
            column_clusters, profiles,
        )
    await _phase(6, "DERIVE: PK/FK from clusters",
                 f"{len(cluster_derived_rels)} relationships derived")

    # --- Phase 4: Summarize column clusters ---
    # Generate natural-language summaries for each cluster and persist them
    # in a separate ChromaDB collection for retrieval in future sessions.
    cluster_summaries: list = []
    if column_clusters:
        log.info("=== PHASE 4: SUMMARIZE CLUSTERS (%d clusters) ===", len(column_clusters))
        cluster_store = get_or_create_cluster_store(persist_dir)
        cluster_summaries = summarize_column_clusters(
            column_clusters, cluster_derived_rels, profiles,
        )
        for cs in cluster_summaries:
            upsert_cluster_summary(
                cluster_store,
                cs.cluster_id,
                cs.summary_text,
                {"cluster_type": cs.cluster_type, "member_count": len(column_clusters[cs.cluster_id])},
            )
        log.info("PHASE 4: stored %d cluster summaries", len(cluster_summaries))
    await _phase(7, "SUMMARIZE: cluster narratives",
                 f"{len(cluster_summaries)} summaries stored")

    # --- Table-level clustering (for prompt management on large datasets) ---
    # Still use column-affinity-based table clustering to decide how to
    # partition tables into REDUCE prompts.
    log.info("=== TABLE CLUSTER (column affinity, %d tables) ===", len(table_names))
    table_clusters, discovered_rels = await asyncio.to_thread(
        cluster_by_column_affinity,
        column_store, table_names,
        CLUSTER_TARGET_SIZE,
        COLUMN_AFFINITY_THRESHOLD,
    )
    await _phase(7, "TABLE CLUSTER: Affinity grouping",
                 f"{len(table_clusters)} table clusters, {len(discovered_rels)} column pairs")

    # Combine all relationship signals for the REDUCE prompt
    discovered_context = _build_discovered_relationships_context(discovered_rels)
    cluster_rel_context = _build_cluster_derived_relationships_context(cluster_derived_rels)
    if cluster_rel_context:
        discovered_context = discovered_context + "\n\n" + cluster_rel_context
    cluster_summary_context = _build_cluster_context(cluster_summaries)
    if cluster_summary_context:
        discovered_context = discovered_context + "\n\n" + cluster_summary_context

    # Rebuild summaries from the store
    all_summaries_docs = get_all_summaries(store)
    new_summaries = {
        doc.metadata.get("table_name", ""): doc.page_content
        for doc in all_summaries_docs
        if doc.metadata.get("table_name")
    }

    # Reload column descriptions from JSON sidecar
    all_column_descriptions: dict[str, dict] = {}
    col_desc_path = output_dir / "column_descriptions.json"
    if col_desc_path.exists():
        try:
            all_column_descriptions = json.loads(
                col_desc_path.read_text(encoding="utf-8")
            )
        except Exception as exc:
            log.warning("Could not reload column descriptions: %s", exc)

    # Save enriched profiles JSON
    enriched_profiles_path = output_dir / "enriched_profiles.json"
    save_enriched_profiles_json(
        profiles, new_summaries, all_column_descriptions, report, enriched_profiles_path,
    )

    # Save discovered relationships (raw column pairs)
    if discovered_rels:
        discovered_path = output_dir / "discovered_column_relationships.json"
        discovered_path.write_text(
            json.dumps(discovered_rels, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )

    # Save cluster-derived PK/FK relationships
    if cluster_derived_rels:
        cluster_rels_path = output_dir / "cluster_derived_relationships.json"
        cluster_rels_path.write_text(
            json.dumps(cluster_derived_rels, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )

    # Save column cluster assignments for debugging/inspection
    if column_clusters:
        cluster_dump = {
            str(cid): members for cid, members in column_clusters.items()
        }
        cluster_dump["singletons"] = column_singletons
        clusters_path = output_dir / "column_clusters.json"
        clusters_path.write_text(
            json.dumps(cluster_dump, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )

    n_tables = len(profiles)
    enriched_er_path = output_dir / "enriched_er_diagram.md"

    # Guard: skip REDUCE if all tables were cached and output already exists
    if skip_reduce and enriched_er_path.exists():
        log.info("=== REDUCE skipped (all tables cached, output exists) ===")
        try:
            enrichment_text = enriched_er_path.read_text(encoding="utf-8")
        except Exception:
            enrichment_text = ""
        clusters_formed = len(table_clusters) if len(table_clusters) > 1 else 1
        await _phase(8, "REDUCE: LLM synthesis", "skipped — cached")
        saved_path = enriched_er_path
        await _phase(9, "Generating enriched ER diagram", "reused existing")
    else:
        if len(table_clusters) <= 1:
            log.info("=== REDUCE (direct, %d tables) ===", n_tables)
            enrichment_text = await reduce_phase(
                store, report, profiles, llm,
                all_column_descriptions=all_column_descriptions,
                discovered_relationships=discovered_context,
                top_k=REDUCE_TOP_K,
                token_budget=REDUCE_TOKEN_BUDGET,
                provider=provider,
                model=model,
            )
            clusters_formed = 1
        else:
            log.info("=== REDUCE per cluster (%d clusters) ===", len(table_clusters))
            cluster_analyses = await reduce_cluster_phase(
                table_clusters, store, report, llm,
                all_column_descriptions=all_column_descriptions,
                token_budget=PER_CLUSTER_TOKEN_BUDGET,
                max_workers=MAP_MAX_WORKERS,
                provider=provider,
                model=model,
            )

            log.info("=== META-REDUCE ===")
            enrichment_text = await meta_reduce_phase(
                table_clusters, cluster_analyses, report, llm,
                discovered_relationships=discovered_context,
                token_budget=META_REDUCE_TOKEN_BUDGET,
                provider=provider,
                model=model,
            )
            clusters_formed = len(table_clusters)

        await _phase(8, "REDUCE: LLM synthesis",
                     f"{len(enrichment_text):,} chars of analysis")

        # Save enriched ER diagram
        saved_path = save_enriched_er_diagram(enrichment_text, enriched_er_path)

        await _phase(9, "Generating enriched ER diagram",
                     "saved" if saved_path else "no diagram found")

    result = {
        "enrichment": enrichment_text,
        "tables_analyzed": n_tables,
        "tables_summarized": len(new_summaries),
        "tables_cached": 0,
        "relationships_analyzed": len(report.candidates),
        "column_relationships_discovered": len(discovered_rels),
        "cluster_derived_relationships": len(cluster_derived_rels),
        "column_clusters_formed": len(column_clusters),
        "table_clusters_formed": clusters_formed,
        "documents_embedded": len(new_summaries) + 1,
        "enriched_profiles_path": str(enriched_profiles_path),
    }
    if saved_path:
        result["enriched_er_diagram_path"] = str(saved_path)
    return result


# ---------------------------------------------------------------------------
# Orchestrator — public entry point (convenience wrapper)
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

    Thin wrapper that runs batch_enrich() (MAP + APPLY + EMBED) for all
    profiles, then discover_and_reduce_pipeline() (DISCOVER + CLUSTER + REDUCE).

    Args:
        profiles: List of FileProfile objects.
        report: RelationshipReport from the deterministic detector.
        dir_path: Path to the data directory (for metadata/logging).
        provider: LLM provider ("google", "groq", "openai", "anthropic").
        model: LLM model override.
        persist_dir: ChromaDB directory. Defaults to config.VECTOR_STORE_DIR.
        incremental: If True, skip tables already summarized in the store.

    Returns:
        Dict with enrichment text and metadata.
    """
    from file_profiler.config.env import BATCH_SIZE, OUTPUT_DIR as _OUTPUT_DIR

    # Pre-warm the embedding model so the first embed_phase has no cold start
    from file_profiler.agent.vector_store import warm_embeddings
    warm_embeddings()

    # Phase 1-3: MAP + APPLY + EMBED (batched)
    total_summarized = 0
    total_cached = 0
    all_column_descriptions: dict = {}
    batch_size = BATCH_SIZE

    for i in range(0, len(profiles), batch_size):
        batch_profiles = profiles[i : i + batch_size]
        batch_result = await batch_enrich(
            profiles=batch_profiles,
            report=report,
            dir_path=dir_path,
            provider=provider,
            model=model,
            persist_dir=persist_dir,
            incremental=incremental,
        )
        total_summarized += batch_result.get("tables_summarized", 0)
        total_cached += batch_result.get("tables_cached", 0)
        all_column_descriptions.update(batch_result.get("column_descriptions", {}))

    # Write column descriptions sidecar once after all batches
    if all_column_descriptions:
        col_desc_path = _resolve_writable_output_path(_OUTPUT_DIR / "column_descriptions.json")
        col_desc_path.write_text(
            json.dumps(all_column_descriptions, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )

    # Phase 4-5: DISCOVER + CLUSTER + REDUCE
    result = await discover_and_reduce_pipeline(
        profiles=profiles,
        report=report,
        dir_path=dir_path,
        provider=provider,
        model=model,
        persist_dir=persist_dir,
    )

    result["tables_summarized"] = total_summarized
    result["tables_cached"] = total_cached
    return result
