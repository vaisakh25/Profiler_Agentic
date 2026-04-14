"""Persistent ChromaDB vector store for enrichment summaries.

Stores per-table LLM summaries with metadata.  Supports incremental
updates: adding/replacing a single table's summary without
re-embedding everything.

The store uses a fingerprint (hash of table_name + row_count +
column_count) to detect stale summaries when a dataset changes.
"""

from __future__ import annotations

import hashlib
import logging
import shutil
from pathlib import Path
from typing import Optional

from langchain_core.documents import Document
from file_profiler.observability.langsmith import compact_text_output, safe_name, traceable

log = logging.getLogger(__name__)

COLLECTION_NAME = "table_summaries"


def _trace_store_inputs(inputs: dict) -> dict:
    persist_dir = inputs.get("persist_dir")
    return {
        "persist_dir": safe_name(str(persist_dir), kind="path") if persist_dir else "",
        "collection_name": inputs.get("collection_name", COLLECTION_NAME),
    }


def _trace_summary_upsert_inputs(inputs: dict) -> dict:
    summaries = inputs.get("summaries") or {}
    return {
        "table_count": len(summaries),
        "summary_chars": sum(len(text or "") for text in summaries.values()),
    }


def _trace_column_upsert_inputs(inputs: dict) -> dict:
    descriptions = inputs.get("all_column_descriptions") or {}
    return {
        "table_count": len(descriptions),
        "column_count": sum(len(cols or {}) for cols in descriptions.values()),
    }


def _trace_similarity_inputs(inputs: dict) -> dict:
    query = inputs.get("query") or ""
    return {
        "query_chars": len(query),
        "k": inputs.get("k"),
    }


# ---------------------------------------------------------------------------
# Fingerprinting — detect stale summaries
# ---------------------------------------------------------------------------

def _table_fingerprint(table_name: str, row_count: int, col_count: int) -> str:
    """Stable hash for a single table's shape."""
    raw = f"{table_name}:{row_count}:{col_count}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


# ---------------------------------------------------------------------------
# Embeddings singleton
# ---------------------------------------------------------------------------

_embeddings = None


def get_embeddings():
    """Return a cached NVIDIA embeddings instance."""
    global _embeddings
    if _embeddings is None:
        from file_profiler.agent.embedding_factory import get_embedding_function

        _embeddings = get_embedding_function()
    return _embeddings


@traceable(
    name="embeddings.warmup",
    run_type="embedding",
    process_outputs=compact_text_output,
)
def warm_embeddings() -> None:
    """Pre-warm the embedding model so the first real call has no cold start."""
    try:
        emb = get_embeddings()
        emb.embed_query("warmup")
        log.info("Embedding model pre-warmed")
    except Exception as exc:
        log.warning("Embedding pre-warm skipped: %s", exc)


# ---------------------------------------------------------------------------
# Store lifecycle
# ---------------------------------------------------------------------------

@traceable(
    name="vector_store.open",
    run_type="retriever",
    process_inputs=_trace_store_inputs,
    process_outputs=compact_text_output,
)
def get_or_create_store(
    persist_dir: Path,
    collection_name: str = COLLECTION_NAME,
):
    """Open or create a persistent ChromaDB vector store.

    Args:
        persist_dir: Directory for ChromaDB storage files.
        collection_name: Collection name within the store.

    Returns:
        LangChain Chroma instance backed by persistent storage.
    """
    from langchain_chroma import Chroma

    persist_dir.mkdir(parents=True, exist_ok=True)

    try:
        store = Chroma(
            persist_directory=str(persist_dir),
            collection_name=collection_name,
            embedding_function=get_embeddings(),
        )
        log.info("Opened vector store at %s (collection=%s)",
                 persist_dir, collection_name)
        return store
    except Exception as exc:
        log.warning(
            "Vector store corrupt at %s: %s — recreating",
            persist_dir, exc,
        )
        shutil.rmtree(persist_dir, ignore_errors=True)
        persist_dir.mkdir(parents=True, exist_ok=True)
        return Chroma(
            persist_directory=str(persist_dir),
            collection_name=collection_name,
            embedding_function=get_embeddings(),
        )


def clear_store(persist_dir: Path) -> None:
    """Delete all data from the vector store (all collections).

    Clears table_summaries, column_descriptions, and cluster_summaries
    collections.  Uses the ChromaDB API to delete collections (avoids
    Windows file-lock issues with shutil.rmtree).  Falls back to
    filesystem deletion if the API call fails.
    """
    collections_to_clear = [COLLECTION_NAME, "column_descriptions", "cluster_summaries"]
    try:
        from langchain_chroma import Chroma
        for coll_name in collections_to_clear:
            try:
                store = Chroma(
                    persist_directory=str(persist_dir),
                    collection_name=coll_name,
                    embedding_function=get_embeddings(),
                )
                store.delete_collection()
                log.info("Collection deleted: %s", coll_name)
            except Exception as exc:
                log.debug("Could not delete collection %s: %s", coll_name, exc)
        log.info("Vector store cleared: %s", persist_dir)
    except Exception:
        # Fallback: filesystem cleanup
        shutil.rmtree(persist_dir, ignore_errors=True)
        log.info("Vector store directory removed: %s", persist_dir)

    # Reset the cached embeddings singleton so next use gets a fresh instance
    global _embeddings
    _embeddings = None


# ---------------------------------------------------------------------------
# Batched add — respect ChromaDB's max batch size
# ---------------------------------------------------------------------------

_CHROMA_MAX_BATCH = 5000  # Stay under ChromaDB's 5461 limit with margin


def _batched_add_documents(store, docs: list) -> None:
    """Add documents to a Chroma store in batches to avoid exceeding the max batch size."""
    if not docs:
        return
    for i in range(0, len(docs), _CHROMA_MAX_BATCH):
        batch = docs[i : i + _CHROMA_MAX_BATCH]
        store.add_documents(batch)
    if len(docs) > _CHROMA_MAX_BATCH:
        log.debug("Batched add: %d docs in %d batches",
                  len(docs), (len(docs) + _CHROMA_MAX_BATCH - 1) // _CHROMA_MAX_BATCH)


# ---------------------------------------------------------------------------
# CRUD operations
# ---------------------------------------------------------------------------

def upsert_table_summary(
    store,
    table_name: str,
    summary_text: str,
    metadata: dict,
) -> None:
    """Insert or replace a single table's summary document.

    Deletes any existing document with matching table_name before
    inserting.  This makes incremental updates idempotent.
    """
    # Delete existing docs for this table
    try:
        existing = store.get(where={"table_name": table_name})
        if existing and existing["ids"]:
            store.delete(ids=existing["ids"])
            log.debug("Deleted %d old docs for table %s",
                      len(existing["ids"]), table_name)
    except Exception as exc:
        log.debug("Could not check existing docs for %s (collection may be empty): %s",
                  table_name, exc)

    metadata = {**metadata, "doc_type": "table_summary", "table_name": table_name}
    doc = Document(page_content=summary_text, metadata=metadata)
    store.add_documents([doc])
    log.debug("Upserted summary for table %s", table_name)


@traceable(
    name="vector_store.upsert_table_summaries",
    run_type="retriever",
    process_inputs=_trace_summary_upsert_inputs,
    process_outputs=compact_text_output,
)
def batch_upsert_table_summaries(
    store,
    summaries: dict[str, str],
    metadata_map: dict[str, dict],
) -> int:
    """Batch-insert table summaries in a single add_documents call.

    Much faster than calling upsert_table_summary() in a loop because
    embedding + ChromaDB I/O happens once instead of N times.

    Args:
        store: LangChain Chroma instance.
        summaries: {table_name: summary_text}.
        metadata_map: {table_name: {row_count, column_count, fingerprint}}.

    Returns:
        Number of documents upserted.
    """
    if not summaries:
        return 0

    table_names = list(summaries.keys())

    # Bulk delete existing docs for all tables
    try:
        existing = store.get(where={"doc_type": "table_summary"})
        if existing and existing["ids"]:
            # Filter to only the tables we're replacing
            ids_to_delete = []
            metas = existing.get("metadatas", [])
            for i, doc_id in enumerate(existing["ids"]):
                if i < len(metas) and metas[i].get("table_name") in table_names:
                    ids_to_delete.append(doc_id)
            if ids_to_delete:
                store.delete(ids=ids_to_delete)
                log.debug("Bulk-deleted %d old summary docs", len(ids_to_delete))
    except Exception as exc:
        log.debug("Could not bulk-delete existing summaries: %s", exc)

    # Build all docs
    docs = []
    for table_name, summary_text in summaries.items():
        meta = {
            **(metadata_map.get(table_name, {})),
            "doc_type": "table_summary",
            "table_name": table_name,
        }
        docs.append(Document(page_content=summary_text, metadata=meta))

    # Single batch embedding + insert
    _batched_add_documents(store, docs)
    log.info("Batch-upserted %d table summaries", len(docs))
    return len(docs)


@traceable(
    name="vector_store.upsert_column_descriptions",
    run_type="retriever",
    process_inputs=_trace_column_upsert_inputs,
    process_outputs=compact_text_output,
)
def batch_upsert_column_descriptions(
    store,
    all_column_descriptions: dict[str, dict],
    profile_map: dict,
) -> int:
    """Batch-insert column descriptions across all tables in one call.

    Args:
        store: LangChain Chroma instance (column_descriptions collection).
        all_column_descriptions: {table_name: {col_name: {type, role, description}}}.
        profile_map: {table_name: FileProfile} for enriched embedding text.

    Returns:
        Total number of column documents upserted.
    """
    if not all_column_descriptions:
        return 0

    table_names = list(all_column_descriptions.keys())

    # Bulk delete existing column docs for these tables
    try:
        existing = store.get(where={"doc_type": "column_description"})
        if existing and existing["ids"]:
            ids_to_delete = []
            metas = existing.get("metadatas", [])
            for i, doc_id in enumerate(existing["ids"]):
                if i < len(metas) and metas[i].get("table_name") in table_names:
                    ids_to_delete.append(doc_id)
            if ids_to_delete:
                store.delete(ids=ids_to_delete)
                log.debug("Bulk-deleted %d old column docs", len(ids_to_delete))
    except Exception as exc:
        log.debug("Could not bulk-delete existing column docs: %s", exc)

    # Build all docs across all tables
    docs = []
    for table_name, col_descs in all_column_descriptions.items():
        profile = profile_map.get(table_name)
        profile_lookup = {}
        if profile:
            for col in profile.columns:
                profile_lookup[col.name] = col

        for col_name, info in col_descs.items():
            col_type = info.get("type", "unknown")
            role = info.get("role", "regular")
            desc = info.get("description", "")

            text = (
                f"Table: {table_name}, Column: {col_name}, "
                f"Type: {col_type}, Role: {role}. "
                f"{desc}"
            )

            col_prof = profile_lookup.get(col_name)
            if col_prof:
                extras = []
                if col_prof.distinct_count is not None:
                    extras.append(f"distinct={col_prof.distinct_count}")
                if col_prof.is_key_candidate:
                    extras.append("key_candidate=True")
                if col_prof.cardinality:
                    extras.append(f"cardinality={col_prof.cardinality.value}")
                if col_prof.sample_values:
                    samples = col_prof.sample_values[:5]
                    extras.append(f"samples=[{', '.join(str(s) for s in samples)}]")
                if col_prof.top_values:
                    top = [tv.value for tv in col_prof.top_values[:3]]
                    extras.append(f"top_values=[{', '.join(top)}]")
                if extras:
                    text += f" | {'; '.join(extras)}"

            meta = {
                "doc_type": "column_description",
                "table_name": table_name,
                "column_name": col_name,
                "column_type": col_type,
                "role": role,
            }
            docs.append(Document(page_content=text, metadata=meta))

    if docs:
        _batched_add_documents(store, docs)
        log.info("Batch-upserted %d column descriptions across %d tables",
                 len(docs), len(all_column_descriptions))

    return len(docs)


def upsert_relationship_doc(
    store,
    report_text: str,
    metadata: dict,
) -> None:
    """Insert or replace the deterministic relationship report document."""
    try:
        existing = store.get(where={"doc_type": "relationships"})
        if existing and existing["ids"]:
            store.delete(ids=existing["ids"])
    except Exception as exc:
        log.debug("Could not check existing relationship docs: %s", exc)

    metadata = {**metadata, "doc_type": "relationships"}
    doc = Document(page_content=report_text, metadata=metadata)
    store.add_documents([doc])
    log.debug("Upserted relationship document")


def upsert_relationship_candidates(
    store,
    report: "RelationshipReport",
    profiles: list,
) -> int:
    """Store per-table relationship summary documents for semantic retrieval.

    Groups all FK candidates by table and creates one summary document per
    table (instead of one per candidate).  This reduces document count from
    O(candidates) to O(tables), avoiding ChromaDB batch-size limits on
    dense schemas (200+ tables with thousands of FK candidates).

    Each document summarises all outgoing and incoming FK relationships
    for a table, with column types, confidence, and sample values.

    Returns the number of documents stored.
    """
    # Delete existing relationship docs (both old per-candidate and new per-table format)
    for doc_type in ("relationship_candidate", "relationship_summary"):
        try:
            existing = store.get(where={"doc_type": doc_type})
            if existing and existing["ids"]:
                store.delete(ids=existing["ids"])
        except Exception:
            pass

    if not report.candidates:
        return 0

    # Build column profile lookup
    col_lookup: dict[tuple[str, str], object] = {}
    for p in profiles:
        for col in p.columns:
            col_lookup[(p.table_name, col.name)] = col

    # Group candidates by table
    table_rels: dict[str, list] = {}
    for c in report.candidates:
        table_rels.setdefault(c.fk.table_name, []).append(("outgoing", c))
        table_rels.setdefault(c.pk.table_name, []).append(("incoming", c))

    docs = []
    for table_name, rels in table_rels.items():
        parts = [f"Table {table_name} relationships:"]

        outgoing = [(d, c) for d, c in rels if d == "outgoing"]
        incoming = [(d, c) for d, c in rels if d == "incoming"]

        if outgoing:
            parts.append(f"  Outgoing FKs ({len(outgoing)}):")
            for _, c in outgoing[:20]:  # cap at 20 per direction to avoid huge docs
                line = f"    {c.fk.column_name} -> {c.pk.table_name}.{c.pk.column_name} (confidence={c.confidence:.2f})"
                fk_col = col_lookup.get((c.fk.table_name, c.fk.column_name))
                if fk_col and hasattr(fk_col, 'sample_values') and fk_col.sample_values:
                    line += f", samples={fk_col.sample_values[:3]}"
                parts.append(line)
            if len(outgoing) > 20:
                parts.append(f"    ... and {len(outgoing) - 20} more")

        if incoming:
            parts.append(f"  Incoming FKs ({len(incoming)}):")
            for _, c in incoming[:20]:
                line = f"    {c.fk.table_name}.{c.fk.column_name} -> {c.pk.column_name} (confidence={c.confidence:.2f})"
                parts.append(line)
            if len(incoming) > 20:
                parts.append(f"    ... and {len(incoming) - 20} more")

        text = "\n".join(parts)
        meta = {
            "doc_type": "relationship_summary",
            "table_name": table_name,
            "outgoing_count": len(outgoing),
            "incoming_count": len(incoming),
        }
        docs.append(Document(page_content=text, metadata=meta))

    if docs:
        _batched_add_documents(store, docs)
        log.info("Upserted %d per-table relationship summary docs (from %d candidates)",
                 len(docs), len(report.candidates))

    return len(docs)


def query_relationship_candidates(
    store,
    query: str,
    k: int = 10,
) -> list[Document]:
    """Retrieve the k most relevant FK candidate/summary documents via semantic search."""
    # Try new per-table format first, fall back to old per-candidate format
    for doc_type in ("relationship_summary", "relationship_candidate"):
        try:
            results = store.similarity_search(
                query, k=k,
                filter={"doc_type": doc_type},
            )
            if results:
                return results
        except Exception:
            pass
    # Last resort: unfiltered search
    try:
        results = store.similarity_search(query, k=k)
        return [d for d in results if d.metadata.get("doc_type") in
                ("relationship_summary", "relationship_candidate")]
    except Exception:
        return []


@traceable(
    name="vector_store.query_similar_tables",
    run_type="retriever",
    process_inputs=_trace_similarity_inputs,
    process_outputs=compact_text_output,
)
def query_similar_tables(
    store,
    query: str,
    k: int = 15,
) -> list[Document]:
    """Retrieve the k most similar table summaries."""
    try:
        results = store.similarity_search(
            query, k=k,
            filter={"doc_type": "table_summary"},
        )
        return results
    except Exception as exc:
        # Filter syntax varies across ChromaDB versions; fallback
        log.debug("Filtered similarity search failed (%s), retrying without filter", exc)
        results = store.similarity_search(query, k=k)
        return [d for d in results if d.metadata.get("doc_type") == "table_summary"]


def get_all_summaries(store) -> list[Document]:
    """Retrieve all table summary documents from the store."""
    try:
        result = store.get(where={"doc_type": "table_summary"})
        if not result or not result["documents"]:
            return []
        docs = []
        for i, text in enumerate(result["documents"]):
            meta = result["metadatas"][i] if result["metadatas"] else {}
            docs.append(Document(page_content=text, metadata=meta))
        return docs
    except Exception as exc:
        log.warning("get_all_summaries failed: %s", exc)
        return []


def list_stored_tables(store) -> list[str]:
    """Return table names that already have summaries in the store."""
    docs = get_all_summaries(store)
    return [d.metadata.get("table_name", "") for d in docs if d.metadata.get("table_name")]


def get_stored_fingerprints(store) -> dict[str, str]:
    """Return {table_name: fingerprint} for all stored summaries."""
    docs = get_all_summaries(store)
    return {
        d.metadata["table_name"]: d.metadata.get("fingerprint", "")
        for d in docs
        if d.metadata.get("table_name")
    }


def get_table_embeddings(store) -> tuple[list[str], list[list[float]]]:
    """Return stored embedding vectors for all table_summary documents.

    Reads directly from the underlying ChromaDB collection so we can pass
    the vectors to a clustering algorithm without re-embedding.

    Returns:
        Tuple of (table_names, embedding_vectors).
        Both lists are aligned by index.
        Returns ([], []) if the collection is empty or embeddings are absent.
    """
    try:
        result = store._collection.get(
            where={"doc_type": "table_summary"},
            include=["embeddings", "metadatas"],
        )
        metadatas = result.get("metadatas") or []
        embeddings = result.get("embeddings") or []

        table_names: list[str] = []
        vectors: list[list[float]] = []
        for i, meta in enumerate(metadatas):
            name = meta.get("table_name", "")
            emb = embeddings[i] if i < len(embeddings) else None
            if name and emb is not None:
                table_names.append(name)
                vectors.append(list(emb))

        return table_names, vectors
    except Exception as exc:
        log.warning("get_table_embeddings: failed — %s", exc)
        return [], []


# ---------------------------------------------------------------------------
# Column-level embeddings — for cross-table column similarity discovery
# ---------------------------------------------------------------------------

COLUMN_COLLECTION = "column_descriptions"


def get_or_create_column_store(persist_dir: Path):
    """Open or create a persistent ChromaDB store for column descriptions."""
    from langchain_chroma import Chroma

    persist_dir.mkdir(parents=True, exist_ok=True)

    try:
        return Chroma(
            persist_directory=str(persist_dir),
            collection_name=COLUMN_COLLECTION,
            embedding_function=get_embeddings(),
        )
    except Exception as exc:
        log.warning("Column store corrupt at %s: %s — recreating", persist_dir, exc)
        import shutil as _shutil
        _shutil.rmtree(persist_dir / COLUMN_COLLECTION, ignore_errors=True)
        return Chroma(
            persist_directory=str(persist_dir),
            collection_name=COLUMN_COLLECTION,
            embedding_function=get_embeddings(),
        )


def upsert_column_descriptions(
    store,
    table_name: str,
    column_descriptions: dict,
    column_profiles: Optional[list] = None,
) -> int:
    """Embed per-column descriptions for a single table.

    Each column becomes a separate document with metadata identifying
    the table and column.  The document text combines the column name,
    type, role, semantic description, AND discriminating profile signals
    (sample values, cardinality, distinct count) for richer embeddings.

    Args:
        store: LangChain Chroma instance (column_descriptions collection).
        table_name: Name of the table these columns belong to.
        column_descriptions: {col_name: {"type", "role", "description"}}.
        column_profiles: Optional list of ColumnProfile objects for this table.
                         When provided, sample values and stats are included
                         in the embedding text for better similarity matching.

    Returns:
        Number of columns embedded.
    """
    if not column_descriptions:
        return 0

    # Delete existing docs for this table
    try:
        existing = store.get(where={"table_name": table_name})
        if existing and existing["ids"]:
            store.delete(ids=existing["ids"])
    except Exception as exc:
        log.debug("Could not check existing column docs for %s: %s", table_name, exc)

    # Build a lookup from column_profiles for enriching embedding text
    profile_lookup: dict = {}
    if column_profiles:
        for col in column_profiles:
            profile_lookup[col.name] = col

    docs = []
    for col_name, info in column_descriptions.items():
        col_type = info.get("type", "unknown")
        role = info.get("role", "regular")
        desc = info.get("description", "")

        # Build a rich text for embedding — structural + semantic signals
        text = (
            f"Table: {table_name}, Column: {col_name}, "
            f"Type: {col_type}, Role: {role}. "
            f"{desc}"
        )

        # Enrich with profile signals when available
        col_prof = profile_lookup.get(col_name)
        if col_prof:
            extras = []
            if col_prof.distinct_count is not None:
                extras.append(f"distinct={col_prof.distinct_count}")
            if col_prof.is_key_candidate:
                extras.append("key_candidate=True")
            if col_prof.cardinality:
                extras.append(f"cardinality={col_prof.cardinality.value}")
            if col_prof.sample_values:
                samples = col_prof.sample_values[:5]
                extras.append(f"samples=[{', '.join(str(s) for s in samples)}]")
            if col_prof.top_values:
                top = [tv.value for tv in col_prof.top_values[:3]]
                extras.append(f"top_values=[{', '.join(top)}]")
            if extras:
                text += f" | {'; '.join(extras)}"

        meta = {
            "doc_type": "column_description",
            "table_name": table_name,
            "column_name": col_name,
            "column_type": col_type,
            "role": role,
        }
        docs.append(Document(page_content=text, metadata=meta))

    if docs:
        _batched_add_documents(store, docs)
        log.debug("Upserted %d column descriptions for table %s",
                  len(docs), table_name)

    return len(docs)


# ---------------------------------------------------------------------------
# Column-affinity-based table clustering
# ---------------------------------------------------------------------------

def build_table_affinity_matrix(
    store,
    table_names: list[str],
    top_k: int = 5,
    similarity_threshold: float = 0.65,
) -> tuple[list[str], "np.ndarray", list[dict]]:
    """Build a table-to-table affinity matrix from column embedding similarities.

    Uses a single bulk fetch of all column embeddings from ChromaDB, then
    computes the full cosine similarity matrix in numpy.  This replaces
    per-column ChromaDB queries (O(C) queries → 1 query + O(C²) numpy ops).

    Args:
        store: LangChain Chroma instance (column_descriptions collection).
        table_names: Ordered list of table names to include.
        top_k: Per-column neighbours to consider for FK pairs.
        similarity_threshold: Min cosine similarity to count as a link.

    Returns:
        Tuple of:
          - table_names (ordered, may be subset if some have no columns)
          - affinity_matrix: np.ndarray of shape (n_tables, n_tables)
          - column_pairs: list of dicts with source_table, source_column,
            target_table, target_column, similarity_score, source_type, target_type
    """
    import numpy as np

    table_set = set(table_names)
    name_to_idx = {name: i for i, name in enumerate(table_names)}
    n = len(table_names)
    affinity = np.zeros((n, n), dtype=float)
    column_pairs: list[dict] = []
    seen_pairs: set[tuple] = set()

    # Single bulk fetch — embeddings + metadata in one ChromaDB call
    try:
        result = store._collection.get(
            where={"doc_type": "column_description"},
            include=["embeddings", "metadatas"],
        )
        metadatas = result.get("metadatas") or []
        embeddings = result.get("embeddings") or []
        if not metadatas or not embeddings:
            return table_names, affinity, column_pairs
    except Exception as exc:
        log.warning("build_table_affinity_matrix: bulk fetch failed: %s", exc)
        return table_names, affinity, column_pairs

    # Filter to columns belonging to requested tables
    indices = []
    col_info: list[tuple[str, str, dict]] = []  # (table, col, meta)
    for i, meta in enumerate(metadatas):
        table = meta.get("table_name", "")
        col = meta.get("column_name", "")
        if table in table_set and col and i < len(embeddings):
            indices.append(i)
            col_info.append((table, col, meta))

    if not indices:
        return table_names, affinity, column_pairs

    # Build embedding matrix and compute cosine similarity in bulk
    X = np.array([embeddings[i] for i in indices], dtype=np.float32)
    norms = np.linalg.norm(X, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    X_norm = X / norms
    # Full pairwise cosine similarity: (C x C) matrix
    sim_matrix = X_norm @ X_norm.T

    num_cols = len(indices)
    for i in range(num_cols):
        src_table, src_col, src_meta = col_info[i]

        # Get top-k neighbours for this column (excluding same-table)
        row = sim_matrix[i]
        # Mask self
        row[i] = -1.0

        # Get indices sorted by descending similarity
        top_indices = np.argsort(row)[::-1]

        neighbours_found = 0
        for j_idx in top_indices:
            if neighbours_found >= top_k:
                break

            score = float(row[j_idx])
            if score < similarity_threshold:
                break

            tgt_table, tgt_col, tgt_meta = col_info[j_idx]

            # Skip same table
            if tgt_table == src_table:
                continue

            neighbours_found += 1

            # Accumulate affinity
            ti, tj = name_to_idx[src_table], name_to_idx[tgt_table]
            affinity[ti, tj] += score
            affinity[tj, ti] += score

            # Deduplicate column pairs (A→B == B→A)
            pair = tuple(sorted([(src_table, src_col), (tgt_table, tgt_col)]))
            if pair in seen_pairs:
                continue
            seen_pairs.add(pair)

            column_pairs.append({
                "source_table": src_table,
                "source_column": src_col,
                "target_table": tgt_table,
                "target_column": tgt_col,
                "similarity_score": round(score, 4),
                "source_type": src_meta.get("column_type", ""),
                "target_type": tgt_meta.get("column_type", ""),
            })

    column_pairs.sort(key=lambda x: -x["similarity_score"])
    log.info(
        "Affinity matrix: %d tables, %d columns, %d pairs above %.2f "
        "(computed via bulk numpy cosine — 1 ChromaDB query)",
        n, num_cols, len(column_pairs), similarity_threshold,
    )
    return table_names, affinity, column_pairs


def cluster_by_column_affinity(
    store,
    table_names: list[str],
    target_cluster_size: int = 15,
    similarity_threshold: float = 0.65,
) -> tuple[dict[int, list[str]], list[dict]]:
    """Cluster tables by column affinity and return FK candidates for free.

    Uses the table-to-table affinity matrix (built from column embedding
    similarities) as the basis for AgglomerativeClustering.  Tables that
    share many similar columns end up in the same cluster.

    Returns:
        Tuple of:
          - clusters: {cluster_id: [table_names]}
          - column_pairs: list of FK candidate dicts from the affinity computation
    """
    import numpy as np

    n_tables = len(table_names)

    if n_tables <= target_cluster_size:
        # Still compute column pairs even for small datasets
        _, _, column_pairs = build_table_affinity_matrix(
            store, table_names,
            similarity_threshold=similarity_threshold,
        )
        return {0: list(table_names)}, column_pairs

    names, affinity, column_pairs = build_table_affinity_matrix(
        store, table_names,
        similarity_threshold=similarity_threshold,
    )

    if not names or affinity.sum() == 0:
        log.warning("CLUSTER: no column affinity signal — falling back to sequential chunks")
        chunk_size = target_cluster_size
        clusters = {}
        for i, name in enumerate(table_names):
            clusters.setdefault(i // chunk_size, []).append(name)
        return clusters, column_pairs

    try:
        from sklearn.cluster import AgglomerativeClustering

        n_clusters = max(2, int(np.ceil(n_tables / target_cluster_size)))

        # Convert affinity to distance: higher affinity = lower distance
        max_aff = affinity.max()
        if max_aff > 0:
            distance = 1.0 - (affinity / max_aff)
        else:
            distance = np.ones_like(affinity)
        np.fill_diagonal(distance, 0)

        clustering = AgglomerativeClustering(
            n_clusters=min(n_clusters, n_tables),
            metric="precomputed",
            linkage="average",
        )
        labels = clustering.fit_predict(distance)

        clusters: dict[int, list[str]] = {}
        for name, label in zip(names, labels):
            clusters.setdefault(int(label), []).append(name)

        # Include any tables not in the column store
        stored_set = set(names)
        missing = [t for t in table_names if t not in stored_set]
        if missing:
            smallest = min(clusters, key=lambda k: len(clusters[k]))
            clusters[smallest].extend(missing)

        log.info(
            "CLUSTER (column affinity): %d tables → %d clusters, %d column pairs",
            n_tables, len(clusters), len(column_pairs),
        )
        return clusters, column_pairs

    except ImportError:
        log.warning("CLUSTER: sklearn not available — falling back to sequential chunks")
        chunk_size = target_cluster_size
        clusters = {}
        for i, name in enumerate(table_names):
            clusters.setdefault(i // chunk_size, []).append(name)
        return clusters, column_pairs
    except Exception as exc:
        log.warning("CLUSTER: failed (%s) — falling back to sequential chunks", exc)
        chunk_size = target_cluster_size
        clusters = {}
        for i, name in enumerate(table_names):
            clusters.setdefault(i // chunk_size, []).append(name)
        return clusters, column_pairs


# ---------------------------------------------------------------------------
# Phase 3: Column-level DBSCAN clustering
# ---------------------------------------------------------------------------

def _fetch_column_embeddings(
    store,
    table_names: Optional[list[str]] = None,
) -> tuple[list[dict], "np.ndarray"]:
    """Bulk-fetch column embeddings and metadata from ChromaDB.

    Args:
        store: LangChain Chroma instance (column_descriptions collection).
        table_names: Optional filter — only columns from these tables.

    Returns:
        Tuple of (col_infos, embedding_matrix).
        col_infos: list of dicts with table_name, column_name, column_type, role.
        embedding_matrix: np.ndarray of shape (n_columns, embedding_dim).
        Returns ([], empty array) if nothing found.
    """
    import numpy as np

    try:
        result = store._collection.get(
            where={"doc_type": "column_description"},
            include=["embeddings", "metadatas"],
        )
        metadatas = result.get("metadatas") or []
        embeddings = result.get("embeddings") or []
        if not metadatas or not embeddings:
            return [], np.empty((0, 0))
    except Exception as exc:
        log.warning("_fetch_column_embeddings: bulk fetch failed: %s", exc)
        return [], np.empty((0, 0))

    table_set = set(table_names) if table_names else None
    col_infos: list[dict] = []
    vectors: list[list[float]] = []

    for i, meta in enumerate(metadatas):
        table = meta.get("table_name", "")
        col = meta.get("column_name", "")
        if not table or not col or i >= len(embeddings):
            continue
        if table_set and table not in table_set:
            continue
        col_infos.append({
            "table_name": table,
            "column_name": col,
            "column_type": meta.get("column_type", ""),
            "role": meta.get("role", "regular"),
        })
        vectors.append(embeddings[i])

    if not vectors:
        return [], np.empty((0, 0))

    return col_infos, np.array(vectors, dtype=np.float32)


def cluster_columns_dbscan(
    store,
    table_names: Optional[list[str]] = None,
    eps: float = 0.35,
    min_samples: int = 2,
) -> tuple[dict[int, list[dict]], list[dict]]:
    """Cluster columns across all tables using DBSCAN on embedding vectors.

    Unlike cluster_by_column_affinity() which clusters *tables*, this function
    clusters individual *columns* — columns with similar semantic descriptions
    end up in the same cluster regardless of which table they belong to.

    DBSCAN naturally handles:
    - Variable cluster sizes (a PK with 5 FKs vs a PK with 1 FK)
    - Singletons (noise points = standalone attributes, not FKs)
    - No preset K required

    Args:
        store: LangChain Chroma instance (column_descriptions collection).
        table_names: Optional filter — only include columns from these tables.
                     If None, includes all columns in the store.
        eps: Maximum distance between two samples (1 - cosine_similarity).
             Default 0.35 corresponds to similarity threshold of 0.65.
        min_samples: Minimum cluster size. Default 2 (PK + at least 1 FK).

    Returns:
        Tuple of:
          - clusters: {cluster_id: [column_info_dicts]}
            Each dict has: table_name, column_name, column_type, role.
            cluster_id == -1 contains noise points (singletons).
          - singletons: list of column_info_dicts that are noise points.
    """
    import numpy as np

    col_infos, X = _fetch_column_embeddings(store, table_names)
    if not col_infos or X.size == 0:
        log.warning("cluster_columns_dbscan: no column embeddings found")
        return {}, []

    # Normalize for cosine distance
    norms = np.linalg.norm(X, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    X_norm = X / norms

    # Cosine distance matrix: 1 - cosine_similarity
    sim_matrix = X_norm @ X_norm.T
    distance_matrix = 1.0 - sim_matrix
    # Clamp small negatives from floating point
    np.clip(distance_matrix, 0.0, 2.0, out=distance_matrix)
    np.fill_diagonal(distance_matrix, 0.0)

    try:
        from sklearn.cluster import DBSCAN

        clustering = DBSCAN(
            eps=eps,
            min_samples=min_samples,
            metric="precomputed",
        )
        labels = clustering.fit_predict(distance_matrix)
    except ImportError:
        log.warning("cluster_columns_dbscan: sklearn not available")
        return {}, list(col_infos)
    except Exception as exc:
        log.warning("cluster_columns_dbscan: DBSCAN failed — %s", exc)
        return {}, list(col_infos)

    clusters: dict[int, list[dict]] = {}
    singletons: list[dict] = []
    for info, label in zip(col_infos, labels):
        label = int(label)
        info_with_label = {**info, "cluster_id": label}
        if label == -1:
            singletons.append(info_with_label)
        else:
            clusters.setdefault(label, []).append(info_with_label)

    # Filter out clusters where all columns belong to the same table
    # (these are intra-table similarities, not cross-table relationships)
    cross_table_clusters: dict[int, list[dict]] = {}
    for cid, members in clusters.items():
        tables_in_cluster = {m["table_name"] for m in members}
        if len(tables_in_cluster) >= 2:
            cross_table_clusters[cid] = members
        else:
            # Demote single-table clusters to singletons
            for m in members:
                m["cluster_id"] = -1
                singletons.append(m)

    n_cols = len(col_infos)
    n_clusters = len(cross_table_clusters)
    n_noise = len(singletons)
    log.info(
        "COLUMN CLUSTER (DBSCAN): %d columns → %d cross-table clusters, "
        "%d singletons (eps=%.2f, min_samples=%d)",
        n_cols, n_clusters, n_noise, eps, min_samples,
    )

    return cross_table_clusters, singletons


# ---------------------------------------------------------------------------
# Phase 5: Derive PK/FK relationships from column clusters
# ---------------------------------------------------------------------------

def derive_relationships_from_clusters(
    clusters: dict[int, list[dict]],
    profiles: list,
) -> list[dict]:
    """Derive PK-FK relationships from column clusters.

    For each cluster, identifies the primary key column (highest uniqueness
    signal) and treats all other columns as foreign key references to it.

    PK selection priority:
    1. Column with role == "PK" (from LLM MAP phase)
    2. Column with is_key_candidate == True (from profiler)
    3. Column with highest unique_ratio

    Handles edge cases:
    - Multi-PK clusters (2+ PKs from different tables) → sibling PKs, skip
    - Date/metric clusters (no PK candidate) → shared domain, not FK
    - All columns from same table → filtered out upstream by DBSCAN phase

    Args:
        clusters: Output of cluster_columns_dbscan() — {cluster_id: [col_info_dicts]}.
        profiles: List of FileProfile objects (used for uniqueness/cardinality signals).

    Returns:
        List of relationship dicts, each with:
          - fk_table, fk_column: the foreign key side
          - pk_table, pk_column: the primary key side
          - cluster_id: which cluster this came from
          - confidence: derived confidence score (0.0–1.0)
          - method: "column_cluster"
    """
    # Build a lookup: (table_name, column_name) -> ColumnProfile
    col_lookup: dict[tuple[str, str], object] = {}
    for p in profiles:
        for col in p.columns:
            col_lookup[(p.table_name, col.name)] = col

    relationships: list[dict] = []

    for cluster_id, members in clusters.items():
        if len(members) < 2:
            continue

        # Score each member for PK-ness
        pk_scores: list[tuple[float, dict]] = []
        for m in members:
            key = (m["table_name"], m["column_name"])
            col = col_lookup.get(key)
            score = 0.0

            if m.get("role", "").upper() == "PK":
                score += 0.5

            if col:
                if getattr(col, "is_key_candidate", False):
                    score += 0.4
                unique_ratio = getattr(col, "unique_ratio", 0.0)
                score += unique_ratio * 0.3
                # Penalize nullable columns
                if getattr(col, "null_count", 0) > 0:
                    score -= 0.1

            pk_scores.append((score, m))

        pk_scores.sort(key=lambda x: -x[0])

        # Check for multi-PK (sibling PKs from different tables)
        # If top 2 both have strong PK signal, they're likely separate
        # entity PKs that happen to be semantically similar — skip.
        if len(pk_scores) >= 2:
            top_score, top_member = pk_scores[0]
            second_score, second_member = pk_scores[1]
            if (
                top_score >= 0.7
                and second_score >= 0.7
                and top_member["table_name"] != second_member["table_name"]
            ):
                # Both look like PKs — check if they're truly independent
                top_col = col_lookup.get(
                    (top_member["table_name"], top_member["column_name"])
                )
                second_col = col_lookup.get(
                    (second_member["table_name"], second_member["column_name"])
                )
                if top_col and second_col:
                    both_high_cardinality = (
                        getattr(top_col, "unique_ratio", 0) >= 0.95
                        and getattr(second_col, "unique_ratio", 0) >= 0.95
                    )
                    if both_high_cardinality:
                        log.debug(
                            "Cluster %d: sibling PKs detected (%s.%s, %s.%s) — skipping",
                            cluster_id,
                            top_member["table_name"], top_member["column_name"],
                            second_member["table_name"], second_member["column_name"],
                        )
                        continue

        # No PK candidate at all? This is likely a shared attribute domain
        # (e.g., dates, status codes) — not a FK relationship.
        best_score = pk_scores[0][0] if pk_scores else 0.0
        if best_score < 0.2:
            log.debug(
                "Cluster %d: no PK candidate (best_score=%.2f) — shared domain, skipping",
                cluster_id, best_score,
            )
            continue

        # The top-scoring member is the PK; all others are FKs
        _, pk_member = pk_scores[0]
        pk_col = col_lookup.get((pk_member["table_name"], pk_member["column_name"]))

        for _, fk_member in pk_scores[1:]:
            # Skip if FK is from the same table as PK
            if fk_member["table_name"] == pk_member["table_name"]:
                continue

            fk_col = col_lookup.get(
                (fk_member["table_name"], fk_member["column_name"])
            )

            # Compute confidence from available signals
            confidence = 0.50  # base: cluster membership

            # Boost if PK has key_candidate
            if pk_col and getattr(pk_col, "is_key_candidate", False):
                confidence += 0.15

            # Boost if FK distinct count <= PK distinct count (subset signal)
            if pk_col and fk_col:
                pk_distinct = getattr(pk_col, "distinct_count", 0) or 0
                fk_distinct = getattr(fk_col, "distinct_count", 0) or 0
                if pk_distinct > 0 and fk_distinct <= pk_distinct:
                    confidence += 0.10

            # Boost for type compatibility
            if pk_col and fk_col:
                pk_type = getattr(pk_col, "inferred_type", None)
                fk_type = getattr(fk_col, "inferred_type", None)
                if pk_type and fk_type and pk_type == fk_type:
                    confidence += 0.10

            # Boost for name patterns (FK column name contains PK table name)
            fk_name = fk_member["column_name"].lower()
            pk_table_lower = pk_member["table_name"].lower().rstrip("s")
            if pk_table_lower in fk_name:
                confidence += 0.10

            confidence = min(confidence, 1.0)

            relationships.append({
                "fk_table": fk_member["table_name"],
                "fk_column": fk_member["column_name"],
                "pk_table": pk_member["table_name"],
                "pk_column": pk_member["column_name"],
                "cluster_id": cluster_id,
                "confidence": round(confidence, 2),
                "method": "column_cluster",
            })

    relationships.sort(key=lambda r: -r["confidence"])
    log.info(
        "DERIVE RELATIONSHIPS: %d clusters → %d PK→FK relationships",
        len(clusters), len(relationships),
    )
    return relationships


# ---------------------------------------------------------------------------
# Phase 4: Cluster summary store
# ---------------------------------------------------------------------------

CLUSTER_COLLECTION = "cluster_summaries"


def get_or_create_cluster_store(persist_dir: Path):
    """Open or create a persistent ChromaDB store for column cluster summaries."""
    from langchain_chroma import Chroma

    persist_dir.mkdir(parents=True, exist_ok=True)

    try:
        return Chroma(
            persist_directory=str(persist_dir),
            collection_name=CLUSTER_COLLECTION,
            embedding_function=get_embeddings(),
        )
    except Exception as exc:
        log.warning("Cluster store corrupt at %s: %s — recreating", persist_dir, exc)
        import shutil as _shutil
        _shutil.rmtree(persist_dir / CLUSTER_COLLECTION, ignore_errors=True)
        return Chroma(
            persist_directory=str(persist_dir),
            collection_name=CLUSTER_COLLECTION,
            embedding_function=get_embeddings(),
        )


def upsert_cluster_summary(
    store,
    cluster_id: int,
    summary_text: str,
    metadata: dict,
) -> None:
    """Insert or replace a single column cluster summary document.

    Idempotent — deletes any existing document for this cluster_id first.
    """
    try:
        existing = store.get(where={"cluster_id": cluster_id})
        if existing and existing["ids"]:
            store.delete(ids=existing["ids"])
    except Exception as exc:
        log.debug("Could not check existing cluster docs for %d: %s", cluster_id, exc)

    metadata = {**metadata, "doc_type": "cluster_summary", "cluster_id": cluster_id}
    doc = Document(page_content=summary_text, metadata=metadata)
    store.add_documents([doc])
    log.debug("Upserted cluster summary for cluster %d", cluster_id)
