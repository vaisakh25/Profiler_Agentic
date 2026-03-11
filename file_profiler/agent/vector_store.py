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

log = logging.getLogger(__name__)

COLLECTION_NAME = "table_summaries"


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
    """Return a cached HuggingFaceEmbeddings instance (all-MiniLM-L6-v2)."""
    global _embeddings
    if _embeddings is None:
        from langchain_huggingface import HuggingFaceEmbeddings
        _embeddings = HuggingFaceEmbeddings(model_name="all-MiniLM-L6-v2")
    return _embeddings


# ---------------------------------------------------------------------------
# Store lifecycle
# ---------------------------------------------------------------------------

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
    """Delete all data from the vector store.

    Uses the ChromaDB API to delete the collection (avoids Windows
    file-lock issues with shutil.rmtree).  Falls back to filesystem
    deletion if the API call fails.
    """
    try:
        from langchain_chroma import Chroma
        store = Chroma(
            persist_directory=str(persist_dir),
            collection_name=COLLECTION_NAME,
            embedding_function=get_embeddings(),
        )
        store.delete_collection()
        log.info("Vector store collection deleted: %s", persist_dir)
    except Exception:
        # Fallback: filesystem cleanup
        shutil.rmtree(persist_dir, ignore_errors=True)
        log.info("Vector store directory removed: %s", persist_dir)


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
    except Exception:
        pass  # collection might be empty

    metadata = {**metadata, "doc_type": "table_summary", "table_name": table_name}
    doc = Document(page_content=summary_text, metadata=metadata)
    store.add_documents([doc])
    log.debug("Upserted summary for table %s", table_name)


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
    except Exception:
        pass

    metadata = {**metadata, "doc_type": "relationships"}
    doc = Document(page_content=report_text, metadata=metadata)
    store.add_documents([doc])
    log.debug("Upserted relationship document")


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
    except Exception:
        # Filter syntax varies across ChromaDB versions; fallback
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
    except Exception:
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
