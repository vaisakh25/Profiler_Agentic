"""pgvector-backed vector store for enrichment embeddings.

Replaces ChromaDB with PostgreSQL + pgvector for production deployments.
Uses the same embedding model (Jina v3 via API) but stores vectors in
PostgreSQL with IVFFlat indexing for cosine similarity search.

Requires:
  - PostgreSQL with the pgvector extension installed
  - pip install pgvector

Falls back to ChromaDB if pgvector is unavailable.
"""

from __future__ import annotations

import json
import logging
from typing import Optional

from langchain_core.documents import Document

log = logging.getLogger(__name__)

# Schema for pgvector tables
_PGVECTOR_SCHEMA_SQL = """\
CREATE EXTENSION IF NOT EXISTS vector;

CREATE TABLE IF NOT EXISTS embeddings (
    id          TEXT PRIMARY KEY,
    doc_type    TEXT NOT NULL,
    table_name  TEXT NOT NULL DEFAULT '',
    column_name TEXT NOT NULL DEFAULT '',
    content     TEXT NOT NULL,
    embedding   vector(1024),
    metadata    JSONB NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_embeddings_doc_type
    ON embeddings (doc_type);
CREATE INDEX IF NOT EXISTS idx_embeddings_table_name
    ON embeddings (table_name);
"""

# IVFFlat index — created after initial data load for best performance
_IVFFLAT_INDEX_SQL = """\
CREATE INDEX IF NOT EXISTS idx_embeddings_vector
    ON embeddings USING ivfflat (embedding vector_cosine_ops)
    WITH (lists = {lists});
"""


class PgVectorStore:
    """PostgreSQL + pgvector vector store backend.

    Stores embeddings in a single `embeddings` table with doc_type
    discriminator (table_summary, column_description, relationship_summary,
    cluster_summary).
    """

    def __init__(self, pool, embedding_fn) -> None:
        self._pool = pool
        self._embedding_fn = embedding_fn
        self._initialized = False

    async def initialize(self) -> None:
        """Create the embeddings table and pgvector extension if needed."""
        if self._initialized:
            return
        async with self._pool.connection() as conn:
            await conn.execute(_PGVECTOR_SCHEMA_SQL)
            await conn.commit()
        self._initialized = True
        log.info("pgvector schema initialized")

    def _embed(self, texts: list[str]) -> list[list[float]]:
        """Embed a batch of texts using the configured embedding function."""
        return self._embedding_fn.embed_documents(texts)

    def _embed_query(self, text: str) -> list[float]:
        """Embed a single query text."""
        return self._embedding_fn.embed_query(text)

    async def clear(self) -> None:
        """Delete all embeddings."""
        async with self._pool.connection() as conn:
            await conn.execute("DELETE FROM embeddings")
            await conn.commit()
        log.info("pgvector store cleared")

    async def clear_by_type(self, doc_type: str) -> None:
        """Delete all embeddings of a specific doc_type."""
        async with self._pool.connection() as conn:
            await conn.execute(
                "DELETE FROM embeddings WHERE doc_type = %s", (doc_type,)
            )
            await conn.commit()

    async def clear_by_table(self, doc_type: str, table_names: list[str]) -> None:
        """Delete embeddings for specific tables and doc_type."""
        if not table_names:
            return
        async with self._pool.connection() as conn:
            await conn.execute(
                "DELETE FROM embeddings WHERE doc_type = %s AND table_name = ANY(%s)",
                (doc_type, table_names),
            )
            await conn.commit()

    async def upsert_documents(self, docs: list[dict]) -> int:
        """Batch upsert documents with embeddings.

        Each doc dict must have: id, doc_type, content, metadata.
        Optional: table_name, column_name.
        """
        if not docs:
            return 0

        texts = [d["content"] for d in docs]
        embeddings = self._embed(texts)

        async with self._pool.connection() as conn:
            for doc, emb in zip(docs, embeddings):
                emb_str = "[" + ",".join(str(v) for v in emb) + "]"
                await conn.execute(
                    """
                    INSERT INTO embeddings (id, doc_type, table_name, column_name,
                                            content, embedding, metadata)
                    VALUES (%s, %s, %s, %s, %s, %s::vector, %s::jsonb)
                    ON CONFLICT (id) DO UPDATE SET
                        content = EXCLUDED.content,
                        embedding = EXCLUDED.embedding,
                        metadata = EXCLUDED.metadata
                    """,
                    (
                        doc["id"],
                        doc["doc_type"],
                        doc.get("table_name", ""),
                        doc.get("column_name", ""),
                        doc["content"],
                        emb_str,
                        json.dumps(doc.get("metadata", {}), default=str),
                    ),
                )
            await conn.commit()

        return len(docs)

    async def similarity_search(
        self,
        query: str,
        doc_type: Optional[str] = None,
        k: int = 10,
    ) -> list[Document]:
        """Cosine similarity search."""
        query_emb = self._embed_query(query)
        emb_str = "[" + ",".join(str(v) for v in query_emb) + "]"

        sql = """
            SELECT content, metadata, table_name, column_name,
                   1 - (embedding <=> %s::vector) AS similarity
            FROM embeddings
        """
        params: list = [emb_str]

        if doc_type:
            sql += " WHERE doc_type = %s"
            params.append(doc_type)

        sql += " ORDER BY embedding <=> %s::vector LIMIT %s"
        params.extend([emb_str, k])

        async with self._pool.connection() as conn:
            cur = await conn.execute(sql, params)
            rows = await cur.fetchall()

        results = []
        for content, metadata, table_name, column_name, similarity in rows:
            meta = metadata if isinstance(metadata, dict) else json.loads(metadata)
            meta["table_name"] = table_name
            meta["column_name"] = column_name
            meta["similarity"] = round(similarity, 4)
            results.append(Document(page_content=content, metadata=meta))

        return results

    async def get_all_by_type(self, doc_type: str) -> list[Document]:
        """Retrieve all documents of a given type."""
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                "SELECT content, metadata, table_name, column_name "
                "FROM embeddings WHERE doc_type = %s ORDER BY table_name",
                (doc_type,),
            )
            rows = await cur.fetchall()

        results = []
        for content, metadata, table_name, column_name in rows:
            meta = metadata if isinstance(metadata, dict) else json.loads(metadata)
            meta["table_name"] = table_name
            meta["column_name"] = column_name
            meta["doc_type"] = doc_type
            results.append(Document(page_content=content, metadata=meta))
        return results

    async def get_fingerprints(self) -> dict[str, str]:
        """Return {table_name: fingerprint} for all table summaries."""
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                "SELECT table_name, metadata->>'fingerprint' "
                "FROM embeddings WHERE doc_type = 'table_summary' "
                "AND table_name != ''",
            )
            rows = await cur.fetchall()
        return {row[0]: row[1] or "" for row in rows}

    async def fetch_column_embeddings(
        self,
        table_names: Optional[list[str]] = None,
    ) -> tuple[list[dict], list[list[float]]]:
        """Bulk-fetch column embeddings and metadata.

        Returns (col_infos, embedding_vectors) for clustering.
        """
        sql = (
            "SELECT table_name, column_name, metadata, embedding::text "
            "FROM embeddings WHERE doc_type = 'column_description'"
        )
        params: list = []
        if table_names:
            sql += " AND table_name = ANY(%s)"
            params.append(table_names)
        sql += " ORDER BY table_name, column_name"

        async with self._pool.connection() as conn:
            cur = await conn.execute(sql, params)
            rows = await cur.fetchall()

        col_infos = []
        vectors = []
        for table_name, column_name, metadata, emb_text in rows:
            meta = metadata if isinstance(metadata, dict) else json.loads(metadata)
            col_infos.append({
                "table_name": table_name,
                "column_name": column_name,
                "column_type": meta.get("column_type", ""),
                "role": meta.get("role", "regular"),
            })
            # Parse vector string "[0.1,0.2,...]" to list of floats
            emb = [float(x) for x in emb_text.strip("[]").split(",")]
            vectors.append(emb)

        return col_infos, vectors

    async def create_ivfflat_index(self, n_docs: int = 1000) -> None:
        """Create or rebuild IVFFlat index for fast similarity search.

        Should be called after bulk data loading.  The number of lists
        is tuned to sqrt(n_docs).
        """
        import math
        lists = max(1, int(math.sqrt(n_docs)))
        async with self._pool.connection() as conn:
            await conn.execute("DROP INDEX IF EXISTS idx_embeddings_vector")
            await conn.execute(_IVFFLAT_INDEX_SQL.format(lists=lists))
            await conn.commit()
        log.info("pgvector IVFFlat index created (lists=%d)", lists)


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

_store: Optional[PgVectorStore] = None


async def get_pgvector_store() -> Optional[PgVectorStore]:
    """Return the pgvector store singleton, or None if unavailable.

    Checks: (1) pgvector package installed, (2) PostgreSQL configured,
    (3) pgvector extension available in the database.
    """
    global _store
    if _store is not None:
        return _store

    try:
        from file_profiler.config.database import get_pool
        from file_profiler.agent.vector_store import get_embeddings

        pool = await get_pool()
        if not pool:
            return None

        store = PgVectorStore(pool, get_embeddings())
        await store.initialize()
        _store = store
        log.info("pgvector store initialized")
        return _store

    except Exception as exc:
        log.debug("pgvector store unavailable: %s", exc)
        return None
