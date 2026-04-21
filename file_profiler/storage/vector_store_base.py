"""Abstract vector store backend interface.

Defines the contract that both ChromaDB and pgvector backends implement.
The existing vector_store.py module delegates to whichever backend is active.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional


class VectorStoreBackend(ABC):
    """Abstract interface for vector storage of enrichment embeddings."""

    # --- Store lifecycle ---

    @abstractmethod
    def get_or_create_store(self, collection_name: str = "table_summaries"):
        """Return or create the main table-summaries store."""

    @abstractmethod
    def get_or_create_column_store(self):
        """Return or create the column-descriptions store."""

    @abstractmethod
    def get_or_create_cluster_store(self):
        """Return or create the cluster-summaries store."""

    @abstractmethod
    def clear_store(self) -> None:
        """Delete all data from all collections."""

    # --- Embeddings ---

    @abstractmethod
    def get_embeddings(self):
        """Return the embedding function/model instance."""

    # --- Table summaries ---

    @abstractmethod
    def upsert_table_summary(self, table_name: str, summary_text: str, metadata: dict) -> None:
        """Insert or replace a single table summary."""

    @abstractmethod
    def batch_upsert_table_summaries(self, summaries: dict[str, str], metadata_map: dict[str, dict]) -> int:
        """Batch-insert table summaries. Returns count upserted."""

    @abstractmethod
    def get_all_summaries(self) -> list:
        """Retrieve all table summary documents."""

    @abstractmethod
    def get_stored_fingerprints(self) -> dict[str, str]:
        """Return {table_name: fingerprint} for all stored summaries."""

    @abstractmethod
    def similarity_search(self, query: str, collection: str = "table_summaries", k: int = 10, filter: Optional[dict] = None) -> list:
        """Semantic search across a collection."""

    # --- Column descriptions ---

    @abstractmethod
    def batch_upsert_column_descriptions(self, all_column_descriptions: dict[str, dict], profile_map: dict) -> int:
        """Batch-insert column descriptions. Returns count upserted."""

    # --- Relationship docs ---

    @abstractmethod
    def upsert_relationship_candidates(self, report, profiles: list) -> int:
        """Store per-table relationship summary documents. Returns count."""

    # --- Column embeddings for clustering ---

    @abstractmethod
    def fetch_column_embeddings(self, table_names: Optional[list[str]] = None) -> tuple:
        """Bulk-fetch column embeddings. Returns (col_infos, embedding_matrix)."""

    @abstractmethod
    def build_table_affinity_matrix(self, table_names: list[str], top_k: int = 5, similarity_threshold: float = 0.65) -> tuple:
        """Build table-to-table affinity matrix from column similarities."""
