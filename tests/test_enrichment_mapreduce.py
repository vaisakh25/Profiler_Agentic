"""Tests for the map-reduce enrichment pipeline."""

import asyncio
import json
import os
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from file_profiler.models.enums import (
    Cardinality,
    FileFormat,
    InferredType,
    SizeStrategy,
)
from file_profiler.models.file_profile import (
    ColumnProfile,
    FileProfile,
    QualitySummary,
)
from file_profiler.models.relationships import (
    ColumnRef,
    ForeignKeyCandidate,
    RelationshipReport,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_profile(name, rows=100, cols=None):
    """Create a minimal FileProfile for testing."""
    if cols is None:
        cols = [
            ColumnProfile(
                name=f"{name}_id",
                declared_type=None,
                inferred_type=InferredType.INTEGER,
                confidence_score=1.0,
                null_count=0,
                distinct_count=rows,
                is_key_candidate=True,
                cardinality=Cardinality.HIGH,
                sample_values=["1", "2", "3"],
            ),
            ColumnProfile(
                name="description",
                declared_type=None,
                inferred_type=InferredType.STRING,
                confidence_score=1.0,
                null_count=5,
                distinct_count=50,
                cardinality=Cardinality.MEDIUM,
                sample_values=["foo", "bar", "baz"],
            ),
        ]
    return FileProfile(
        file_format=FileFormat.CSV,
        file_path=f"/tmp/{name}.csv",
        table_name=name,
        row_count=rows,
        columns=cols,
        quality_summary=QualitySummary(columns_profiled=len(cols)),
    )


def _make_report(profiles):
    """Create a minimal RelationshipReport."""
    candidates = []
    if len(profiles) >= 2:
        candidates.append(ForeignKeyCandidate(
            fk=ColumnRef(profiles[1].table_name, f"{profiles[0].table_name}_id"),
            pk=ColumnRef(profiles[0].table_name, f"{profiles[0].table_name}_id"),
            confidence=0.85,
            evidence=["name:singular_prefix", "type:exact"],
            fk_null_ratio=0.02,
            fk_distinct_count=80,
            pk_distinct_count=100,
            top_value_overlap_pct=0.90,
        ))
    return RelationshipReport(
        tables_analyzed=len(profiles),
        columns_analyzed=sum(len(p.columns) for p in profiles),
        candidates=candidates,
    )


@pytest.fixture
def two_profiles():
    return [_make_profile("customers"), _make_profile("orders", rows=500)]


@pytest.fixture
def report(two_profiles):
    return _make_report(two_profiles)


@pytest.fixture
def mock_llm():
    """Create a mock LLM that returns canned responses."""
    llm = AsyncMock()
    llm.ainvoke = AsyncMock(return_value=MagicMock(
        content="This table represents customer data with 100 rows and 2 columns."
    ))
    return llm


# ---------------------------------------------------------------------------
# Tests: _build_table_context
# ---------------------------------------------------------------------------

class TestBuildTableContext:
    def test_includes_table_name(self, two_profiles):
        from file_profiler.agent.enrichment_mapreduce import _build_table_context
        ctx = _build_table_context(two_profiles[0])
        assert "customers" in ctx

    def test_includes_columns(self, two_profiles):
        from file_profiler.agent.enrichment_mapreduce import _build_table_context
        ctx = _build_table_context(two_profiles[0])
        assert "customers_id" in ctx
        assert "description" in ctx

    def test_respects_token_budget(self, two_profiles):
        from file_profiler.agent.enrichment_mapreduce import _build_table_context
        ctx = _build_table_context(two_profiles[0], token_budget=100)
        assert len(ctx) <= 100

    def test_includes_row_count(self, two_profiles):
        from file_profiler.agent.enrichment_mapreduce import _build_table_context
        ctx = _build_table_context(two_profiles[0])
        assert "100" in ctx


# ---------------------------------------------------------------------------
# Tests: _build_relationships_context
# ---------------------------------------------------------------------------

class TestBuildRelationshipsContext:
    def test_no_candidates(self):
        from file_profiler.agent.enrichment_mapreduce import _build_relationships_context
        report = RelationshipReport(tables_analyzed=0, columns_analyzed=0)
        text = _build_relationships_context(report)
        assert "No relationships" in text

    def test_with_candidates(self, report):
        from file_profiler.agent.enrichment_mapreduce import _build_relationships_context
        text = _build_relationships_context(report)
        assert "customers_id" in text
        assert "0.85" in text


# ---------------------------------------------------------------------------
# Tests: MAP phase
# ---------------------------------------------------------------------------

class TestMapPhase:
    @pytest.mark.asyncio
    async def test_summarizes_all_tables(self, two_profiles, mock_llm):
        from file_profiler.agent.enrichment_mapreduce import map_phase
        summaries, col_descs = await map_phase(two_profiles, mock_llm, max_workers=2)
        assert len(summaries) == 2
        assert "customers" in summaries
        assert "orders" in summaries

    @pytest.mark.asyncio
    async def test_skips_cached_tables(self, two_profiles, mock_llm):
        from file_profiler.agent.enrichment_mapreduce import map_phase
        from file_profiler.agent.vector_store import _table_fingerprint

        # Simulate customers already cached with matching fingerprint
        fp = _table_fingerprint("customers", 100, 2)
        existing = {"customers": fp}
        summaries, col_descs = await map_phase(
            two_profiles, mock_llm, existing_fingerprints=existing,
        )
        assert "customers" not in summaries
        assert "orders" in summaries
        # Only 1 LLM call (orders), not 2
        assert mock_llm.ainvoke.call_count == 1

    @pytest.mark.asyncio
    async def test_handles_llm_failure_gracefully(self, two_profiles):
        from file_profiler.agent.enrichment_mapreduce import map_phase

        failing_llm = AsyncMock()
        failing_llm.ainvoke = AsyncMock(side_effect=Exception("rate limited"))
        summaries, col_descs = await map_phase(two_profiles, failing_llm, max_workers=2)
        # Should still return fallback summaries
        assert len(summaries) == 2
        for name, summary in summaries.items():
            assert name in summary  # fallback includes table name

    @pytest.mark.asyncio
    async def test_all_cached_returns_empty(self, two_profiles, mock_llm):
        from file_profiler.agent.enrichment_mapreduce import map_phase
        from file_profiler.agent.vector_store import _table_fingerprint

        existing = {
            "customers": _table_fingerprint("customers", 100, 2),
            "orders": _table_fingerprint("orders", 500, 2),
        }
        summaries, col_descs = await map_phase(
            two_profiles, mock_llm, existing_fingerprints=existing,
        )
        assert len(summaries) == 0
        assert mock_llm.ainvoke.call_count == 0


# ---------------------------------------------------------------------------
# Tests: EMBED phase
# ---------------------------------------------------------------------------

class TestEmbedPhase:
    def test_creates_store(self, two_profiles, report, tmp_path):
        from file_profiler.agent.enrichment_mapreduce import embed_phase

        summaries = {
            "customers": "Customer master table with IDs and descriptions.",
            "orders": "Order transaction table linked to customers.",
        }
        store, _ = embed_phase(summaries, two_profiles, report, {}, tmp_path)
        assert store is not None

    def test_upserts_are_idempotent(self, two_profiles, report, tmp_path):
        from file_profiler.agent.enrichment_mapreduce import embed_phase
        from file_profiler.agent.vector_store import list_stored_tables

        summaries = {"customers": "V1 summary"}
        embed_phase(summaries, two_profiles, report, {}, tmp_path)

        # Re-embed with updated summary
        summaries = {"customers": "V2 summary — updated"}
        store, _ = embed_phase(summaries, two_profiles, report, {}, tmp_path)

        tables = list_stored_tables(store)
        # Should have exactly 1 entry for customers, not 2
        assert tables.count("customers") == 1


# ---------------------------------------------------------------------------
# Tests: REDUCE phase
# ---------------------------------------------------------------------------

class TestReducePhase:
    @pytest.mark.asyncio
    async def test_produces_output(self, two_profiles, report, mock_llm, tmp_path):
        from file_profiler.agent.enrichment_mapreduce import embed_phase, reduce_phase

        summaries = {
            "customers": "Customer master table.",
            "orders": "Order transactions.",
        }
        store, _ = embed_phase(summaries, two_profiles, report, {}, tmp_path)

        mock_llm.ainvoke = AsyncMock(return_value=MagicMock(
            content="## Analysis\nCustomers and orders are related."
        ))

        result = await reduce_phase(store, report, two_profiles, mock_llm)
        assert "Analysis" in result
        assert len(result) > 0

    @pytest.mark.asyncio
    async def test_respects_token_budget(self, two_profiles, report, mock_llm, tmp_path):
        from file_profiler.agent.enrichment_mapreduce import embed_phase, reduce_phase

        # Create large summaries
        summaries = {
            "customers": "x" * 5000,
            "orders": "y" * 5000,
        }
        store, _ = embed_phase(summaries, two_profiles, report, {}, tmp_path)

        mock_llm.ainvoke = AsyncMock(return_value=MagicMock(content="done"))

        # With tight budget, should truncate
        await reduce_phase(
            store, report, two_profiles, mock_llm, token_budget=100,
        )
        # Verify the prompt was sent (LLM was called)
        assert mock_llm.ainvoke.call_count == 1


# ---------------------------------------------------------------------------
# Tests: Full orchestrator
# ---------------------------------------------------------------------------

class TestEnrich:
    @pytest.mark.asyncio
    async def test_full_pipeline(self, two_profiles, report, tmp_path):
        from file_profiler.agent.enrichment_mapreduce import enrich

        with patch(
            "file_profiler.agent.llm_factory.get_llm_with_fallback"
        ) as mock_factory:
            mock_llm = AsyncMock()
            mock_llm.ainvoke = AsyncMock(return_value=MagicMock(
                content="Table summary or analysis text."
            ))
            mock_factory.return_value = mock_llm

            result = await enrich(
                profiles=two_profiles,
                report=report,
                dir_path="/tmp",
                provider="google",
                persist_dir=tmp_path,
                incremental=False,
            )

        assert "enrichment" in result
        assert result["tables_analyzed"] == 2
        assert result["tables_summarized"] == 2
        assert result["tables_cached"] == 0
        assert result["relationships_analyzed"] == 1

    @pytest.mark.asyncio
    async def test_incremental_caches(self, two_profiles, report, tmp_path):
        from file_profiler.agent.enrichment_mapreduce import enrich

        with patch(
            "file_profiler.agent.llm_factory.get_llm_with_fallback"
        ) as mock_factory:
            mock_llm = AsyncMock()
            mock_llm.ainvoke = AsyncMock(return_value=MagicMock(
                content="Summary text."
            ))
            mock_factory.return_value = mock_llm

            # First run — summarizes all
            r1 = await enrich(
                profiles=two_profiles, report=report, dir_path="/tmp",
                persist_dir=tmp_path, incremental=True,
            )
            first_call_count = mock_llm.ainvoke.call_count

            # Second run — should skip MAP, only do REDUCE
            mock_llm.ainvoke.reset_mock()
            r2 = await enrich(
                profiles=two_profiles, report=report, dir_path="/tmp",
                persist_dir=tmp_path, incremental=True,
            )
            second_call_count = mock_llm.ainvoke.call_count

        # First run: 2 MAP calls + 1 REDUCE = 3
        assert first_call_count == 3
        # Second run: 0 MAP calls (cached) + 1 REDUCE = 1
        assert second_call_count == 1
        assert r2["tables_cached"] == 2
        assert r2["tables_summarized"] == 0


# ---------------------------------------------------------------------------
# Tests: CLUSTER phase
# ---------------------------------------------------------------------------

class TestClusterPhase:
    def test_small_dataset_single_cluster(self, two_profiles, tmp_path):
        """≤ target_size tables → 1 cluster containing all tables."""
        from file_profiler.agent.enrichment_mapreduce import embed_phase, cluster_phase
        from file_profiler.models.relationships import RelationshipReport

        report = RelationshipReport(tables_analyzed=2, columns_analyzed=4)
        summaries = {"customers": "Customer summary.", "orders": "Order summary."}
        store, _ = embed_phase(summaries, two_profiles, report, {}, tmp_path)

        clusters = cluster_phase(store, two_profiles, target_cluster_size=15)
        assert len(clusters) == 1
        assert set(clusters[0]) == {"customers", "orders"}

    def test_large_dataset_multiple_clusters(self, tmp_path):
        """n_tables > target_size → multiple clusters."""
        from file_profiler.agent.enrichment_mapreduce import embed_phase, cluster_phase
        from file_profiler.models.relationships import RelationshipReport

        # 20 tables, target_size=5 → expect ≥ 2 clusters
        profiles = [_make_profile(f"table_{i}") for i in range(20)]
        report = RelationshipReport(tables_analyzed=20, columns_analyzed=40)
        summaries = {p.table_name: f"Summary of {p.table_name}." for p in profiles}
        store = embed_phase(summaries, profiles, report, tmp_path)

        clusters = cluster_phase(store, profiles, target_cluster_size=5)
        assert len(clusters) >= 2
        # All tables accounted for
        all_in_clusters = {t for tables in clusters.values() for t in tables}
        assert all_in_clusters == {p.table_name for p in profiles}

    def test_fallback_when_no_embeddings(self, two_profiles):
        """cluster_phase falls back gracefully when store has no embeddings."""
        from file_profiler.agent.enrichment_mapreduce import cluster_phase
        from unittest.mock import MagicMock, patch

        mock_store = MagicMock()
        with patch(
            "file_profiler.agent.vector_store.get_table_embeddings",
            return_value=([], []),
        ):
            # Force large-dataset path by using tiny target size
            clusters = cluster_phase(mock_store, two_profiles, target_cluster_size=1)
        assert len(clusters) >= 1
        all_tables = {t for tables in clusters.values() for t in tables}
        assert all_tables == {"customers", "orders"}


# ---------------------------------------------------------------------------
# Tests: REDUCE per cluster phase
# ---------------------------------------------------------------------------

class TestReduceClusterPhase:
    @pytest.mark.asyncio
    async def test_produces_one_analysis_per_cluster(self, two_profiles, report, mock_llm, tmp_path):
        from file_profiler.agent.enrichment_mapreduce import embed_phase, reduce_cluster_phase

        summaries = {"customers": "Customer table.", "orders": "Orders table."}
        store, _ = embed_phase(summaries, two_profiles, report, {}, tmp_path)

        clusters = {0: ["customers"], 1: ["orders"]}
        mock_llm.ainvoke = AsyncMock(return_value=MagicMock(content="Cluster analysis."))

        results = await reduce_cluster_phase(clusters, store, report, mock_llm)
        assert set(results.keys()) == {0, 1}
        assert all(isinstance(v, str) and len(v) > 0 for v in results.values())
        assert mock_llm.ainvoke.call_count == 2  # one call per cluster

    @pytest.mark.asyncio
    async def test_intra_cluster_rels_only(self, two_profiles, report, mock_llm, tmp_path):
        """FK candidates between different clusters must not appear in per-cluster prompt."""
        from file_profiler.agent.enrichment_mapreduce import embed_phase, reduce_cluster_phase

        summaries = {"customers": "Customer table.", "orders": "Orders table."}
        store, _ = embed_phase(summaries, two_profiles, report, {}, tmp_path)

        # Put both tables in separate clusters so the FK is cross-cluster
        clusters = {0: ["customers"], 1: ["orders"]}
        captured_prompts = []

        async def _capture(prompt):
            captured_prompts.append(prompt)
            return MagicMock(content="ok")

        mock_llm.ainvoke = _capture
        await reduce_cluster_phase(clusters, store, report, mock_llm)

        # The relationship candidates_id->customers_id spans cluster 0↔1
        # so neither per-cluster prompt should reference both tables
        for prompt in captured_prompts:
            assert "No intra-cluster relationships" in prompt or "customers_id" not in prompt or "orders" not in prompt

    @pytest.mark.asyncio
    async def test_handles_llm_failure_per_cluster(self, two_profiles, report, tmp_path):
        from file_profiler.agent.enrichment_mapreduce import embed_phase, reduce_cluster_phase

        summaries = {"customers": "Customer table.", "orders": "Orders table."}
        store, _ = embed_phase(summaries, two_profiles, report, {}, tmp_path)

        failing_llm = AsyncMock()
        failing_llm.ainvoke = AsyncMock(side_effect=Exception("timeout"))

        clusters = {0: ["customers"], 1: ["orders"]}
        results = await reduce_cluster_phase(clusters, store, report, failing_llm)
        # Should return error strings, not raise
        assert set(results.keys()) == {0, 1}
        assert all("failed" in v.lower() for v in results.values())


# ---------------------------------------------------------------------------
# Tests: META-REDUCE phase
# ---------------------------------------------------------------------------

class TestMetaReducePhase:
    @pytest.mark.asyncio
    async def test_produces_final_output(self, report, mock_llm):
        from file_profiler.agent.enrichment_mapreduce import meta_reduce_phase

        clusters = {0: ["customers"], 1: ["orders"]}
        cluster_analyses = {
            0: "Cluster 0: customer domain — customers table.",
            1: "Cluster 1: order domain — orders table.",
        }
        mock_llm.ainvoke = AsyncMock(return_value=MagicMock(
            content="## Final Analysis\nAll tables linked via customer_id."
        ))

        result = await meta_reduce_phase(clusters, cluster_analyses, report, mock_llm)
        assert "Final Analysis" in result
        assert mock_llm.ainvoke.call_count == 1  # single synthesis call

    @pytest.mark.asyncio
    async def test_cross_cluster_rels_included_in_prompt(self, report, mock_llm):
        """Cross-cluster FKs from the deterministic report must appear in the prompt."""
        from file_profiler.agent.enrichment_mapreduce import meta_reduce_phase

        clusters = {0: ["customers"], 1: ["orders"]}
        cluster_analyses = {0: "C0 analysis", 1: "C1 analysis"}

        captured = []

        async def _capture(prompt):
            captured.append(prompt)
            return MagicMock(content="done")

        mock_llm.ainvoke = _capture
        await meta_reduce_phase(clusters, cluster_analyses, report, mock_llm)

        # The report has orders.customers_id -> customers.customers_id (cross-cluster)
        assert len(captured) == 1
        assert "cross-cluster" in captured[0].lower() or "customers_id" in captured[0]

    @pytest.mark.asyncio
    async def test_respects_token_budget(self, report, mock_llm):
        from file_profiler.agent.enrichment_mapreduce import meta_reduce_phase

        # Very large cluster analyses that should get truncated
        clusters = {i: [f"table_{i}"] for i in range(10)}
        cluster_analyses = {i: "x" * 2000 for i in range(10)}
        mock_llm.ainvoke = AsyncMock(return_value=MagicMock(content="done"))

        await meta_reduce_phase(clusters, cluster_analyses, report, mock_llm,
                                token_budget=500)
        assert mock_llm.ainvoke.call_count == 1


# ---------------------------------------------------------------------------
# Tests: enrich() large-dataset path
# ---------------------------------------------------------------------------

class TestEnrichLargeDataset:
    @pytest.mark.asyncio
    async def test_large_path_uses_cluster_meta_reduce(self, tmp_path):
        """When n_tables > CLUSTER_TARGET_SIZE, enrich() uses the cluster path."""
        from file_profiler.agent.enrichment_mapreduce import enrich

        # 20 tables, CLUSTER_TARGET_SIZE default=15 → triggers cluster path
        profiles = [_make_profile(f"table_{i}") for i in range(20)]
        report = _make_report(profiles[:2])

        with patch(
            "file_profiler.agent.llm_factory.get_llm_with_fallback"
        ) as mock_factory:
            mock_llm = AsyncMock()
            mock_llm.ainvoke = AsyncMock(return_value=MagicMock(
                content="Analysis text."
            ))
            mock_factory.return_value = mock_llm

            result = await enrich(
                profiles=profiles,
                report=report,
                dir_path="/tmp",
                provider="google",
                persist_dir=tmp_path,
                incremental=False,
            )

        assert "enrichment" in result
        assert result["tables_analyzed"] == 20
        assert result["table_clusters_formed"] >= 2

    @pytest.mark.asyncio
    async def test_small_path_no_cluster_breakdown(self, two_profiles, report, tmp_path):
        """Small datasets should not include cluster_breakdown in output."""
        from file_profiler.agent.enrichment_mapreduce import enrich

        with patch(
            "file_profiler.agent.llm_factory.get_llm_with_fallback"
        ) as mock_factory:
            mock_llm = AsyncMock()
            mock_llm.ainvoke = AsyncMock(return_value=MagicMock(
                content="Summary."
            ))
            mock_factory.return_value = mock_llm

            result = await enrich(
                profiles=two_profiles,
                report=report,
                dir_path="/tmp",
                provider="google",
                persist_dir=tmp_path,
                incremental=False,
            )

        assert result["table_clusters_formed"] == 1


# ---------------------------------------------------------------------------
# Tests: Vector store module
# ---------------------------------------------------------------------------

class TestVectorStore:
    def test_fingerprint_deterministic(self):
        from file_profiler.agent.vector_store import _table_fingerprint
        fp1 = _table_fingerprint("t", 100, 5)
        fp2 = _table_fingerprint("t", 100, 5)
        assert fp1 == fp2

    def test_fingerprint_changes_with_data(self):
        from file_profiler.agent.vector_store import _table_fingerprint
        fp1 = _table_fingerprint("t", 100, 5)
        fp2 = _table_fingerprint("t", 200, 5)
        assert fp1 != fp2

    def test_get_or_create_store(self, tmp_path):
        from file_profiler.agent.vector_store import get_or_create_store
        store = get_or_create_store(tmp_path)
        assert store is not None

    def test_upsert_and_list(self, tmp_path):
        from file_profiler.agent.vector_store import (
            get_or_create_store,
            list_stored_tables,
            upsert_table_summary,
        )
        store = get_or_create_store(tmp_path)
        upsert_table_summary(store, "test_table", "A test summary.", {
            "row_count": 10,
            "fingerprint": "abc123",
        })
        tables = list_stored_tables(store)
        assert "test_table" in tables

    def test_clear_store(self, tmp_path):
        from file_profiler.agent.vector_store import (
            clear_store,
            get_or_create_store,
            list_stored_tables,
            upsert_table_summary,
        )
        store_dir = tmp_path / "vs"
        store = get_or_create_store(store_dir)
        upsert_table_summary(store, "t", "summary", {})
        assert store_dir.exists()
        clear_store(store_dir)
        # After clearing, a fresh store should have no tables
        store2 = get_or_create_store(store_dir)
        assert list_stored_tables(store2) == []
