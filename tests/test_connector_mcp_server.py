"""Regression tests for remote enrichment orchestration."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from file_profiler.connector_mcp_server import (
    _staging_cache,
    remote_check_enrichment_status,
    remote_enrich_relationships,
)
from file_profiler.models.enums import FileFormat, SizeStrategy
from file_profiler.models.file_profile import FileProfile
from file_profiler.models.relationships import RelationshipReport


def _make_profile(table_name: str) -> FileProfile:
    return FileProfile(
        file_format=FileFormat.CSV,
        file_path=f"{table_name}.csv",
        table_name=table_name,
        row_count=10,
        size_strategy=SizeStrategy.MEMORY_SAFE,
    )


@pytest.mark.asyncio
async def test_remote_enrich_status_returns_cached_artifact_paths(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from file_profiler import connector_mcp_server
    from file_profiler.agent import enrichment_mapreduce
    from file_profiler import main as file_profiler_main

    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(connector_mcp_server, "OUTPUT_DIR", output_dir)

    connection_id = "remote-demo"
    profile = _make_profile("orders")
    _staging_cache.clear()
    _staging_cache[connection_id] = [profile]

    async def _fake_batch_enrich(**kwargs):
        return {
            "tables_summarized": 1,
            "tables_cached": 0,
            "column_descriptions": {},
        }

    async def _fake_discover_and_reduce_pipeline(**kwargs):
        staged_output = kwargs["output_dir"]
        assert staged_output == output_dir / "connectors" / connection_id
        staged_output.mkdir(parents=True, exist_ok=True)
        enriched_profiles = staged_output / "enriched_profiles.json"
        enriched_profiles.write_text("[]\n", encoding="utf-8")
        enriched_er = staged_output / "enriched_er_diagram.md"
        enriched_er.write_text("erDiagram\n  ORDERS {\n    int id\n  }\n", encoding="utf-8")
        return {
            "enrichment": "ER diagram ready",
            "tables_analyzed": 1,
            "relationships_analyzed": 0,
            "column_relationships_discovered": 0,
            "cluster_derived_relationships": 0,
            "column_clusters_formed": 0,
            "table_clusters_formed": 1,
            "documents_embedded": 1,
            "enriched_profiles_path": str(enriched_profiles),
            "enriched_er_diagram_path": str(enriched_er),
        }

    monkeypatch.setattr(enrichment_mapreduce, "batch_enrich", _fake_batch_enrich)
    monkeypatch.setattr(
        enrichment_mapreduce,
        "discover_and_reduce_pipeline",
        _fake_discover_and_reduce_pipeline,
    )
    monkeypatch.setattr(
        file_profiler_main,
        "analyze_relationships",
        lambda *args, **kwargs: RelationshipReport(1, 0, []),
    )

    enrich_result = await remote_enrich_relationships(connection_id=connection_id)
    status = await remote_check_enrichment_status(connection_id=connection_id)

    assert Path(enrich_result["enriched_er_diagram_path"]).exists()
    assert Path(enrich_result["enriched_profiles_path"]).exists()
    assert status["status"] == "complete"
    assert status["enriched_er_diagram_path"] == enrich_result["enriched_er_diagram_path"]
    assert status["enriched_profiles_path"] == enrich_result["enriched_profiles_path"]

