"""Regression tests for remote enrichment orchestration."""

from __future__ import annotations

from pathlib import Path

import pytest

from file_profiler.connector_mcp_server import (
    _staging_cache,
    remote_detect_relationships,
    remote_check_enrichment_status,
    remote_enrich_relationships,
    remote_visualize_profile,
)
from file_profiler.models.enums import FileFormat, SizeStrategy
from file_profiler.models.file_profile import FileProfile
from file_profiler.models.relationships import RelationshipReport


def _make_profile(
    table_name: str,
    *,
    connection_id: str = "",
    source_uri: str = "minio://demo-bucket/sales/",
) -> FileProfile:
    return FileProfile(
        file_format=FileFormat.CSV,
        file_path=f"{source_uri.rstrip('/')}/{table_name}.csv",
        table_name=table_name,
        row_count=10,
        size_strategy=SizeStrategy.MEMORY_SAFE,
        source_uri=source_uri,
        connection_id=connection_id or None,
    )


def _write_staged_profile(output_dir: Path, connection_id: str, profile: FileProfile) -> None:
    from file_profiler.output.profile_writer import write as _write_profile

    staging = output_dir / "connectors" / connection_id
    staging.mkdir(parents=True, exist_ok=True)
    _write_profile(profile, staging / f"{profile.table_name}_profile.json")


def _seed_remote_source_state(
    connector_mcp_server,
    *,
    connection_id: str,
    profiles: list[FileProfile],
    uri: str,
    profile_epoch: str = "epoch-1",
) -> None:
    table_fingerprints = connector_mcp_server._compute_fingerprints(profiles)
    source_fingerprint = connector_mcp_server._compute_source_fingerprint(table_fingerprints)
    connector_mcp_server._write_source_state(
        connection_id,
        uri=uri,
        profiling_method=connector_mcp_server._REMOTE_PROFILE_METHOD,
        table_fingerprints=table_fingerprints,
        source_fingerprint=source_fingerprint,
        profile_epoch=profile_epoch,
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
    profile = _make_profile("orders", connection_id=connection_id)
    _staging_cache.clear()
    _staging_cache[connection_id] = [profile]
    _seed_remote_source_state(
        connector_mcp_server,
        connection_id=connection_id,
        profiles=[profile],
        uri="minio://demo-bucket/sales/",
        profile_epoch="epoch-a",
    )

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

    # A new profiling epoch invalidates prior enrichment, even if fingerprints match.
    _seed_remote_source_state(
        connector_mcp_server,
        connection_id=connection_id,
        profiles=[profile],
        uri="minio://demo-bucket/sales/",
        profile_epoch="epoch-b",
    )
    stale_status = await remote_check_enrichment_status(connection_id=connection_id)
    assert stale_status["status"] == "stale"
    assert "epoch" in stale_status["reason"].lower()


@pytest.mark.asyncio
async def test_remote_detect_relationships_reloads_staged_profiles_from_disk(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from file_profiler import connector_mcp_server

    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(connector_mcp_server, "OUTPUT_DIR", output_dir)

    connection_id = "cold-cache"
    _staging_cache.clear()

    _write_staged_profile(
        output_dir,
        connection_id,
        _make_profile("orders", connection_id=connection_id),
    )
    _write_staged_profile(
        output_dir,
        connection_id,
        _make_profile("customers", connection_id=connection_id),
    )

    _seed_remote_source_state(
        connector_mcp_server,
        connection_id=connection_id,
        profiles=[
            _make_profile("orders", connection_id=connection_id),
            _make_profile("customers", connection_id=connection_id),
        ],
        uri="minio://demo-bucket/sales/",
    )

    result = await remote_detect_relationships(connection_id=connection_id)

    assert result["status"] == "intermediate"
    assert result["connection_id"] == connection_id
    assert connection_id in _staging_cache
    assert len(_staging_cache[connection_id]) == 2


@pytest.mark.asyncio
async def test_remote_detect_relationships_recovers_from_source_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from file_profiler import connector_mcp_server
    from file_profiler import main as file_profiler_main

    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(connector_mcp_server, "OUTPUT_DIR", output_dir)

    connection_id = "recover-source-state"
    source_uri = "minio://demo-bucket/orders.csv"
    profile_remote_calls: list[str] = []
    expected_output_dir = str(output_dir)

    _staging_cache.clear()
    connector_mcp_server._write_source_state(connection_id, uri=source_uri)

    def _fake_profile_remote(
        uri: str,
        connection_id: str | None = None,
        table_filter: list[str] | None = None,
        output_dir: str | Path | None = None,
        progress_callback=None,
    ):
        profile_remote_calls.append(uri)
        assert connection_id == "recover-source-state"
        assert str(output_dir) == expected_output_dir
        return _make_profile(
            "orders",
            connection_id="recover-source-state",
            source_uri=source_uri,
        )

    monkeypatch.setattr(file_profiler_main, "profile_remote", _fake_profile_remote)
    monkeypatch.setattr(
        file_profiler_main,
        "analyze_relationships",
        lambda *args, **kwargs: RelationshipReport(1, 0, []),
    )

    result = await remote_detect_relationships(connection_id=connection_id)

    assert result["status"] == "intermediate"
    assert result["connection_id"] == connection_id
    assert profile_remote_calls == [source_uri]
    assert connection_id in _staging_cache
    assert len(_staging_cache[connection_id]) == 1
    assert (
        output_dir
        / "connectors"
        / connection_id
        / "orders_profile.json"
    ).exists()


@pytest.mark.asyncio
async def test_remote_detect_relationships_rejects_non_locked_strategy_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from file_profiler import connector_mcp_server

    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(connector_mcp_server, "OUTPUT_DIR", output_dir)

    connection_id = "strategy-lock"
    _staging_cache.clear()

    _write_staged_profile(
        output_dir,
        connection_id,
        _make_profile("orders", connection_id=connection_id),
    )
    connector_mcp_server._write_source_state(
        connection_id,
        uris=[
            "minio://demo-bucket/sales/orders.csv",
            "minio://demo-bucket/sales/customers.csv",
        ],
        profiling_method="profile_multiple_remote_files",
    )

    result = await remote_detect_relationships(connection_id=connection_id)

    assert "error" in result
    assert "profile_remote_source" in result["error"]


@pytest.mark.asyncio
async def test_remote_visualize_profile_unavailable_when_chart_module_missing() -> None:
    result = await remote_visualize_profile()

    assert result["status"] == "unavailable"
    assert result["error"] == "visualization_unavailable"

