"""Deterministic E2E test for the enrichment pipeline.

Runs: profile -> detect relationships -> build documents -> enrich wrapper.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from file_profiler.agent.enrichment import build_documents, enrich, extract_sample_rows
from file_profiler.main import analyze_relationships, profile_directory


def _write_fixture_parquet_tables(base_dir: Path) -> None:
    person = pd.DataFrame(
        {
            "person_id": [1, 2, 3],
            "gender": ["F", "M", "F"],
            "birth_year": [1988, 1993, 1977],
        }
    )
    visit = pd.DataFrame(
        {
            "visit_occurrence_id": [101, 102, 103],
            "person_id": [1, 2, 3],
            "visit_type": ["ER", "IP", "OP"],
        }
    )
    condition = pd.DataFrame(
        {
            "condition_occurrence_id": [1001, 1002, 1003],
            "person_id": [1, 2, 3],
            "visit_occurrence_id": [101, 102, 103],
            "condition_code": ["J45", "E11", "I10"],
        }
    )

    person.to_parquet(base_dir / "person.parquet", index=False)
    visit.to_parquet(base_dir / "visit_occurrence.parquet", index=False)
    condition.to_parquet(base_dir / "condition_occurrence.parquet", index=False)


@pytest.mark.asyncio
async def test_enrichment(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    test_dir = tmp_path / "tables"
    output_dir = tmp_path / "output"
    test_dir.mkdir(parents=True, exist_ok=True)
    output_dir.mkdir(parents=True, exist_ok=True)

    _write_fixture_parquet_tables(test_dir)

    profiles = profile_directory(str(test_dir), output_dir=str(output_dir))
    assert len(profiles) == 3

    report = analyze_relationships(
        profiles,
        output_path=str(output_dir / "test_relationships.json"),
    )
    assert report.tables_analyzed == 3

    docs = build_documents(profiles, report, str(test_dir))
    assert len(docs) >= 4

    for profile in profiles:
        rows = extract_sample_rows(profile.file_path, n=3)
        assert rows, f"Expected sample rows for {profile.table_name}"

    async def _fake_mapreduce_enrich(*args, **kwargs):
        return {
            "enrichment": (
                "```mermaid\n"
                "erDiagram\n"
                "  PERSON ||--o{ VISIT_OCCURRENCE : \"person_id -> person_id\"\n"
                "```"
            ),
            "tables_analyzed": len(profiles),
            "relationships_analyzed": len(report.candidates),
            "documents_embedded": len(docs),
        }

    import file_profiler.agent.enrichment_mapreduce as enrichment_mapreduce

    monkeypatch.setattr(enrichment_mapreduce, "enrich", _fake_mapreduce_enrich)

    result = await enrich(
        profiles=profiles,
        report=report,
        dir_path=str(test_dir),
        provider="google",
        model="gemini-2.5-flash",
    )

    assert result["tables_analyzed"] == 3
    assert result["documents_embedded"] >= 4
    assert "erDiagram" in result["enrichment"]
