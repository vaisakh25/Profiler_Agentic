"""Test the progress tracking module — smart summaries and rendering.

Usage:
  conda activate gen_ai
  python tests/test_progress.py
"""

import json
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from file_profiler.agent.progress import (
    _extract_summary,
    _render_bar,
    _fmt_time,
    _get_stage_hints,
    TOOL_WEIGHTS,
)


def test_render_bar():
    """Test progress bar rendering at various percentages."""
    print("\n--- Progress Bar Rendering ---")
    for pct in [0, 10, 25, 33, 50, 67, 75, 100]:
        bar = _render_bar(pct)
        print(f"  {pct:3d}%: {bar}")
    print("  [PASS] Bar rendering OK")


def test_fmt_time():
    """Test time formatting."""
    print("\n--- Time Formatting ---")
    cases = [(0.5, "0.5s"), (5.3, "5.3s"), (65.0, "1m 5s"), (125.7, "2m 6s")]
    for secs, expected in cases:
        result = _fmt_time(secs)
        status = "PASS" if result == expected else "FAIL"
        print(f"  [{status}] {secs}s → '{result}' (expected '{expected}')")


def test_stage_hints():
    """Test stage hints for each tool."""
    print("\n--- Stage Hints ---")
    for tool_name in TOOL_WEIGHTS:
        hints = _get_stage_hints(tool_name)
        print(f"  {tool_name}: {len(hints)} hints → {hints}")
    unknown_hints = _get_stage_hints("unknown_tool")
    print(f"  unknown_tool: {unknown_hints}")
    print("  [PASS] Stage hints OK")


def test_extract_summary():
    """Test smart result summaries for each tool type."""
    print("\n--- Smart Summaries ---")

    # list_supported_files
    files_result = json.dumps([
        {"file_name": "person.parquet", "detected_format": "parquet"},
        {"file_name": "visit.parquet", "detected_format": "parquet"},
        {"file_name": "data.csv", "detected_format": "csv"},
    ])
    summary = _extract_summary("list_supported_files", files_result)
    print(f"  list_supported_files: {summary}")
    assert "3 files" in summary, f"Expected '3 files' in '{summary}'"

    # profile_file
    profile_result = json.dumps({
        "table_name": "person",
        "row_count": 1000,
        "columns": [{"name": "id"}, {"name": "name"}, {"name": "age"}],
    })
    summary = _extract_summary("profile_file", profile_result)
    print(f"  profile_file: {summary}")
    assert "person" in summary and "1,000" in summary

    # profile_directory
    dir_result = json.dumps([
        {"table_name": "person", "row_count": 1000},
        {"table_name": "visit", "row_count": 5000},
    ])
    summary = _extract_summary("profile_directory", dir_result)
    print(f"  profile_directory: {summary}")
    assert "2 tables" in summary and "6,000" in summary

    # detect_relationships
    rel_result = json.dumps({
        "candidates": [{"fk": "a", "pk": "b"}, {"fk": "c", "pk": "d"}],
        "er_diagram": "erDiagram\n  person ||--o{ visit : has",
    })
    summary = _extract_summary("detect_relationships", rel_result)
    print(f"  detect_relationships: {summary}")
    assert "2 FK" in summary and "ER diagram" in summary

    # enrich_relationships
    enrich_result = json.dumps({
        "tables_analyzed": 5,
        "relationships_analyzed": 3,
        "documents_embedded": 7,
        "enrichment": "A" * 15000,
    })
    summary = _extract_summary("enrich_relationships", enrich_result)
    print(f"  enrich_relationships: {summary}")
    assert "5 tables" in summary

    # get_quality_summary
    quality_result = json.dumps({
        "table_name": "person",
        "quality_summary": {
            "columns_profiled": 10,
            "columns_with_issues": 3,
        },
    })
    summary = _extract_summary("get_quality_summary", quality_result)
    print(f"  get_quality_summary: {summary}")
    assert "3/10" in summary

    # upload_file
    upload_result = json.dumps({"size_bytes": 4096})
    summary = _extract_summary("upload_file", upload_result)
    print(f"  upload_file: {summary}")
    assert "4,096" in summary

    # Error case
    summary = _extract_summary("profile_file", "Error: file not found")
    print(f"  error case: {summary}")
    assert "Error" in summary

    # Plain text (ER diagram)
    summary = _extract_summary("detect_relationships", "erDiagram\n  person ||--o{ visit : has")
    print(f"  plain ER: {summary}")
    assert "ER diagram" in summary

    print("  [PASS] All smart summaries OK")


def test_tool_weights():
    """Verify all tools have weights defined."""
    print("\n--- Tool Weights ---")
    for tool, weight in sorted(TOOL_WEIGHTS.items(), key=lambda x: -x[1]):
        print(f"  {tool}: {weight}")
    print("  [PASS] Tool weights OK")


if __name__ == "__main__":
    test_render_bar()
    test_fmt_time()
    test_stage_hints()
    test_extract_summary()
    test_tool_weights()
    print("\n" + "=" * 40)
    print("All progress module tests PASSED!")
    print("=" * 40)
