"""Tests for chart generation used by visualize_profile MCP tools."""

from __future__ import annotations

from pathlib import Path

from file_profiler.output.chart_generator import generate_chart


def _sample_profile() -> dict:
    return {
        "table_name": "orders",
        "row_count": 100,
        "quality_summary": {
            "columns_profiled": 4,
            "columns_with_issues": 1,
        },
        "columns": [
            {
                "name": "order_id",
                "inferred_type": "INTEGER",
                "null_count": 0,
                "distinct_count": 100,
                "unique_ratio": 1.0,
                "confidence_score": 1.0,
            },
            {
                "name": "customer_id",
                "inferred_type": "INTEGER",
                "null_count": 1,
                "distinct_count": 80,
                "unique_ratio": 0.8,
                "confidence_score": 0.98,
                "mean": 45.0,
                "median": 43.0,
                "std_dev": 12.0,
                "skewness": 0.4,
                "kurtosis": 0.2,
                "outlier_count": 2,
                "p5": 12.0,
                "p25": 32.0,
                "p75": 55.0,
                "p95": 79.0,
                "top_values": [{"value": "12", "count": 4}],
            },
            {
                "name": "amount",
                "inferred_type": "FLOAT",
                "null_count": 3,
                "distinct_count": 92,
                "unique_ratio": 0.92,
                "confidence_score": 0.99,
                "mean": 120.4,
                "median": 98.6,
                "std_dev": 47.5,
                "skewness": 1.3,
                "kurtosis": 2.1,
                "outlier_count": 5,
                "p5": 20.0,
                "p25": 70.0,
                "p75": 150.0,
                "p95": 240.0,
                "top_values": [{"value": "99.99", "count": 3}],
            },
            {
                "name": "status",
                "inferred_type": "STRING",
                "null_count": 0,
                "distinct_count": 3,
                "unique_ratio": 0.03,
                "confidence_score": 0.95,
                "length_p10": 4,
                "length_p50": 7,
                "length_p90": 10,
                "length_max": 10,
                "top_values": [
                    {"value": "completed", "count": 70},
                    {"value": "pending", "count": 20},
                ],
            },
        ],
    }


def _assert_chart_files(charts: list[dict], output_dir: Path) -> None:
    assert charts
    for chart in charts:
        assert chart["url"].startswith("/charts/")
        filename = chart["url"].split("/charts/")[-1]
        assert (output_dir / "charts" / filename).exists()


def test_generate_overview_creates_chart_files(tmp_path: Path):
    charts = generate_chart(
        chart_type="overview",
        output_dir=tmp_path,
        theme="light",
        profile_dict=_sample_profile(),
    )

    _assert_chart_files(charts, tmp_path)


def test_generate_relationship_confidence_chart(tmp_path: Path):
    relationship_data = {
        "candidates": [
            {
                "confidence": 0.96,
                "fk": {"table_name": "orders", "column_name": "customer_id"},
            },
            {
                "confidence": 0.88,
                "fk": {"table_name": "orders", "column_name": "sales_rep_id"},
            },
        ]
    }

    charts = generate_chart(
        chart_type="relationship_confidence",
        output_dir=tmp_path,
        relationship_data=relationship_data,
    )

    _assert_chart_files(charts, tmp_path)


def test_generate_column_chart_returns_empty_without_column(tmp_path: Path):
    charts = generate_chart(
        chart_type="distribution",
        output_dir=tmp_path,
        profile_dict=_sample_profile(),
        column_name="does_not_exist",
    )
    assert charts == []
