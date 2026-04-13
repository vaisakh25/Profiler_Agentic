from __future__ import annotations

from pathlib import Path

from file_profiler.output.chart_generator import AVAILABLE_CHART_TYPES, generate_chart


def _sample_profile(table_name: str = "orders") -> dict:
    return {
        "table_name": table_name,
        "row_count": 4,
        "file_format": "csv",
        "quality_summary": {
            "columns_with_issues": 1,
            "null_heavy_columns": 0,
            "type_conflict_columns": 0,
            "corrupt_rows_detected": 0,
        },
        "columns": [
            {
                "name": "id",
                "inferred_type": "INTEGER",
                "confidence_score": 1.0,
                "null_count": 0,
                "distinct_count": 4,
                "is_key_candidate": True,
                "mean": 2.5,
                "median": 2.5,
                "std_dev": 1.29,
                "skewness": 0.0,
                "kurtosis": -1.2,
                "p5": 1.15,
                "p25": 1.75,
                "p75": 3.25,
                "p95": 3.85,
                "iqr": 1.5,
                "coefficient_of_variation": 0.52,
                "outlier_count": 0,
                "sample_values": ["1", "2", "3", "4"],
                "top_values": [{"value": "1", "count": 1}, {"value": "2", "count": 1}],
            },
            {
                "name": "amount",
                "inferred_type": "FLOAT",
                "confidence_score": 1.0,
                "null_count": 0,
                "distinct_count": 4,
                "mean": 25.0,
                "median": 25.0,
                "std_dev": 12.91,
                "skewness": 0.0,
                "kurtosis": -1.36,
                "p5": 11.5,
                "p25": 17.5,
                "p75": 32.5,
                "p95": 38.5,
                "iqr": 15.0,
                "coefficient_of_variation": 0.51,
                "outlier_count": 0,
                "sample_values": ["10", "20", "30", "40"],
                "top_values": [{"value": "10", "count": 1}, {"value": "20", "count": 1}],
            },
            {
                "name": "status",
                "inferred_type": "STRING",
                "confidence_score": 0.9,
                "null_count": 0,
                "distinct_count": 2,
                "length_p10": 4,
                "length_p50": 4,
                "length_p90": 7,
                "length_max": 7,
                "sample_values": ["paid", "paid", "pending", "paid"],
                "top_values": [{"value": "paid", "count": 3}, {"value": "pending", "count": 1}],
            },
        ],
    }


def test_chart_registry_matches_documented_surface() -> None:
    assert set(AVAILABLE_CHART_TYPES) == {
        "overview",
        "data_quality_scorecard",
        "null_distribution",
        "type_distribution",
        "cardinality",
        "completeness",
        "numeric_summary",
        "skewness",
        "outlier_summary",
        "correlation_matrix",
        "top_values",
        "string_lengths",
        "distribution",
        "column_detail",
        "overview_directory",
        "row_counts",
        "quality_heatmap",
        "relationship_confidence",
    }


def test_generate_chart_renders_representative_chart_types(tmp_path: Path) -> None:
    profile = _sample_profile()
    profiles = [profile, _sample_profile("customers")]
    relationship_data = {
        "candidates": [
            {
                "fk": {"table_name": "orders", "column_name": "customer_id"},
                "pk": {"table_name": "customers", "column_name": "id"},
                "confidence": 0.91,
            }
        ]
    }

    cases = [
        generate_chart("overview", tmp_path, profile_dict=profile, theme="dark"),
        generate_chart("top_values", tmp_path, profile_dict=profile, column_name="status", theme="light"),
        generate_chart("overview_directory", tmp_path, profile_dicts=profiles, theme="dark"),
        generate_chart("relationship_confidence", tmp_path, relationship_data=relationship_data, theme="light"),
    ]

    for charts in cases:
        assert charts
        assert charts[0]["url"].startswith("/charts/")
        assert Path(charts[0]["path"]).exists()


def test_generate_chart_handles_missing_inputs_without_crashing(tmp_path: Path) -> None:
    profile = _sample_profile()

    assert generate_chart("distribution", tmp_path, profile_dict=profile) == []
    assert generate_chart("correlation_matrix", tmp_path, profile_dict={"table_name": "tiny", "row_count": 1, "columns": []}) == []
    assert generate_chart("relationship_confidence", tmp_path, relationship_data={}) == []
