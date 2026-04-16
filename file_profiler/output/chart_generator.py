"""
Chart generation backend for profiled table visualizations.

This module is shared by both the local and remote MCP servers.
"""

from __future__ import annotations

import hashlib
import json
import math
import re
from pathlib import Path
from typing import Any

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt
from matplotlib.patches import Patch
import pandas as pd
import seaborn as sns

from file_profiler.observability.langsmith import compact_text_output, traceable


AVAILABLE_CHART_TYPES: tuple[str, ...] = (
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
)

_THEMES: dict[str, dict[str, Any]] = {
    "dark": {
        "style": "darkgrid",
        "facecolor": "#10151d",
        "axes_facecolor": "#151c26",
        "text": "#f3f6fb",
        "muted": "#a8b3c7",
        "grid": "#314052",
        "accent": "#4cc9f0",
        "accent2": "#90be6d",
        "accent3": "#f8961e",
        "danger": "#f94144",
        "warning": "#f9c74f",
        "info": "#577590",
    },
    "light": {
        "style": "whitegrid",
        "facecolor": "#ffffff",
        "axes_facecolor": "#f8fafc",
        "text": "#13202f",
        "muted": "#4f5d73",
        "grid": "#d9e0ea",
        "accent": "#1976d2",
        "accent2": "#2e7d32",
        "accent3": "#ef6c00",
        "danger": "#c62828",
        "warning": "#ed6c02",
        "info": "#546e7a",
    },
}


@traceable(
    name="output.chart_generator.generate_chart",
    run_type="chain",
    process_outputs=compact_text_output,
)
def generate_chart(
    chart_type: str,
    output_dir: str | Path,
    theme: str = "dark",
    profile_dict: dict[str, Any] | None = None,
    profile_dicts: list[dict[str, Any]] | None = None,
    relationship_data: dict[str, Any] | None = None,
    column_name: str | None = None,
) -> list[dict[str, str]]:
    """Generate a chart and return UI-ready chart descriptors."""
    if chart_type not in AVAILABLE_CHART_TYPES:
        return []

    theme_key = theme if theme in _THEMES else "dark"
    style = _THEMES[theme_key]
    charts_dir, url_prefix = _resolve_public_chart_dir(Path(output_dir))
    charts_dir.mkdir(parents=True, exist_ok=True)

    if chart_type in {"overview_directory", "row_counts", "quality_heatmap"}:
        if not profile_dicts:
            return []
        fig = _render_multi_table_chart(chart_type, profile_dicts, style)
        if fig is None:
            return []
        return [_save_chart(fig, charts_dir, url_prefix, chart_type, "*", None, theme_key, _humanize_chart_type(chart_type))]

    if chart_type == "relationship_confidence":
        fig = _render_relationship_confidence_chart(relationship_data or {}, style)
        if fig is None:
            return []
        return [_save_chart(fig, charts_dir, url_prefix, chart_type, "*", None, theme_key, "Relationship Confidence")]

    if not profile_dict:
        return []

    if chart_type in {"top_values", "string_lengths", "distribution", "column_detail"}:
        if not column_name:
            return []
        column = _find_column(profile_dict, column_name)
        if not column:
            return []
        fig = _render_column_chart(chart_type, profile_dict, column, style)
        if fig is None:
            return []
        return [_save_chart(
            fig,
            charts_dir,
            url_prefix,
            chart_type,
            str(profile_dict.get("table_name", "table")),
            column_name,
            theme_key,
            f"{_humanize_chart_type(chart_type)} - {profile_dict.get('table_name', 'Table')}.{column_name}",
        )]

    fig = _render_single_table_chart(chart_type, profile_dict, style)
    if fig is None:
        return []
    table_name = str(profile_dict.get("table_name", "table"))
    return [_save_chart(
        fig,
        charts_dir,
        url_prefix,
        chart_type,
        table_name,
        None,
        theme_key,
        f"{_humanize_chart_type(chart_type)} - {table_name}",
    )]


def _resolve_public_chart_dir(output_dir: Path) -> tuple[Path, str]:
    """Return a chart directory and a URL prefix served by the web UI."""
    output_dir = output_dir.resolve()
    parts = list(output_dir.parts)
    if "connectors" in parts:
        idx = parts.index("connectors")
        base_dir = Path(*parts[:idx])
        relative = Path(*parts[idx:])
        charts_dir = base_dir / "charts" / relative
        url_prefix = "/" + Path("charts", *relative.parts).as_posix()
        return charts_dir, url_prefix

    charts_dir = output_dir / "charts"
    return charts_dir, "/charts"


def _save_chart(
    fig,
    charts_dir: Path,
    url_prefix: str,
    chart_type: str,
    table_name: str,
    column_name: str | None,
    theme: str,
    title: str,
) -> dict[str, str]:
    slug = _slugify(f"{chart_type}-{table_name}-{column_name or 'all'}-{theme}")
    digest = hashlib.sha1(json.dumps([chart_type, table_name, column_name, theme]).encode("utf-8")).hexdigest()[:10]
    filename = f"{slug}-{digest}.png"
    path = charts_dir / filename
    fig.savefig(path, dpi=160, bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    return {"title": title, "url": f"{url_prefix}/{filename}", "path": str(path)}


def _apply_theme(style: dict[str, Any]) -> None:
    sns.set_theme(style=style["style"])
    plt.rcParams.update({
        "figure.facecolor": style["facecolor"],
        "axes.facecolor": style["axes_facecolor"],
        "axes.edgecolor": style["grid"],
        "axes.labelcolor": style["text"],
        "xtick.color": style["text"],
        "ytick.color": style["text"],
        "text.color": style["text"],
        "grid.color": style["grid"],
    })


def _render_single_table_chart(chart_type: str, profile: dict[str, Any], style: dict[str, Any]):
    _apply_theme(style)
    if chart_type == "overview":
        return _chart_overview(profile, style)
    if chart_type == "data_quality_scorecard":
        return _chart_quality_scorecard(profile, style)
    if chart_type == "null_distribution":
        return _chart_null_distribution(profile, style)
    if chart_type == "type_distribution":
        return _chart_type_distribution(profile, style)
    if chart_type == "cardinality":
        return _chart_cardinality(profile, style)
    if chart_type == "completeness":
        return _chart_completeness(profile, style)
    if chart_type == "numeric_summary":
        return _chart_numeric_summary(profile, style)
    if chart_type == "skewness":
        return _chart_skewness(profile, style)
    if chart_type == "outlier_summary":
        return _chart_outlier_summary(profile, style)
    if chart_type == "correlation_matrix":
        return _chart_correlation_matrix(profile, style)
    return None


def _render_column_chart(chart_type: str, profile: dict[str, Any], column: dict[str, Any], style: dict[str, Any]):
    _apply_theme(style)
    if chart_type == "top_values":
        return _chart_top_values(profile, column, style)
    if chart_type == "string_lengths":
        return _chart_string_lengths(profile, column, style)
    if chart_type == "distribution":
        return _chart_distribution(profile, column, style)
    if chart_type == "column_detail":
        return _chart_column_detail(profile, column, style)
    return None


def _render_multi_table_chart(chart_type: str, profiles: list[dict[str, Any]], style: dict[str, Any]):
    _apply_theme(style)
    if chart_type == "overview_directory":
        return _chart_overview_directory(profiles, style)
    if chart_type == "row_counts":
        return _chart_row_counts(profiles, style)
    if chart_type == "quality_heatmap":
        return _chart_quality_heatmap(profiles, style)
    return None


def _render_relationship_confidence_chart(relationship_data: dict[str, Any], style: dict[str, Any]):
    _apply_theme(style)
    candidates = relationship_data.get("candidates", []) if isinstance(relationship_data, dict) else []
    if not candidates:
        return None

    rows = []
    for candidate in candidates[:20]:
        fk = candidate.get("fk", {})
        pk = candidate.get("pk", {})
        rows.append({
            "label": f"{fk.get('table_name', '?')}.{fk.get('column_name', '?')} -> {pk.get('table_name', '?')}.{pk.get('column_name', '?')}",
            "confidence": float(candidate.get("confidence", 0.0) or 0.0),
        })

    if not rows:
        return None

    df = pd.DataFrame(rows).sort_values("confidence", ascending=True)
    fig, ax = plt.subplots(figsize=(12, max(4, len(df) * 0.45)))
    fig.patch.set_facecolor(style["facecolor"])
    ax.barh(df["label"], df["confidence"], color=style["accent"])
    ax.set_xlim(0, 1)
    ax.set_xlabel("Confidence")
    ax.set_title("Relationship Confidence")
    return fig


def _chart_overview(profile: dict[str, Any], style: dict[str, Any]):
    columns = _columns(profile)
    if not columns:
        return None

    fig, axes = plt.subplots(2, 3, figsize=(16, 10))
    fig.patch.set_facecolor(style["facecolor"])
    fig.suptitle(f"Overview - {profile.get('table_name', 'Table')}", fontsize=16)
    _draw_profile_summary_card(axes[0, 0], profile, style)
    _draw_null_distribution(axes[0, 1], profile, style)
    _draw_type_distribution(axes[0, 2], profile, style)
    _draw_cardinality(axes[1, 0], profile, style)
    _draw_completeness(axes[1, 1], profile, style)
    _draw_numeric_summary(axes[1, 2], profile, style)
    fig.tight_layout()
    return fig


def _chart_quality_scorecard(profile: dict[str, Any], style: dict[str, Any]):
    metrics = _quality_scores(profile)
    labels = list(metrics.keys())
    values = list(metrics.values())
    angles = [n / float(len(labels)) * 2 * math.pi for n in range(len(labels))]
    values += values[:1]
    angles += angles[:1]

    fig = plt.figure(figsize=(8, 8))
    fig.patch.set_facecolor(style["facecolor"])
    ax = fig.add_subplot(111, polar=True)
    ax.set_facecolor(style["axes_facecolor"])
    ax.plot(angles, values, color=style["accent"], linewidth=2)
    ax.fill(angles, values, color=style["accent"], alpha=0.25)
    ax.set_xticks(angles[:-1])
    ax.set_xticklabels(labels)
    ax.set_yticks([0.2, 0.4, 0.6, 0.8, 1.0])
    ax.set_yticklabels(["20", "40", "60", "80", "100"])
    ax.set_title(f"Data Quality Scorecard - {profile.get('table_name', 'Table')}")
    return fig


def _chart_null_distribution(profile: dict[str, Any], style: dict[str, Any]):
    fig, ax = plt.subplots(figsize=(12, 5))
    fig.patch.set_facecolor(style["facecolor"])
    _draw_null_distribution(ax, profile, style)
    fig.tight_layout()
    return fig


def _chart_type_distribution(profile: dict[str, Any], style: dict[str, Any]):
    fig, ax = plt.subplots(figsize=(8, 6))
    fig.patch.set_facecolor(style["facecolor"])
    if not _draw_type_distribution(ax, profile, style):
        plt.close(fig)
        return None
    fig.tight_layout()
    return fig


def _chart_cardinality(profile: dict[str, Any], style: dict[str, Any]):
    fig, ax = plt.subplots(figsize=(12, 5))
    fig.patch.set_facecolor(style["facecolor"])
    _draw_cardinality(ax, profile, style)
    fig.tight_layout()
    return fig


def _chart_completeness(profile: dict[str, Any], style: dict[str, Any]):
    fig, ax = plt.subplots(figsize=(12, 5))
    fig.patch.set_facecolor(style["facecolor"])
    _draw_completeness(ax, profile, style)
    fig.tight_layout()
    return fig


def _chart_numeric_summary(profile: dict[str, Any], style: dict[str, Any]):
    fig, ax = plt.subplots(figsize=(12, 5))
    fig.patch.set_facecolor(style["facecolor"])
    if not _draw_numeric_summary(ax, profile, style):
        plt.close(fig)
        return None
    fig.tight_layout()
    return fig


def _chart_skewness(profile: dict[str, Any], style: dict[str, Any]):
    numeric = [c for c in _numeric_columns(profile) if _finite(c.get("skewness"))]
    if not numeric:
        return None
    fig, ax = plt.subplots(figsize=(12, 5))
    fig.patch.set_facecolor(style["facecolor"])
    data = pd.DataFrame({"column": [c["name"] for c in numeric], "skewness": [float(c.get("skewness")) for c in numeric]})
    palette = [style["danger"] if abs(v) > 1 else style["accent"] for v in data["skewness"]]
    ax.bar(data["column"], data["skewness"], color=palette)
    ax.axhline(0, color=style["muted"], linewidth=1)
    ax.set_title(f"Skewness - {profile.get('table_name', 'Table')}")
    ax.set_ylabel("Skewness")
    ax.tick_params(axis="x", rotation=30)
    fig.tight_layout()
    return fig


def _chart_outlier_summary(profile: dict[str, Any], style: dict[str, Any]):
    numeric = [c for c in _numeric_columns(profile) if _finite(c.get("outlier_count"))]
    if not numeric:
        return None
    row_count = max(int(profile.get("row_count", 0) or 0), 1)
    fig, ax = plt.subplots(figsize=(12, 5))
    fig.patch.set_facecolor(style["facecolor"])
    pct = [((int(c.get("outlier_count") or 0)) / row_count) * 100 for c in numeric]
    colors = [style["danger"] if value > 5 else style["warning"] if value > 0 else style["accent2"] for value in pct]
    ax.bar([c["name"] for c in numeric], pct, color=colors)
    ax.set_title(f"Outlier Summary - {profile.get('table_name', 'Table')}")
    ax.set_ylabel("Outliers (% of rows)")
    ax.tick_params(axis="x", rotation=30)
    fig.tight_layout()
    return fig


def _chart_correlation_matrix(profile: dict[str, Any], style: dict[str, Any]):
    samples = _numeric_sample_frame(profile)
    if samples is None or samples.shape[1] < 2 or samples.shape[0] < 2:
        return None
    corr = samples.corr(numeric_only=True)
    fig, ax = plt.subplots(figsize=(8, 6))
    fig.patch.set_facecolor(style["facecolor"])
    sns.heatmap(corr, annot=True, cmap="coolwarm", center=0, ax=ax, fmt=".2f")
    ax.set_title(f"Correlation Matrix - {profile.get('table_name', 'Table')}")
    fig.tight_layout()
    return fig


def _chart_top_values(profile: dict[str, Any], column: dict[str, Any], style: dict[str, Any]):
    top_values = column.get("top_values") or []
    if not top_values:
        return None
    labels = [str(item.get("value", "")) for item in top_values[:10]]
    counts = [int(item.get("count", 0) or 0) for item in top_values[:10]]
    fig, ax = plt.subplots(figsize=(10, max(4, len(labels) * 0.45)))
    fig.patch.set_facecolor(style["facecolor"])
    ax.barh(labels[::-1], counts[::-1], color=style["accent"])
    ax.set_title(f"Top Values - {profile.get('table_name', 'Table')}.{column.get('name', 'column')}")
    ax.set_xlabel("Count")
    fig.tight_layout()
    return fig


def _chart_string_lengths(profile: dict[str, Any], column: dict[str, Any], style: dict[str, Any]):
    percentiles = {
        "P10": _float_or_none(column.get("length_p10")),
        "P50": _float_or_none(column.get("length_p50")),
        "P90": _float_or_none(column.get("length_p90")),
        "Max": _float_or_none(column.get("length_max")),
    }
    values = {k: v for k, v in percentiles.items() if v is not None}
    if not values:
        return None
    fig, ax = plt.subplots(figsize=(8, 5))
    fig.patch.set_facecolor(style["facecolor"])
    ax.bar(values.keys(), values.values(), color=[style["info"], style["accent"], style["accent2"], style["accent3"]][:len(values)])
    ax.set_title(f"String Lengths - {profile.get('table_name', 'Table')}.{column.get('name', 'column')}")
    ax.set_ylabel("Length")
    fig.tight_layout()
    return fig


def _chart_distribution(profile: dict[str, Any], column: dict[str, Any], style: dict[str, Any]):
    points = {
        "P5": _float_or_none(column.get("p5")),
        "Q1": _float_or_none(column.get("p25")),
        "Median": _float_or_none(column.get("median")),
        "Q3": _float_or_none(column.get("p75")),
        "P95": _float_or_none(column.get("p95")),
    }
    values = {k: v for k, v in points.items() if v is not None}
    if not values:
        return None
    fig, (ax, table_ax) = plt.subplots(2, 1, figsize=(10, 8), gridspec_kw={"height_ratios": [3, 1.6]})
    fig.patch.set_facecolor(style["facecolor"])
    ax.plot(list(values.keys()), list(values.values()), marker="o", color=style["accent"])
    mean = _float_or_none(column.get("mean"))
    if mean is not None:
        ax.axhline(mean, color=style["warning"], linestyle="--", label="Mean")
        ax.legend()
    ax.set_title(f"Distribution - {profile.get('table_name', 'Table')}.{column.get('name', 'column')}")
    stats_rows = [
        ["Std Dev", _fmt_num(column.get("std_dev"))],
        ["Skewness", _fmt_num(column.get("skewness"))],
        ["Kurtosis", _fmt_num(column.get("kurtosis"))],
        ["IQR", _fmt_num(column.get("iqr"))],
        ["CV", _fmt_num(column.get("coefficient_of_variation"))],
        ["Outliers", str(int(column.get("outlier_count") or 0))],
    ]
    table_ax.axis("off")
    table = table_ax.table(cellText=stats_rows, colLabels=["Metric", "Value"], loc="center")
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1, 1.3)
    fig.tight_layout()
    return fig


def _chart_column_detail(profile: dict[str, Any], column: dict[str, Any], style: dict[str, Any]):
    fig, axes = plt.subplots(2, 2, figsize=(14, 9))
    fig.patch.set_facecolor(style["facecolor"])
    fig.suptitle(f"Column Detail - {profile.get('table_name', 'Table')}.{column.get('name', 'column')}", fontsize=15)
    _draw_column_summary_card(axes[0, 0], column, style)
    _draw_top_values(axes[0, 1], column, style)
    _draw_column_percentiles(axes[1, 0], column, style)
    _draw_string_lengths(axes[1, 1], column, style)
    fig.tight_layout()
    return fig


def _chart_overview_directory(profiles: list[dict[str, Any]], style: dict[str, Any]):
    fig, axes = plt.subplots(1, 2, figsize=(15, 6))
    fig.patch.set_facecolor(style["facecolor"])
    _draw_row_counts(axes[0], profiles, style)
    if not _draw_quality_heatmap(axes[1], profiles, style):
        plt.close(fig)
        return None
    fig.tight_layout()
    return fig


def _chart_row_counts(profiles: list[dict[str, Any]], style: dict[str, Any]):
    fig, ax = plt.subplots(figsize=(12, 5))
    fig.patch.set_facecolor(style["facecolor"])
    _draw_row_counts(ax, profiles, style)
    fig.tight_layout()
    return fig


def _chart_quality_heatmap(profiles: list[dict[str, Any]], style: dict[str, Any]):
    fig, ax = plt.subplots(figsize=(12, max(4, len(profiles) * 0.45)))
    fig.patch.set_facecolor(style["facecolor"])
    if not _draw_quality_heatmap(ax, profiles, style):
        plt.close(fig)
        return None
    fig.tight_layout()
    return fig


def _draw_profile_summary_card(ax, profile: dict[str, Any], style: dict[str, Any]) -> None:
    ax.axis("off")
    rows = [
        ("Rows", str(int(profile.get("row_count", 0) or 0))),
        ("Columns", str(len(_columns(profile)))),
        ("Format", str(profile.get("file_format", profile.get("format", "unknown")))),
        ("Issues", str(int((profile.get("quality_summary") or {}).get("columns_with_issues", 0) or 0))),
        ("Null-heavy", str(int((profile.get("quality_summary") or {}).get("null_heavy_columns", 0) or 0))),
        ("Corrupt rows", str(int((profile.get("quality_summary") or {}).get("corrupt_rows_detected", 0) or 0))),
    ]
    table = ax.table(cellText=rows, colLabels=["Metric", "Value"], loc="center")
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1.1, 1.5)
    ax.set_title("Profile Summary")


def _draw_null_distribution(ax, profile: dict[str, Any], style: dict[str, Any]) -> None:
    columns = _columns(profile)
    row_count = max(int(profile.get("row_count", 0) or 0), 1)
    labels = [c["name"] for c in columns]
    values = [round((int(c.get("null_count", 0) or 0) / row_count) * 100, 2) for c in columns]
    colors = [style["danger"] if v >= 50 else style["warning"] if v > 0 else style["accent2"] for v in values]
    ax.bar(labels, values, color=colors)
    ax.set_ylabel("Null %")
    ax.set_title("Null Distribution")
    ax.tick_params(axis="x", rotation=30)


def _draw_type_distribution(ax, profile: dict[str, Any], style: dict[str, Any]) -> bool:
    columns = _columns(profile)
    counts: dict[str, int] = {}
    for column in columns:
        inferred = str(column.get("inferred_type", "UNKNOWN"))
        counts[inferred] = counts.get(inferred, 0) + 1
    if not counts:
        return False
    ax.pie(counts.values(), labels=counts.keys(), autopct="%1.0f%%", startangle=90)
    ax.set_title("Type Distribution")
    return True


def _draw_cardinality(ax, profile: dict[str, Any], style: dict[str, Any]) -> None:
    columns = _columns(profile)
    labels = [c["name"] for c in columns]
    values = [int(c.get("distinct_count", 0) or 0) for c in columns]
    colors = [style["accent2"] if c.get("is_key_candidate") else style["accent"] for c in columns]
    ax.bar(labels, values, color=colors)
    ax.set_title("Cardinality")
    ax.set_ylabel("Distinct Count")
    ax.tick_params(axis="x", rotation=30)
    ax.legend(handles=[Patch(color=style["accent2"], label="Key candidate"), Patch(color=style["accent"], label="Other")], loc="upper right")


def _draw_completeness(ax, profile: dict[str, Any], style: dict[str, Any]) -> None:
    columns = _columns(profile)
    row_count = max(int(profile.get("row_count", 0) or 0), 1)
    labels = [c["name"] for c in columns]
    nulls = [int(c.get("null_count", 0) or 0) for c in columns]
    filled = [max(row_count - value, 0) for value in nulls]
    ax.bar(labels, filled, color=style["accent2"], label="Filled")
    ax.bar(labels, nulls, bottom=filled, color=style["danger"], label="Null")
    ax.set_title("Completeness")
    ax.set_ylabel("Rows")
    ax.tick_params(axis="x", rotation=30)
    ax.legend()


def _draw_numeric_summary(ax, profile: dict[str, Any], style: dict[str, Any]) -> bool:
    numeric = _numeric_columns(profile)
    if not numeric:
        ax.axis("off")
        ax.text(0.5, 0.5, "No numeric columns", ha="center", va="center")
        ax.set_title("Numeric Summary")
        return False
    df = pd.DataFrame({
        "column": [c["name"] for c in numeric],
        "mean": [float(c.get("mean") or 0.0) for c in numeric],
        "median": [float(c.get("median") or 0.0) for c in numeric],
        "std_dev": [float(c.get("std_dev") or 0.0) for c in numeric],
    })
    melted = df.melt(id_vars="column", value_vars=["mean", "median", "std_dev"], var_name="metric", value_name="value")
    sns.barplot(data=melted, x="column", y="value", hue="metric", ax=ax, palette=[style["accent"], style["accent2"], style["accent3"]])
    ax.set_title("Numeric Summary")
    ax.tick_params(axis="x", rotation=30)
    return True


def _draw_row_counts(ax, profiles: list[dict[str, Any]], style: dict[str, Any]) -> None:
    rows = sorted(
        [{"table_name": p.get("table_name", "?"), "row_count": int(p.get("row_count", 0) or 0)} for p in profiles],
        key=lambda item: item["row_count"],
        reverse=True,
    )
    ax.bar([r["table_name"] for r in rows], [r["row_count"] for r in rows], color=style["accent"])
    ax.set_title("Row Counts")
    ax.tick_params(axis="x", rotation=35)


def _draw_quality_heatmap(ax, profiles: list[dict[str, Any]], style: dict[str, Any]) -> bool:
    rows = []
    for profile in profiles:
        summary = profile.get("quality_summary") or {}
        rows.append({
            "table": profile.get("table_name", "?"),
            "issues": int(summary.get("columns_with_issues", 0) or 0),
            "null_heavy": int(summary.get("null_heavy_columns", 0) or 0),
            "type_conflict": int(summary.get("type_conflict_columns", 0) or 0),
            "corrupt_rows": int(summary.get("corrupt_rows_detected", 0) or 0),
        })
    if not rows:
        return False
    df = pd.DataFrame(rows).set_index("table")
    sns.heatmap(df, annot=True, cmap="YlOrRd", cbar=True, ax=ax, fmt="g")
    ax.set_title("Quality Heatmap")
    return True


def _draw_column_summary_card(ax, column: dict[str, Any], style: dict[str, Any]) -> None:
    ax.axis("off")
    rows = [
        ("Type", str(column.get("inferred_type", "UNKNOWN"))),
        ("Distinct", str(int(column.get("distinct_count", 0) or 0))),
        ("Null Count", str(int(column.get("null_count", 0) or 0))),
        ("Mean", _fmt_num(column.get("mean"))),
        ("Median", _fmt_num(column.get("median"))),
        ("Std Dev", _fmt_num(column.get("std_dev"))),
    ]
    table = ax.table(cellText=rows, colLabels=["Metric", "Value"], loc="center")
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1.1, 1.4)
    ax.set_title("Column Summary")


def _draw_top_values(ax, column: dict[str, Any], style: dict[str, Any]) -> None:
    top_values = column.get("top_values") or []
    if not top_values:
        ax.axis("off")
        ax.text(0.5, 0.5, "No top values", ha="center", va="center")
        ax.set_title("Top Values")
        return
    labels = [str(item.get("value", "")) for item in top_values[:8]]
    counts = [int(item.get("count", 0) or 0) for item in top_values[:8]]
    ax.barh(labels[::-1], counts[::-1], color=style["accent"])
    ax.set_title("Top Values")


def _draw_column_percentiles(ax, column: dict[str, Any], style: dict[str, Any]) -> None:
    values = {
        "P5": _float_or_none(column.get("p5")),
        "Q1": _float_or_none(column.get("p25")),
        "Median": _float_or_none(column.get("median")),
        "Q3": _float_or_none(column.get("p75")),
        "P95": _float_or_none(column.get("p95")),
    }
    filtered = {k: v for k, v in values.items() if v is not None}
    if not filtered:
        ax.axis("off")
        ax.text(0.5, 0.5, "No percentile data", ha="center", va="center")
        ax.set_title("Percentiles")
        return
    ax.plot(list(filtered.keys()), list(filtered.values()), marker="o", color=style["accent3"])
    ax.set_title("Percentiles")


def _draw_string_lengths(ax, column: dict[str, Any], style: dict[str, Any]) -> None:
    values = {
        "P10": _float_or_none(column.get("length_p10")),
        "P50": _float_or_none(column.get("length_p50")),
        "P90": _float_or_none(column.get("length_p90")),
    }
    filtered = {k: v for k, v in values.items() if v is not None}
    if not filtered:
        ax.axis("off")
        ax.text(0.5, 0.5, "No string length data", ha="center", va="center")
        ax.set_title("String Lengths")
        return
    ax.bar(filtered.keys(), filtered.values(), color=style["info"])
    ax.set_title("String Lengths")


def _quality_scores(profile: dict[str, Any]) -> dict[str, float]:
    columns = _columns(profile)
    total_columns = max(len(columns), 1)
    row_count = max(int(profile.get("row_count", 0) or 0), 1)
    summary = profile.get("quality_summary") or {}
    null_total = sum(int(c.get("null_count", 0) or 0) for c in columns)
    issue_columns = int(summary.get("columns_with_issues", 0) or 0)
    distinct_keys = sum(1 for c in columns if c.get("is_key_candidate"))
    confidence = [_float_or_none(c.get("confidence_score")) for c in columns]
    confidence = [v for v in confidence if v is not None]
    outliers = [int(c.get("outlier_count", 0) or 0) for c in _numeric_columns(profile)]
    return {
        "Completeness": max(0.0, min(1.0, 1 - (null_total / (row_count * total_columns)))),
        "Consistency": max(0.0, min(1.0, 1 - (issue_columns / total_columns))),
        "Type Conf.": max(0.0, min(1.0, (sum(confidence) / len(confidence)) if confidence else 0.0)),
        "Uniqueness": max(0.0, min(1.0, distinct_keys / total_columns)),
        "Schema": 1.0 if not profile.get("structural_issues") else 0.5,
        "Outlier Hlth": max(0.0, min(1.0, 1 - ((sum(outliers) / row_count) if outliers else 0.0))),
    }


def _numeric_columns(profile: dict[str, Any]) -> list[dict[str, Any]]:
    result = []
    for column in _columns(profile):
        inferred = str(column.get("inferred_type", ""))
        if inferred in {"INTEGER", "FLOAT"} or _float_or_none(column.get("mean")) is not None:
            result.append(column)
    return result


def _numeric_sample_frame(profile: dict[str, Any]) -> pd.DataFrame | None:
    series: dict[str, list[float]] = {}
    for column in _numeric_columns(profile):
        numeric_values = []
        for value in column.get("sample_values") or []:
            parsed = _parse_float(value)
            if parsed is not None:
                numeric_values.append(parsed)
        if len(numeric_values) >= 2:
            series[str(column.get("name", "column"))] = numeric_values
    if len(series) < 2:
        return None
    min_len = min(len(values) for values in series.values())
    if min_len < 2:
        return None
    trimmed = {key: values[:min_len] for key, values in series.items()}
    return pd.DataFrame(trimmed)


def _find_column(profile: dict[str, Any], column_name: str) -> dict[str, Any] | None:
    target = column_name.strip().lower()
    for column in _columns(profile):
        if str(column.get("name", "")).strip().lower() == target:
            return column
    return None


def _columns(profile: dict[str, Any]) -> list[dict[str, Any]]:
    columns = profile.get("columns") or []
    return [c for c in columns if isinstance(c, dict)]


def _humanize_chart_type(chart_type: str) -> str:
    return chart_type.replace("_", " ").title()


def _slugify(value: str) -> str:
    slug = re.sub(r"[^A-Za-z0-9]+", "-", value.strip().lower()).strip("-")
    return slug or "chart"


def _float_or_none(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _finite(value: Any) -> bool:
    numeric = _float_or_none(value)
    return numeric is not None and math.isfinite(numeric)


def _fmt_num(value: Any) -> str:
    numeric = _float_or_none(value)
    if numeric is None:
        return "-"
    return f"{numeric:.3f}" if abs(numeric) < 1000 else f"{numeric:,.0f}"


def _parse_float(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip().replace(",", "")
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None
