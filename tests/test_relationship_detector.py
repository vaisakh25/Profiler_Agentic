"""
Tests for the cross-table relationship detector.

Covers:
  TestPkEligibility       — _is_pk_eligible criteria
  TestFkEligibility       — _is_fk_eligible criteria
  TestNameScoring         — _name_score patterns
  TestTypeScoring         — _type_score compatibility
  TestCardinalityScoring  — _cardinality_score signals
  TestValueOverlap        — _value_overlap computation
  TestOverlapScore        — _overlap_score_from_pct thresholds
  TestConfidenceScoring   — known FK pair confidence; unrelated pair confidence
  TestEdgeCases           — single table, all-null, zero tables
  TestReport              — sort order, tables_analyzed, columns_analyzed
  TestRelationshipWriter  — JSON written; parent dirs created; overwrites
  TestMainIntegration     — analyze_relationships() entry point
"""

from __future__ import annotations

import json
import textwrap
from dataclasses import field
from pathlib import Path
from typing import Optional

import pytest

from file_profiler.analysis.relationship_detector import (
    MIN_CONFIDENCE,
    _cardinality_score,
    _is_fk_eligible,
    _is_pk_eligible,
    _name_score,
    _overlap_score_from_pct,
    _type_score,
    _value_overlap,
    detect,
)
from file_profiler.main import analyze_relationships
from file_profiler.models.enums import (
    Cardinality,
    FileFormat,
    InferredType,
    QualityFlag,
    SizeStrategy,
)
from file_profiler.models.file_profile import ColumnProfile, FileProfile, TopValue
from file_profiler.models.relationships import (
    ColumnRef,
    ForeignKeyCandidate,
    RelationshipReport,
)
from file_profiler.output.relationship_writer import write as write_report


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _col(
    name: str,
    inferred_type: InferredType = InferredType.INTEGER,
    null_count: int = 0,
    distinct_count: int = 100,
    unique_ratio: float = 1.0,
    is_key_candidate: bool = False,
    quality_flags: Optional[list[QualityFlag]] = None,
    top_values: Optional[list[TopValue]] = None,
) -> ColumnProfile:
    return ColumnProfile(
        name=name,
        declared_type=None,
        inferred_type=inferred_type,
        confidence_score=0.99,
        null_count=null_count,
        distinct_count=distinct_count,
        unique_ratio=unique_ratio,
        is_key_candidate=is_key_candidate,
        quality_flags=quality_flags or [],
        top_values=top_values or [],
    )


def _fp(table_name: str, columns: list[ColumnProfile], row_count: int = 100) -> FileProfile:
    return FileProfile(
        source_type="file",
        file_format=FileFormat.CSV,
        file_path=f"/data/{table_name}.csv",
        table_name=table_name,
        row_count=row_count,
        is_row_count_exact=True,
        columns=columns,
    )


def _tv(*values: str) -> list[TopValue]:
    return [TopValue(value=v, count=1) for v in values]


# ---------------------------------------------------------------------------
# TestPkEligibility
# ---------------------------------------------------------------------------

class TestPkEligibility:

    def test_is_key_candidate_makes_pk_eligible(self):
        col = _col("id", is_key_candidate=True, unique_ratio=0.0, null_count=10)
        assert _is_pk_eligible(col)

    def test_high_unique_ratio_and_no_nulls_is_eligible(self):
        col = _col("id", unique_ratio=0.95, null_count=0)
        assert _is_pk_eligible(col)

    def test_unique_ratio_below_threshold_not_eligible(self):
        col = _col("id", unique_ratio=0.80, null_count=0, is_key_candidate=False)
        assert not _is_pk_eligible(col)

    def test_nulls_present_disqualifies_high_unique(self):
        col = _col("id", unique_ratio=0.99, null_count=1, is_key_candidate=False)
        assert not _is_pk_eligible(col)

    def test_fully_null_flag_disqualifies(self):
        col = _col("id", is_key_candidate=True, quality_flags=[QualityFlag.FULLY_NULL])
        assert not _is_pk_eligible(col)

    def test_structural_corruption_disqualifies(self):
        col = _col("id", is_key_candidate=True, quality_flags=[QualityFlag.STRUCTURAL_CORRUPTION])
        assert not _is_pk_eligible(col)

    def test_free_text_type_disqualifies(self):
        col = _col("notes", inferred_type=InferredType.FREE_TEXT, is_key_candidate=True)
        assert not _is_pk_eligible(col)

    def test_boolean_type_disqualifies(self):
        col = _col("active", inferred_type=InferredType.BOOLEAN, is_key_candidate=True)
        assert not _is_pk_eligible(col)

    def test_null_only_type_disqualifies(self):
        col = _col("x", inferred_type=InferredType.NULL_ONLY, is_key_candidate=True)
        assert not _is_pk_eligible(col)


# ---------------------------------------------------------------------------
# TestFkEligibility
# ---------------------------------------------------------------------------

class TestFkEligibility:

    def test_normal_integer_col_is_eligible(self):
        col = _col("customer_id", unique_ratio=0.5, is_key_candidate=False)
        assert _is_fk_eligible(col)

    def test_key_candidate_is_not_fk_eligible(self):
        # PK-like columns should not be treated as FKs
        col = _col("id", is_key_candidate=True)
        assert not _is_fk_eligible(col)

    def test_fully_null_disqualifies_fk(self):
        col = _col("x", quality_flags=[QualityFlag.FULLY_NULL])
        assert not _is_fk_eligible(col)

    def test_boolean_disqualifies_fk(self):
        col = _col("flag", inferred_type=InferredType.BOOLEAN)
        assert not _is_fk_eligible(col)

    def test_free_text_disqualifies_fk(self):
        col = _col("description", inferred_type=InferredType.FREE_TEXT)
        assert not _is_fk_eligible(col)

    def test_string_col_is_eligible(self):
        col = _col("code", inferred_type=InferredType.STRING, unique_ratio=0.3)
        assert _is_fk_eligible(col)


# ---------------------------------------------------------------------------
# TestNameScoring
# ---------------------------------------------------------------------------

class TestNameScoring:

    def test_direct_prefix(self):
        score, ev = _name_score("customers_id", "id", "customers")
        assert score == 0.50
        assert ev == "name:direct_prefix"

    def test_singular_prefix(self):
        score, ev = _name_score("customer_id", "id", "customers")
        assert score == 0.45
        assert ev == "name:singular_prefix"

    def test_exact_match(self):
        score, ev = _name_score("customer_id", "customer_id", "orders")
        assert score == 0.40
        assert ev == "name:exact"

    def test_embedded(self):
        score, ev = _name_score("ref_customer_id", "id", "customers")
        assert score == 0.35
        assert ev == "name:embedded"

    def test_no_match_returns_zero(self):
        score, ev = _name_score("amount", "id", "customers")
        assert score == 0.0
        assert ev == ""

    def test_short_table_name_guard_blocks_prefix(self):
        # pk_table.rstrip('s') = "ab" which is < 3 chars
        score, ev = _name_score("abs_id", "id", "abs")
        assert score == 0.0

    def test_short_table_name_guard_allows_exact(self):
        # Even with a short table, exact column name match still scores
        score, ev = _name_score("id", "id", "ab")
        assert score == 0.40
        assert ev == "name:exact"

    def test_direct_prefix_takes_priority_over_singular(self):
        # pk_table "orders" → direct is "orders_id", singular is "order_id"
        # "orders_id" should get direct_prefix (0.50) not singular_prefix
        score, ev = _name_score("orders_id", "id", "orders")
        assert score == 0.50
        assert ev == "name:direct_prefix"


# ---------------------------------------------------------------------------
# TestTypeScoring
# ---------------------------------------------------------------------------

class TestTypeScoring:

    def test_exact_match(self):
        score, ev = _type_score(InferredType.INTEGER, InferredType.INTEGER)
        assert score == 0.20
        assert ev == "type:exact"

    def test_numeric_compat(self):
        score, ev = _type_score(InferredType.FLOAT, InferredType.INTEGER)
        assert score == 0.10
        assert ev == "type:numeric_compat"

    def test_string_compat_string_int(self):
        score, ev = _type_score(InferredType.STRING, InferredType.INTEGER)
        assert score == 0.05
        assert ev == "type:string_compat"

    def test_string_compat_int_uuid(self):
        score, ev = _type_score(InferredType.INTEGER, InferredType.UUID)
        assert score == 0.05
        assert ev == "type:string_compat"

    def test_incompatible_returns_zero(self):
        score, ev = _type_score(InferredType.DATE, InferredType.BOOLEAN)
        assert score == 0.0
        assert ev == ""

    def test_boolean_not_compatible_with_integer(self):
        score, ev = _type_score(InferredType.BOOLEAN, InferredType.INTEGER)
        assert score == 0.0


# ---------------------------------------------------------------------------
# TestCardinalityScoring
# ---------------------------------------------------------------------------

class TestCardinalityScoring:

    def test_is_key_candidate_pk(self):
        pk = _col("id", is_key_candidate=True, distinct_count=100)
        fk = _col("customer_id", distinct_count=80)
        score, ev = _cardinality_score(fk, pk)
        assert score >= 0.20   # at minimum: pk:key_candidate (0.20); fk_subset may add 0.05
        assert "pk:key_candidate" in ev

    def test_high_unique_ratio_pk(self):
        pk = _col("id", is_key_candidate=False, unique_ratio=0.97, null_count=0, distinct_count=100)
        fk = _col("customer_id", distinct_count=80)
        score, ev = _cardinality_score(fk, pk)
        assert score >= 0.15
        assert "pk:high_unique" in ev

    def test_fk_subset_signal(self):
        pk = _col("id", is_key_candidate=True, distinct_count=100)
        fk = _col("customer_id", distinct_count=50)
        score, ev = _cardinality_score(fk, pk)
        assert "cardinality:fk_subset" in ev

    def test_fk_equal_to_pk_distinct_is_subset(self):
        pk = _col("id", is_key_candidate=True, distinct_count=100)
        fk = _col("customer_id", distinct_count=100)
        _, ev = _cardinality_score(fk, pk)
        assert "cardinality:fk_subset" in ev

    def test_fk_exceeds_pk_no_subset(self):
        pk = _col("id", is_key_candidate=True, distinct_count=50)
        fk = _col("customer_id", distinct_count=200)
        _, ev = _cardinality_score(fk, pk)
        assert "cardinality:fk_subset" not in ev

    def test_key_candidate_dominates_high_unique(self):
        # Both is_key_candidate and high unique_ratio are true — only key_candidate scores
        pk = _col("id", is_key_candidate=True, unique_ratio=0.99, null_count=0, distinct_count=100)
        fk = _col("customer_id", distinct_count=80)
        score, ev = _cardinality_score(fk, pk)
        assert "pk:key_candidate" in ev
        assert "pk:high_unique" not in ev


# ---------------------------------------------------------------------------
# TestValueOverlap
# ---------------------------------------------------------------------------

class TestValueOverlap:

    def test_full_overlap(self):
        fk = _tv("1", "2", "3")
        pk = _tv("1", "2", "3", "4", "5")
        result = _value_overlap(fk, pk)
        assert result == 1.0

    def test_partial_overlap(self):
        fk = _tv("1", "2", "3", "4")
        pk = _tv("1", "2", "5", "6")
        result = _value_overlap(fk, pk)
        assert result == 0.5

    def test_no_overlap(self):
        fk = _tv("A", "B")
        pk = _tv("X", "Y")
        result = _value_overlap(fk, pk)
        assert result == 0.0

    def test_empty_fk_returns_none(self):
        assert _value_overlap([], _tv("1", "2")) is None

    def test_empty_pk_returns_none(self):
        assert _value_overlap(_tv("1", "2"), []) is None

    def test_both_empty_returns_none(self):
        assert _value_overlap([], []) is None


# ---------------------------------------------------------------------------
# TestOverlapScore
# ---------------------------------------------------------------------------

class TestOverlapScore:

    def test_high_overlap(self):
        score, ev = _overlap_score_from_pct(0.80)
        assert score == 0.15
        assert ev == "overlap:high"

    def test_above_high_threshold(self):
        score, ev = _overlap_score_from_pct(1.0)
        assert score == 0.15

    def test_medium_overlap(self):
        score, ev = _overlap_score_from_pct(0.60)
        assert score == 0.10
        assert ev == "overlap:medium"

    def test_exactly_at_medium_threshold(self):
        score, ev = _overlap_score_from_pct(0.50)
        assert score == 0.10

    def test_below_medium_threshold(self):
        score, ev = _overlap_score_from_pct(0.49)
        assert score == 0.0

    def test_none_returns_zero(self):
        score, ev = _overlap_score_from_pct(None)
        assert score == 0.0
        assert ev == ""


# ---------------------------------------------------------------------------
# TestConfidenceScoring
# ---------------------------------------------------------------------------

class TestConfidenceScoring:

    def _make_profiles(self) -> list[FileProfile]:
        """
        customers: id (PK), name (STRING)
        orders:    order_id (PK), customer_id (FK → customers.id), total (FLOAT)
        """
        customers_id = _col(
            "id",
            inferred_type=InferredType.INTEGER,
            null_count=0,
            distinct_count=100,
            unique_ratio=1.0,
            is_key_candidate=True,
            top_values=_tv("1", "2", "3", "4", "5", "6", "7", "8", "9", "10"),
        )
        customers_name = _col(
            "name",
            inferred_type=InferredType.STRING,
            distinct_count=100,
            unique_ratio=1.0,
        )
        customers = _fp("customers", [customers_id, customers_name], row_count=100)

        orders_id = _col(
            "order_id",
            inferred_type=InferredType.INTEGER,
            null_count=0,
            distinct_count=50,
            unique_ratio=1.0,
            is_key_candidate=True,
        )
        customer_fk = _col(
            "customer_id",
            inferred_type=InferredType.INTEGER,
            null_count=0,
            distinct_count=80,
            unique_ratio=0.80,
            is_key_candidate=False,
            top_values=_tv("1", "2", "3", "4", "5", "6", "7", "8", "9", "10"),
        )
        # Use DATE type and large distinct_count so type+cardinality signals are too weak
        orders_total = _col(
            "total",
            inferred_type=InferredType.DATE,  # incompatible with INTEGER
            distinct_count=200,               # > pk distinct_count → no fk_subset signal
        )
        orders = _fp("orders", [orders_id, customer_fk, orders_total], row_count=100)

        return [customers, orders]

    def test_known_fk_pair_has_high_confidence(self):
        profiles = self._make_profiles()
        report = detect(profiles)
        # Find orders.customer_id -> customers.id
        match = next(
            (c for c in report.candidates
             if c.fk.table_name == "orders"
             and c.fk.column_name == "customer_id"
             and c.pk.table_name == "customers"
             and c.pk.column_name == "id"),
            None,
        )
        assert match is not None
        assert match.confidence >= 0.70

    def test_known_fk_evidence_contains_name_signal(self):
        profiles = self._make_profiles()
        report = detect(profiles)
        match = next(
            c for c in report.candidates
            if c.fk.column_name == "customer_id" and c.pk.column_name == "id"
        )
        assert any(e.startswith("name:") for e in match.evidence)

    def test_unrelated_columns_below_threshold(self):
        profiles = self._make_profiles()
        report = detect(profiles)
        # orders.total -> customers.id should not appear (name mismatch + type mismatch)
        match = next(
            (c for c in report.candidates
             if c.fk.column_name == "total" and c.pk.column_name == "id"),
            None,
        )
        assert match is None


# ---------------------------------------------------------------------------
# TestEdgeCases
# ---------------------------------------------------------------------------

class TestEdgeCases:

    def test_single_table_returns_empty_candidates(self):
        col = _col("id", is_key_candidate=True)
        profiles = [_fp("only_table", [col])]
        report = detect(profiles)
        assert report.candidates == []
        assert report.tables_analyzed == 1

    def test_zero_tables_returns_empty_candidates(self):
        report = detect([])
        assert report.candidates == []
        assert report.tables_analyzed == 0
        assert report.columns_analyzed == 0

    def test_all_columns_fully_null_returns_empty(self):
        col_a = _col("id", is_key_candidate=True, quality_flags=[QualityFlag.FULLY_NULL])
        col_b = _col("ref_id", quality_flags=[QualityFlag.FULLY_NULL])
        profiles = [
            _fp("table_a", [col_a]),
            _fp("table_b", [col_b]),
        ]
        report = detect(profiles)
        assert report.candidates == []

    def test_no_pk_candidates_returns_empty(self):
        # Both cols have low unique_ratio and is_key_candidate=False
        col_a = _col("col1", unique_ratio=0.5, is_key_candidate=False, null_count=5)
        col_b = _col("col2", unique_ratio=0.3, is_key_candidate=False, null_count=5)
        profiles = [
            _fp("table_a", [col_a]),
            _fp("table_b", [col_b]),
        ]
        report = detect(profiles)
        assert report.candidates == []

    def test_self_join_excluded(self):
        # A table should never be matched to itself
        id_col = _col("id", is_key_candidate=True, distinct_count=100, null_count=0)
        fk_col = _col("parent_id", unique_ratio=0.5, is_key_candidate=False, distinct_count=80)
        profiles = [_fp("categories", [id_col, fk_col])]
        report = detect(profiles)
        assert report.candidates == []

    def test_confidence_capped_at_one(self):
        # Perfect name match + exact type + key_candidate PK + fk_subset + high overlap
        pk_col = _col(
            "id",
            is_key_candidate=True,
            null_count=0,
            distinct_count=100,
            unique_ratio=1.0,
            top_values=_tv("1", "2", "3", "4", "5"),
        )
        fk_col = _col(
            "customers_id",          # direct_prefix match: 0.50
            inferred_type=InferredType.INTEGER,  # exact type: 0.20
            is_key_candidate=False,
            distinct_count=80,
            top_values=_tv("1", "2", "3", "4", "5"),  # high overlap: 0.15
        )
        profiles = [_fp("customers", [pk_col]), _fp("orders", [fk_col])]
        report = detect(profiles)
        for c in report.candidates:
            assert c.confidence <= 1.0


# ---------------------------------------------------------------------------
# TestReport
# ---------------------------------------------------------------------------

class TestReport:

    def _three_table_profiles(self) -> list[FileProfile]:
        pk1 = _col("id", is_key_candidate=True, null_count=0, distinct_count=100)
        pk2 = _col("id", is_key_candidate=True, null_count=0, distinct_count=50)
        fk1 = _col("table_a_id", unique_ratio=0.5, is_key_candidate=False, distinct_count=80)
        fk2 = _col("table_b_id", unique_ratio=0.4, is_key_candidate=False, distinct_count=30)
        t_a = _fp("table_a", [pk1], row_count=100)
        t_b = _fp("table_b", [pk2], row_count=50)
        t_c = _fp("table_c", [fk1, fk2], row_count=100)
        return [t_a, t_b, t_c]

    def test_candidates_sorted_by_confidence_descending(self):
        profiles = self._three_table_profiles()
        report = detect(profiles)
        confidences = [c.confidence for c in report.candidates]
        assert confidences == sorted(confidences, reverse=True)

    def test_tables_analyzed_count(self):
        profiles = self._three_table_profiles()
        report = detect(profiles)
        assert report.tables_analyzed == 3

    def test_columns_analyzed_count(self):
        profiles = self._three_table_profiles()
        report = detect(profiles)
        # table_a: 1, table_b: 1, table_c: 2 = 4 total
        assert report.columns_analyzed == 4

    def test_report_is_relationship_report_instance(self):
        report = detect(self._three_table_profiles())
        assert isinstance(report, RelationshipReport)

    def test_fk_null_ratio_computed(self):
        pk = _col("id", is_key_candidate=True, null_count=0, distinct_count=100)
        fk = _col("table_a_id", null_count=5, distinct_count=80, unique_ratio=0.8, is_key_candidate=False)
        profiles = [_fp("table_a", [pk], row_count=100), _fp("table_b", [fk], row_count=100)]
        report = detect(profiles)
        if report.candidates:
            cand = report.candidates[0]
            assert cand.fk_null_ratio == pytest.approx(0.05)


# ---------------------------------------------------------------------------
# TestRelationshipWriter
# ---------------------------------------------------------------------------

class TestRelationshipWriter:

    def _sample_report(self) -> RelationshipReport:
        return RelationshipReport(
            tables_analyzed=2,
            columns_analyzed=4,
            candidates=[
                ForeignKeyCandidate(
                    fk=ColumnRef("orders", "customer_id"),
                    pk=ColumnRef("customers", "id"),
                    confidence=0.85,
                    evidence=["name:singular_prefix", "type:exact", "pk:key_candidate"],
                    fk_null_ratio=0.0,
                    fk_distinct_count=80,
                    pk_distinct_count=100,
                    top_value_overlap_pct=0.90,
                )
            ],
        )

    def test_writes_valid_json(self, tmp_path):
        out = tmp_path / "relationships.json"
        write_report(self._sample_report(), out)
        data = json.loads(out.read_text(encoding="utf-8"))
        assert isinstance(data, dict)

    def test_json_has_required_top_level_keys(self, tmp_path):
        out = tmp_path / "relationships.json"
        write_report(self._sample_report(), out)
        data = json.loads(out.read_text(encoding="utf-8"))
        assert "tables_analyzed" in data
        assert "columns_analyzed" in data
        assert "candidates" in data

    def test_candidate_structure_correct(self, tmp_path):
        out = tmp_path / "relationships.json"
        write_report(self._sample_report(), out)
        data = json.loads(out.read_text(encoding="utf-8"))
        cand = data["candidates"][0]
        assert cand["fk"]["table_name"] == "orders"
        assert cand["fk"]["column_name"] == "customer_id"
        assert cand["pk"]["table_name"] == "customers"
        assert cand["pk"]["column_name"] == "id"
        assert cand["confidence"] == pytest.approx(0.85)

    def test_creates_parent_directories(self, tmp_path):
        out = tmp_path / "subdir" / "deep" / "relationships.json"
        write_report(self._sample_report(), out)
        assert out.exists()

    def test_overwrites_existing_file(self, tmp_path):
        out = tmp_path / "relationships.json"
        out.write_text("{}", encoding="utf-8")
        write_report(self._sample_report(), out)
        data = json.loads(out.read_text(encoding="utf-8"))
        assert data["tables_analyzed"] == 2

    def test_empty_candidates_written(self, tmp_path):
        out = tmp_path / "empty_rel.json"
        report = RelationshipReport(tables_analyzed=1, columns_analyzed=3, candidates=[])
        write_report(report, out)
        data = json.loads(out.read_text(encoding="utf-8"))
        assert data["candidates"] == []


# ---------------------------------------------------------------------------
# TestMainIntegration
# ---------------------------------------------------------------------------

class TestMainIntegration:

    def _write_csv(self, path: Path, content: str) -> None:
        path.write_text(textwrap.dedent(content).lstrip(), encoding="utf-8")

    def test_returns_relationship_report(self, tmp_path):
        from file_profiler.main import profile_directory
        c = tmp_path / "customers.csv"
        o = tmp_path / "orders.csv"
        self._write_csv(c, """
            id,name
            1,Alice
            2,Bob
            3,Carol
        """)
        self._write_csv(o, """
            order_id,customer_id,amount
            101,1,50.00
            102,2,75.00
            103,1,30.00
        """)
        profiles = profile_directory(tmp_path)
        report = analyze_relationships(profiles)
        assert isinstance(report, RelationshipReport)

    def test_finds_known_fk_in_multi_table_dir(self, tmp_path):
        from file_profiler.main import profile_directory
        c = tmp_path / "customers.csv"
        o = tmp_path / "orders.csv"
        self._write_csv(c, "\n".join(
            ["id,name"] + [f"{i},Customer{i}" for i in range(1, 101)]
        ))
        self._write_csv(o, "\n".join(
            ["order_id,customer_id,amount"] +
            [f"{i},{(i % 100) + 1},{i * 10.5}" for i in range(1, 201)]
        ))
        profiles = profile_directory(tmp_path)
        report = analyze_relationships(profiles)
        match = next(
            (c for c in report.candidates
             if c.fk.column_name == "customer_id"
             and c.pk.column_name == "id"),
            None,
        )
        assert match is not None
        assert match.confidence >= MIN_CONFIDENCE

    def test_writes_json_when_output_path_given(self, tmp_path):
        from file_profiler.main import profile_directory
        c = tmp_path / "customers.csv"
        o = tmp_path / "orders.csv"
        self._write_csv(c, "\n".join(
            ["id,name"] + [f"{i},Name{i}" for i in range(1, 51)]
        ))
        self._write_csv(o, "\n".join(
            ["order_id,customer_id"] + [f"{i},{(i % 50) + 1}" for i in range(1, 101)]
        ))
        profiles = profile_directory(tmp_path)
        out = tmp_path / "rels.json"
        analyze_relationships(profiles, output_path=out)
        assert out.exists()
        data = json.loads(out.read_text(encoding="utf-8"))
        assert "candidates" in data

    def test_no_output_when_path_is_none(self, tmp_path):
        from file_profiler.main import profile_directory
        c = tmp_path / "customers.csv"
        self._write_csv(c, "id,name\n1,Alice\n2,Bob\n")
        profiles = profile_directory(tmp_path)
        # Should not raise, should return a report with no candidates (single table)
        report = analyze_relationships(profiles, output_path=None)
        assert isinstance(report, RelationshipReport)
        assert len(list(tmp_path.glob("*.json"))) == 0
