"""E2E test for the enrichment pipeline — standalone (no MCP server needed).

Runs: profile → detect relationships → build documents → embed → LLM enrich.

Usage:
  conda activate gen_ai
  python tests/test_enrichment_e2e.py
"""

import asyncio
import os
import sys
import shutil

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), ".env"))


async def test_enrichment():
    from pathlib import Path

    from file_profiler.main import profile_directory, analyze_relationships
    from file_profiler.agent.enrichment import (
        build_documents,
        create_vector_store,
        extract_sample_rows,
        enrich,
    )

    project_root = Path(__file__).resolve().parent.parent
    data_dir = project_root / "data" / "files"
    output_dir = project_root / "data" / "output"
    test_dir = project_root / "data" / "test_enrich"

    # Create a small test directory with 3 files
    test_dir.mkdir(exist_ok=True)
    for fname in ["person.parquet", "visit_occurrence.parquet", "condition_occurrence.parquet"]:
        src = data_dir / fname
        if src.exists():
            shutil.copy2(src, test_dir / fname)

    try:
        # Step 1: Profile
        print("\n[1/5] Profiling files...")
        profiles = profile_directory(str(test_dir), output_dir=str(output_dir))
        print(f"      Profiled {len(profiles)} files")
        for p in profiles:
            print(f"      - {p.table_name}: {p.row_count} rows, {len(p.columns)} cols")

        # Step 2: Detect relationships
        print("\n[2/5] Detecting relationships...")
        report = analyze_relationships(
            profiles,
            output_path=str(output_dir / "test_relationships.json"),
            er_diagram_path=str(output_dir / "test_er_diagram.md"),
        )
        print(f"      Found {len(report.candidates)} FK candidates")
        for c in report.candidates[:5]:
            print(f"      - {c.fk.table_name}.{c.fk.column_name} → "
                  f"{c.pk.table_name}.{c.pk.column_name} "
                  f"(confidence={c.confidence:.2f})")

        # Step 3: Extract sample rows
        print("\n[3/5] Extracting sample rows...")
        for p in profiles:
            rows = extract_sample_rows(p.file_path, n=3)
            print(f"      {p.table_name}: {len(rows)} rows extracted")
            if rows:
                print(f"        columns: {list(rows[0].keys())[:5]}...")

        # Step 4: Build documents
        print("\n[4/5] Building documents...")
        docs = build_documents(profiles, report, str(test_dir))
        print(f"      Built {len(docs)} documents")
        for doc in docs:
            dtype = doc.metadata.get("doc_type", "unknown")
            print(f"      - {dtype}: {len(doc.page_content)} chars")

        # Step 5: Full enrichment (vector store + LLM)
        print("\n[5/5] Running LLM enrichment (this may take a minute)...")
        result = await enrich(
            profiles=profiles,
            report=report,
            dir_path=str(test_dir),
            provider="google",
            model="gemini-2.5-flash",
        )

        print(f"\n{'='*60}")
        print("ENRICHMENT RESULT")
        print(f"{'='*60}")
        print(f"Tables analyzed: {result['tables_analyzed']}")
        print(f"Relationships analyzed: {result['relationships_analyzed']}")
        print(f"Documents embedded: {result['documents_embedded']}")
        print(f"\n--- LLM Analysis ({len(result['enrichment'])} chars) ---\n")
        print(result["enrichment"][:4000])
        if len(result["enrichment"]) > 4000:
            print(f"\n... ({len(result['enrichment']) - 4000} more chars)")
        print(f"\n{'='*60}")

        has_er = "erDiagram" in result["enrichment"] or "mermaid" in result["enrichment"].lower()
        print(f"\n[{'PASS' if has_er else 'WARN'}] Enriched ER diagram {'found' if has_er else 'not found'}")
        print("[PASS] Enrichment pipeline complete!")

    finally:
        # Cleanup test directory
        shutil.rmtree(test_dir, ignore_errors=True)


if __name__ == "__main__":
    asyncio.run(test_enrichment())
