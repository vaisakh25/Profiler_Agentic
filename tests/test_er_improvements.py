"""Test ER diagram improvements: audit column separation + deduplication."""
import sys
import io
if sys.platform == "win32":
    import asyncio
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

if __name__ != "__main__":
    sys.exit(0)

from file_profiler.main import profile_directory
from file_profiler.output.er_diagram_writer import generate, _is_audit_fk
from file_profiler.analysis.relationship_detector import detect

# Profile all CSVs
data_dir = "data/files/wwi_files"
profiles = profile_directory(data_dir, output_dir="data/output", parallel=False)
print(f"Loaded {len(profiles)} profiles")

# Detect relationships
report = detect(profiles)
print(f"Detected {len(report.candidates)} FK candidates (>= 0.50 confidence)")

# Count audit vs domain among all candidates
audit_all = [c for c in report.candidates if _is_audit_fk(c.fk.column_name)]
domain_all = [c for c in report.candidates if not _is_audit_fk(c.fk.column_name)]
print(f"  Audit FKs (lasteditedby etc): {len(audit_all)}")
print(f"  Domain FKs: {len(domain_all)}")

# Generate ER diagram
lines = generate(profiles, report)
mermaid_lines = [l for l in lines if "||--o{" in l]

print(f"\nER diagram: {len(mermaid_lines)} domain relationship edges drawn")

# Check for TRUE bidirectional duplicates (same column pair reversed)
edge_pairs = []
for l in mermaid_lines:
    parts = l.strip().split(" ||--o{ ")
    if len(parts) == 2:
        pk = parts[0].strip()
        rest = parts[1].split(" : ")
        fk = rest[0].strip()
        label = rest[1].strip('"') if len(rest) > 1 else ""
        edge_pairs.append((pk, fk, label))

true_dupes = 0
seen_labels = set()
for pk, fk, label in edge_pairs:
    # Extract column names from label "fk_col -> pk_col"
    if " -> " in label:
        fk_col, pk_col = label.split(" -> ")
        forward = (fk, fk_col.strip(), pk, pk_col.strip())
        reverse = (pk, pk_col.strip(), fk, fk_col.strip())
        if reverse in seen_labels:
            true_dupes += 1
            print(f"  TRUE BIDIRECTIONAL: {fk}.{fk_col} -> {pk}.{pk_col}")
        seen_labels.add(forward)

print(f"\nTrue bidirectional duplicates: {true_dupes}")

# Show audit FK section
print("\n--- Audit FK Section ---")
in_audit = False
for l in lines:
    if "Audit FK" in l:
        in_audit = True
    if in_audit:
        print(l)

# Compare before/after
print(f"\n=== SUMMARY ===")
print(f"Total FK candidates detected: {len(report.candidates)}")
print(f"Audit FKs separated: {len(audit_all)}")
print(f"Domain edges in diagram: {len(mermaid_lines)}")
print(f"True bidirectional dupes: {true_dupes}")
print(f"Reduction: {len(report.candidates)} -> {len(mermaid_lines)} edges ({100 - len(mermaid_lines)*100//max(len(report.candidates),1)}% reduction)")
