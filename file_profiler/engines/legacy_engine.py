# Layer 9 — Legacy Flat File Handler
#
# For fixed-width and other positional export formats.
#
# Requires a position mapping config: { column_name: (start, end) }
# No delimiter detection — column boundaries are absolute character positions.
# Strip padding (left/right) before type inference.
#
# Also handles:
#   - Encoded legacy date formats (e.g. YYYYMMDD -> ISO)
#   - Truncated values (flag if max_length == field_width exactly)
#   - Multi-line logical records (buffer until record terminator)
#
# Entry point:
#   profile(path: str | Path, strategy: SizeStrategy, position_map: dict) -> list[RawColumnData]
