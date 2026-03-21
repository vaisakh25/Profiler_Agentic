"""
Layer 6 — JSON / NDJSON Profiling Engine

Steps (all gated by SizeStrategy):
  A — Detect JSON shape: SINGLE_OBJECT / ARRAY_OF_OBJECTS / NDJSON / DEEP_NESTED
  B — Schema discovery via union: stream first N records, collect keys, track occurrence_ratio
  C — Flatten strategy: HYBRID (flatten shallow struct fields, stringify deep arrays)
  D — Sampling: full read / chunked reservoir / skip-interval stream
  E — Build RawColumnData (pivot records → columns)

Critical rules:
  - Never blindly explode large arrays (destroys row count integrity)
  - Flag TYPE_CONFLICT when same key holds different types across records
  - Track occurrence_ratio < 1.0 as optional/sparse field

Entry point:
  profile(path, strategy, intake) -> tuple[list[RawColumnData], int, bool]

Returns:
  (raw_columns, row_count, is_row_count_exact)
"""

from __future__ import annotations

import gzip
import io
import json
import logging
import random
import zipfile
from pathlib import Path
from typing import Any, Generator, Optional

from file_profiler.config import settings
from file_profiler.intake.errors import CorruptFileError
from file_profiler.intake.validator import IntakeResult
from file_profiler.models.enums import (
    FlattenStrategy,
    JSONShape,
    QualityFlag,
    SizeStrategy,
)
from file_profiler.models.file_profile import RawColumnData

log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def profile(
    path: str | Path,
    strategy: SizeStrategy,
    intake: IntakeResult,
) -> tuple[list[RawColumnData], int, bool]:
    """
    Profile a JSON file and return per-column raw data ready for the column profiler.

    Args:
        path:     Path to the JSON file (plain or compressed).
        strategy: Size strategy selected by Layer 3.
        intake:   IntakeResult from Layer 1.

    Returns:
        (raw_columns, row_count, is_row_count_exact)

    Raises:
        CorruptFileError — file cannot be parsed as JSON at all.
    """
    path = Path(path).resolve()

    # Step A — detect shape
    shape = _detect_shape(path, intake)
    log.debug("%s: JSON shape = %s", path.name, shape.value)

    if shape == JSONShape.SINGLE_OBJECT:
        return _profile_single_object(path, intake)

    # DuckDB fast path for large flat JSON / NDJSON files
    if (
        shape in (JSONShape.NDJSON, JSONShape.ARRAY_OF_OBJECTS)
        and intake.compression != "zip"
    ):
        duckdb_result = _try_duckdb_path(path, intake, shape)
        if duckdb_result is not None:
            return duckdb_result

    # Steps B+D combined — schema discovery and sampling in a SINGLE pass.
    # Previously this required two full iterations through the file.
    schema, row_count, sampled = _discover_schema_and_sample(
        path, intake, shape, strategy,
    )

    if not schema:
        log.warning("JSON engine: no schema discovered from %s", path.name)
        return [], 0, True

    if not sampled:
        log.warning("JSON engine: no records sampled from %s", path.name)
        return [], row_count, True

    # Step C — determine flatten strategy per field
    field_strategies = _assign_flatten_strategies(schema)

    # Step E — flatten records and build RawColumnData
    flat_keys = sorted(field_strategies.keys())
    raw_columns = _build_raw_columns(flat_keys, field_strategies, sampled, row_count)

    return raw_columns, row_count, True


# ---------------------------------------------------------------------------
# Step A — Shape Detection
# ---------------------------------------------------------------------------

def _detect_shape(path: Path, intake: IntakeResult) -> JSONShape:
    """Peek at the file content to determine the top-level JSON structure."""
    text = _read_text_snippet(path, intake, max_bytes=4096)
    stripped = text.lstrip()

    if not stripped:
        raise CorruptFileError(f"JSON file is empty or unreadable: {path.name}")

    first_char = stripped[0]

    if first_char == "[":
        # Could be array of objects — check if nested
        try:
            # Parse a small chunk to inspect structure
            sample_records = list(_iter_records_from_text(
                _read_full_text(path, intake), JSONShape.ARRAY_OF_OBJECTS, max_records=5
            ))
            if sample_records and any(_has_deep_nesting(r) for r in sample_records):
                return JSONShape.DEEP_NESTED
            return JSONShape.ARRAY_OF_OBJECTS
        except (json.JSONDecodeError, StopIteration):
            return JSONShape.ARRAY_OF_OBJECTS

    if first_char == "{":
        # Could be single object or NDJSON
        # Check if there are multiple lines each being a valid JSON object
        lines = stripped.splitlines()
        if len(lines) >= 2:
            valid_lines = 0
            for line in lines[:5]:
                line = line.strip()
                if not line:
                    continue
                try:
                    parsed = json.loads(line)
                    if isinstance(parsed, dict):
                        valid_lines += 1
                except json.JSONDecodeError:
                    break

            if valid_lines >= 2:
                # Check for deep nesting in NDJSON
                sample_records = []
                for line in lines[:5]:
                    line = line.strip()
                    if line:
                        try:
                            sample_records.append(json.loads(line))
                        except json.JSONDecodeError:
                            pass
                if sample_records and any(_has_deep_nesting(r) for r in sample_records):
                    return JSONShape.DEEP_NESTED
                return JSONShape.NDJSON

        # Single object (possibly deep nested)
        try:
            obj = json.loads(stripped)
            if isinstance(obj, dict) and _has_deep_nesting(obj):
                return JSONShape.DEEP_NESTED
        except json.JSONDecodeError:
            pass
        return JSONShape.SINGLE_OBJECT

    raise CorruptFileError(
        f"JSON file {path.name} does not start with {{ or [ — "
        f"cannot determine shape."
    )


def _has_deep_nesting(obj: dict, depth: int = 0, max_depth: int = 2) -> bool:
    """Check if a dict has nesting deeper than max_depth levels."""
    if depth > max_depth:
        return True
    for value in obj.values():
        if isinstance(value, dict):
            if _has_deep_nesting(value, depth + 1, max_depth):
                return True
        if isinstance(value, list):
            for item in value[:3]:  # check first few items
                if isinstance(item, dict):
                    if _has_deep_nesting(item, depth + 1, max_depth):
                        return True
    return False


# ---------------------------------------------------------------------------
# Step B — Schema Discovery
# ---------------------------------------------------------------------------

def _discover_schema(
    path: Path,
    intake: IntakeResult,
    shape: JSONShape,
) -> dict[str, _FieldInfo]:
    """
    Stream the first N records and build a union schema.

    Returns {flat_key: _FieldInfo} with occurrence counts and observed types.
    """
    schema: dict[str, _FieldInfo] = {}
    count = 0

    for record in _iter_records(path, intake, shape):
        if count >= settings.JSON_SCHEMA_DISCOVERY_SAMPLE:
            break
        flat = _flatten_record(record)
        for key, value in flat.items():
            if key not in schema:
                schema[key] = _FieldInfo(key=key)
            schema[key].occurrence_count += 1
            if value is not None:
                observed_type = type(value).__name__
                schema[key].observed_types.add(observed_type)
        count += 1

    # Set total_sampled so occurrence_ratio can be computed
    for info in schema.values():
        info.total_sampled = count

    return schema


class _FieldInfo:
    """Tracks schema metadata for a discovered JSON field."""
    __slots__ = ("key", "occurrence_count", "total_sampled", "observed_types")

    def __init__(self, key: str) -> None:
        self.key = key
        self.occurrence_count = 0
        self.total_sampled = 0
        self.observed_types: set[str] = set()

    @property
    def occurrence_ratio(self) -> float:
        if self.total_sampled == 0:
            return 0.0
        return self.occurrence_count / self.total_sampled

    @property
    def has_type_conflict(self) -> bool:
        # Ignore NoneType — a field can be nullable without conflict
        real_types = self.observed_types - {"NoneType"}
        return len(real_types) > 1


# ---------------------------------------------------------------------------
# Step C — Flatten Strategy Assignment
# ---------------------------------------------------------------------------

def _assign_flatten_strategies(
    schema: dict[str, _FieldInfo],
) -> dict[str, FlattenStrategy]:
    """
    Assign a flatten strategy to each discovered field.

    HYBRID (default): primitive and shallow struct fields are flattened;
    list/dict fields deeper than 2 levels are stringified.
    """
    strategies: dict[str, FlattenStrategy] = {}
    for key, info in schema.items():
        # Fields that were already flattened by _flatten_record are primitives
        # or shallow structs — they use HYBRID (which means "already flat").
        # Fields containing lists/dicts in their values would have been
        # stringified during flattening.
        strategies[key] = FlattenStrategy.HYBRID
    return strategies


# ---------------------------------------------------------------------------
# Combined single-pass: Schema Discovery + Sampling
# ---------------------------------------------------------------------------

def _discover_schema_and_sample(
    path: Path,
    intake: IntakeResult,
    shape: JSONShape,
    strategy: SizeStrategy,
) -> tuple[dict[str, "_FieldInfo"], int, list[dict]]:
    """
    Discover the union schema AND collect samples in a SINGLE iteration.

    Replaces the separate _discover_schema + _reservoir_sample /
    _skip_interval_sample calls that previously required two full passes
    through the file.

    Schema discovery runs on the first JSON_SCHEMA_DISCOVERY_SAMPLE records;
    sampling runs across all records according to the strategy.

    Returns:
        (schema, row_count, sampled_records)
    """
    schema_limit = settings.JSON_SCHEMA_DISCOVERY_SAMPLE
    k = settings.SAMPLE_ROW_COUNT
    interval = settings.STREAM_SKIP_INTERVAL
    rng = random.Random(42)

    schema: dict[str, _FieldInfo] = {}
    sample: list[dict] = []
    total = 0

    for record in _iter_records(path, intake, shape):
        # --- Schema discovery (first N records) ---
        if total < schema_limit:
            flat = _flatten_record(record)
            for key, value in flat.items():
                if key not in schema:
                    schema[key] = _FieldInfo(key=key)
                schema[key].occurrence_count += 1
                if value is not None:
                    schema[key].observed_types.add(type(value).__name__)

        # --- Sampling ---
        if strategy == SizeStrategy.MEMORY_SAFE:
            sample.append(record)
        elif strategy == SizeStrategy.LAZY_SCAN:
            if total < k:
                sample.append(record)
            else:
                j = rng.randint(0, total)
                if j < k:
                    sample[j] = record
        else:  # STREAM_ONLY
            if total % interval == 0:
                sample.append(record)

        total += 1

    # Finalize schema: set total_sampled for occurrence_ratio
    schema_count = min(total, schema_limit)
    for info in schema.values():
        info.total_sampled = schema_count

    return schema, total, sample


# ---------------------------------------------------------------------------
# DuckDB fast path (flat JSON, > DUCKDB_ROW_THRESHOLD rows)
# ---------------------------------------------------------------------------

def _try_duckdb_path(
    path: Path,
    intake: IntakeResult,
    shape: JSONShape,
) -> Optional[tuple[list[RawColumnData], int, bool]]:
    """
    Attempt DuckDB-accelerated profiling for large flat JSON files.

    Returns None if DuckDB is not applicable (nested data, small table,
    or DuckDB failure), allowing the caller to fall through to the
    Python path.
    """
    try:
        from file_profiler.engines.duckdb_sampler import (
            duckdb_connection,
            duckdb_count_json,
            duckdb_sample_json,
        )
    except ImportError:
        return None

    try:
        with duckdb_connection() as con:
            row_count = duckdb_count_json(path, _con=con)

            if row_count <= settings.DUCKDB_ROW_THRESHOLD:
                return None

            headers, sampled_rows = duckdb_sample_json(path, _con=con)
    except Exception as exc:
        log.debug("DuckDB JSON failed for %s: %s", path.name, exc)
        return None

    if not sampled_rows:
        log.warning("DuckDB: no rows sampled from %s", path.name)
        return [], row_count, True

    # Check for nested structs in sampled data.  DuckDB returns dicts for
    # struct columns — if any column has dict values the Python engine will
    # produce a better flattened result.
    first_row = sampled_rows[0]
    for v in first_row:
        if v.startswith("{") or v.startswith("["):
            # Likely a struct/list column — fall back to Python for proper flattening
            log.debug(
                "DuckDB: %s has nested columns — falling back to Python",
                path.name,
            )
            return None

    raw_cols: list[RawColumnData] = []
    for col_idx, name in enumerate(headers):
        values: list[Optional[str]] = []
        null_count = 0
        for row in sampled_rows:
            v = row[col_idx] if col_idx < len(row) else ""
            if v == "":
                values.append(None)
                null_count += 1
            else:
                values.append(v)
        raw_cols.append(RawColumnData(
            name=name,
            declared_type=None,
            values=values,
            total_count=row_count,
            null_count=null_count,
        ))

    log.info(
        "DuckDB profiled (JSON): %s (%d rows, %d columns, %d sampled)",
        path.name, row_count, len(headers), len(sampled_rows),
    )
    return raw_cols, row_count, True


# ---------------------------------------------------------------------------
# Step D — Record Iteration & Sampling
# ---------------------------------------------------------------------------

def _iter_records(
    path: Path,
    intake: IntakeResult,
    shape: JSONShape,
) -> Generator[dict, None, None]:
    """Yield individual records from a JSON file regardless of shape."""
    if shape == JSONShape.NDJSON or (
        shape == JSONShape.DEEP_NESTED
        and _is_ndjson_format(path, intake)
    ):
        yield from _iter_ndjson(path, intake)
    elif shape in (JSONShape.ARRAY_OF_OBJECTS, JSONShape.DEEP_NESTED):
        yield from _iter_array(path, intake)
    elif shape == JSONShape.SINGLE_OBJECT:
        text = _read_full_text(path, intake)
        try:
            obj = json.loads(text)
            if isinstance(obj, dict):
                yield obj
        except json.JSONDecodeError as exc:
            raise CorruptFileError(
                f"Cannot parse JSON from {path.name}: {exc}"
            ) from exc


def _iter_ndjson(path: Path, intake: IntakeResult) -> Generator[dict, None, None]:
    """Yield one dict per line from an NDJSON file."""
    with _open_text(path, intake) as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
                if isinstance(obj, dict):
                    yield obj
            except json.JSONDecodeError:
                log.debug("Skipping unparseable NDJSON line in %s", path.name)


def _iter_array(path: Path, intake: IntakeResult) -> Generator[dict, None, None]:
    """Yield dicts from a JSON array of objects."""
    text = _read_full_text(path, intake)
    try:
        data = json.loads(text)
    except json.JSONDecodeError as exc:
        raise CorruptFileError(
            f"Cannot parse JSON array from {path.name}: {exc}"
        ) from exc

    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                yield item
    elif isinstance(data, dict):
        # Might be a wrapper: {"data": [{...}, ...]}
        for value in data.values():
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, dict):
                        yield item
                break  # use the first array found


def _iter_records_from_text(
    text: str,
    shape: JSONShape,
    max_records: int = -1,
) -> Generator[dict, None, None]:
    """Yield records from already-loaded text (used for shape detection)."""
    if shape == JSONShape.ARRAY_OF_OBJECTS:
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            return
        if isinstance(data, list):
            count = 0
            for item in data:
                if isinstance(item, dict):
                    yield item
                    count += 1
                    if max_records > 0 and count >= max_records:
                        return


def _reservoir_sample(
    path: Path,
    intake: IntakeResult,
    shape: JSONShape,
) -> tuple[int, list[dict]]:
    """Vitter's Algorithm R — uniform random sample bounded to SAMPLE_ROW_COUNT."""
    k = settings.SAMPLE_ROW_COUNT
    sample: list[dict] = []
    rng = random.Random(42)
    total = 0

    for record in _iter_records(path, intake, shape):
        if total < k:
            sample.append(record)
        else:
            j = rng.randint(0, total)
            if j < k:
                sample[j] = record
        total += 1

    return total, sample


def _skip_interval_sample(
    path: Path,
    intake: IntakeResult,
    shape: JSONShape,
) -> tuple[int, list[dict]]:
    """Skip-interval sampling for STREAM_ONLY strategy."""
    interval = settings.STREAM_SKIP_INTERVAL
    sample: list[dict] = []
    total = 0

    for record in _iter_records(path, intake, shape):
        if total % interval == 0:
            sample.append(record)
        total += 1

    return total, sample


# ---------------------------------------------------------------------------
# Step E — Flatten Records & Build RawColumnData
# ---------------------------------------------------------------------------

def _flatten_record(
    record: dict,
    prefix: str = "",
    max_depth: int = 3,
    _depth: int = 0,
) -> dict[str, Any]:
    """
    Flatten a nested dict into dot-free underscore-joined keys.

    - Primitive values are kept as-is.
    - Nested dicts are recursively expanded up to max_depth.
    - Lists and deeply nested dicts are stringified (JSON string).

    Examples:
        {"user": {"name": "Alice", "scores": [1,2,3]}}
        → {"user_name": "Alice", "user_scores": "[1, 2, 3]"}
    """
    flat: dict[str, Any] = {}

    for key, value in record.items():
        flat_key = f"{prefix}{key}" if not prefix else f"{prefix}_{key}"

        if isinstance(value, dict) and _depth < max_depth:
            nested = _flatten_record(value, flat_key, max_depth, _depth + 1)
            flat.update(nested)
        elif isinstance(value, (list, dict)):
            # Stringify complex types
            try:
                flat[flat_key] = json.dumps(value, default=str)
            except (TypeError, ValueError):
                flat[flat_key] = str(value)
        else:
            flat[flat_key] = value

    return flat


def _build_raw_columns(
    flat_keys: list[str],
    field_strategies: dict[str, FlattenStrategy],
    records: list[dict],
    total_count: int,
) -> list[RawColumnData]:
    """Pivot flattened records into columnar RawColumnData."""
    if not flat_keys or not records:
        return []

    # Flatten all records
    flat_records = [_flatten_record(r) for r in records]

    raw_cols: list[RawColumnData] = []
    for key in flat_keys:
        values: list[Optional[str]] = []
        null_count = 0

        for flat_record in flat_records:
            raw_value = flat_record.get(key)
            if raw_value is None:
                values.append(None)
                null_count += 1
            else:
                str_val = _value_to_str(raw_value)
                if str_val == "":
                    values.append(None)
                    null_count += 1
                else:
                    values.append(str_val)

        raw_cols.append(RawColumnData(
            name=key,
            declared_type=None,
            values=values,
            total_count=total_count,
            null_count=null_count,
        ))

    return raw_cols


def _profile_single_object(
    path: Path,
    intake: IntakeResult,
) -> tuple[list[RawColumnData], int, bool]:
    """
    Profile a single JSON object as a 1-row table.

    Each top-level key becomes a column; the single object is the one row.
    """
    text = _read_full_text(path, intake)
    try:
        obj = json.loads(text)
    except json.JSONDecodeError as exc:
        raise CorruptFileError(
            f"Cannot parse single JSON object from {path.name}: {exc}"
        ) from exc

    if not isinstance(obj, dict):
        raise CorruptFileError(
            f"Expected a JSON object in {path.name}, got {type(obj).__name__}"
        )

    flat = _flatten_record(obj)
    if not flat:
        return [], 0, True

    raw_cols: list[RawColumnData] = []
    for key, value in sorted(flat.items()):
        str_val = _value_to_str(value)
        is_null = str_val is None or str_val == ""
        raw_cols.append(RawColumnData(
            name=key,
            declared_type=None,
            values=[None if is_null else str_val],
            total_count=1,
            null_count=1 if is_null else 0,
        ))

    return raw_cols, 1, True


# ---------------------------------------------------------------------------
# Value conversion helpers
# ---------------------------------------------------------------------------

def _value_to_str(value: Any) -> Optional[str]:
    """Convert a Python value to Optional[str] for RawColumnData."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (list, dict)):
        try:
            return json.dumps(value, default=str)
        except (TypeError, ValueError):
            return str(value)
    return str(value)


# ---------------------------------------------------------------------------
# I/O helpers
# ---------------------------------------------------------------------------

def _open_text(path: Path, intake: IntakeResult):
    """Return a text-mode file handle, transparently decompressing."""
    encoding = intake.encoding

    if intake.compression == "gz":
        return gzip.open(path, "rt", encoding=encoding, errors="replace")

    if intake.compression == "zip":
        return _ZipTextWrapper(path, encoding)

    return open(path, "r", encoding=encoding, errors="replace")


class _ZipTextWrapper:
    """Context manager: opens the first entry of a zip archive as text."""

    def __init__(self, path: Path, encoding: str) -> None:
        self._path = path
        self._encoding = encoding
        self._zf = None
        self._fh = None

    def __enter__(self):
        self._zf = zipfile.ZipFile(self._path, "r")
        entries = self._zf.namelist()
        entry_name = entries[0] if entries else None
        if entry_name is None:
            raise CorruptFileError(f"ZIP archive {self._path.name} has no entries")
        binary = self._zf.open(entry_name)
        self._fh = io.TextIOWrapper(
            binary, encoding=self._encoding, errors="replace", newline=""
        )
        return self._fh

    def __exit__(self, *_):
        if self._fh:
            self._fh.detach()
        if self._zf:
            self._zf.close()


def _read_text_snippet(path: Path, intake: IntakeResult, max_bytes: int = 4096) -> str:
    """Read a small text snippet for shape detection."""
    encoding = intake.encoding

    if intake.compression == "gz":
        try:
            with gzip.open(path, "rb") as fh:
                raw = fh.read(max_bytes)
            return raw.decode(encoding, errors="replace")
        except Exception:
            pass

    if intake.compression == "zip":
        try:
            with zipfile.ZipFile(path, "r") as zf:
                entry = zf.namelist()[0]
                with zf.open(entry) as fh:
                    raw = fh.read(max_bytes)
                return raw.decode(encoding, errors="replace")
        except Exception:
            pass

    with open(path, "r", encoding=encoding, errors="replace") as fh:
        return fh.read(max_bytes)


def _read_full_text(path: Path, intake: IntakeResult) -> str:
    """Read the entire file as text."""
    encoding = intake.encoding

    if intake.compression == "gz":
        with gzip.open(path, "rt", encoding=encoding, errors="replace") as fh:
            return fh.read()

    if intake.compression == "zip":
        with zipfile.ZipFile(path, "r") as zf:
            entry = zf.namelist()[0]
            with zf.open(entry) as fh:
                return fh.read().decode(encoding, errors="replace")

    with open(path, "r", encoding=encoding, errors="replace") as fh:
        return fh.read()


def _is_ndjson_format(path: Path, intake: IntakeResult) -> bool:
    """Check if the file is actually NDJSON (each line is a JSON object)."""
    text = _read_text_snippet(path, intake, max_bytes=2048)
    lines = text.strip().splitlines()
    if len(lines) < 2:
        return False
    valid = 0
    for line in lines[:5]:
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
            if isinstance(obj, dict):
                valid += 1
        except json.JSONDecodeError:
            return False
    return valid >= 2
