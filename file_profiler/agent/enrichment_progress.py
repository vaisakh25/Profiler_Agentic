"""Enrichment progress file IPC.

The MCP server writes a progress JSON file at each real pipeline phase
completion.  The web server polls this file to update the step tracker
synchronously with actual work, replacing the timer-based hint rotation.

Progress file format:
    {"step": 5, "name": "COLUMN CLUSTER: DBSCAN grouping", "detail": "3 clusters", "ts": 1710000000.0}
"""

from __future__ import annotations

import json
import logging
import time
from pathlib import Path

log = logging.getLogger(__name__)

_PROGRESS_FILENAME = ".enrichment_progress.json"


def progress_file_path(output_dir: Path) -> Path:
    """Return the path to the enrichment progress file."""
    return output_dir / _PROGRESS_FILENAME


def write_progress(
    output_dir: Path,
    step: int,
    name: str,
    detail: str = "",
    stats: dict | None = None,
) -> None:
    """Write current pipeline step to the progress file (atomic-ish).

    Args:
        stats: Optional live stats dict with keys like tables_done,
               total_tables, rows, columns for real-time UI counters.
    """
    path = progress_file_path(output_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    data = {
        "step": step,
        "name": name,
        "detail": detail,
        "ts": time.time(),
    }
    if stats:
        data["stats"] = stats
    payload = json.dumps(data)
    try:
        path.write_text(payload, encoding="utf-8")
    except Exception as exc:
        log.debug("Could not write progress file: %s", exc)


def read_progress(output_dir: Path) -> dict | None:
    """Read current progress from the progress file.

    Returns None if the file doesn't exist or is stale (> 5 minutes old).
    """
    path = progress_file_path(output_dir)
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        # Ignore stale progress from a previous run
        if time.time() - data.get("ts", 0) > 300:
            return None
        return data
    except Exception:
        return None


def clear_progress(output_dir: Path) -> None:
    """Remove the progress file after completion."""
    path = progress_file_path(output_dir)
    try:
        path.unlink(missing_ok=True)
    except Exception:
        pass


# ---------------------------------------------------------------------------
# Completion manifest — persistent enrichment state across restarts
# ---------------------------------------------------------------------------

_MANIFEST_FILENAME = ".enrichment_manifest.json"
_CHECKPOINT_FILENAME = ".enrichment_checkpoints.json"
_DEAD_LETTERS_FILENAME = ".enrichment_dead_letters.json"


# ---------------------------------------------------------------------------
# Per-table checkpointing — resumable MAP phase
# ---------------------------------------------------------------------------

def checkpoint_file_path(output_dir: Path) -> Path:
    """Return the path to the per-table checkpoint file."""
    return output_dir / _CHECKPOINT_FILENAME


def write_table_checkpoint(
    output_dir: Path,
    table_name: str,
    summary: str,
    column_descriptions: dict,
    fingerprint: str,
) -> None:
    """Atomically persist a single table's MAP result for crash recovery.

    Uses a temp-file + os.replace pattern so the checkpoint file is never
    in a partially-written state.  Safe for concurrent readers.
    """
    import os
    import tempfile

    path = checkpoint_file_path(output_dir)
    path.parent.mkdir(parents=True, exist_ok=True)

    # Read existing checkpoints
    existing = _read_checkpoint_file(path)

    # Add/update this table's entry
    existing[table_name] = {
        "summary": summary,
        "column_descriptions": column_descriptions,
        "fingerprint": fingerprint,
        "ts": time.time(),
    }

    # Atomic write: temp file in same directory, then os.replace
    payload = json.dumps(existing, ensure_ascii=False)
    try:
        fd, tmp_path = tempfile.mkstemp(
            dir=str(path.parent), suffix=".tmp", prefix=".ckpt_",
        )
        try:
            os.write(fd, payload.encode("utf-8"))
            os.close(fd)
            os.replace(tmp_path, str(path))
        except Exception:
            os.close(fd) if not os.get_inheritable(fd) else None
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise
    except Exception as exc:
        log.debug("Could not write table checkpoint for %s: %s", table_name, exc)


def read_table_checkpoints(output_dir: Path) -> dict[str, dict]:
    """Read all per-table checkpoints from disk.

    Returns:
        Dict mapping table_name -> {summary, column_descriptions, fingerprint, ts}.
        Empty dict if no checkpoint file exists.
    """
    path = checkpoint_file_path(output_dir)
    return _read_checkpoint_file(path)


def clear_table_checkpoints(output_dir: Path) -> None:
    """Remove the checkpoint file after successful pipeline completion."""
    path = checkpoint_file_path(output_dir)
    try:
        path.unlink(missing_ok=True)
    except Exception:
        pass


def _read_checkpoint_file(path: Path) -> dict:
    """Read the checkpoint JSON file, returning {} on any error."""
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            return data
        return {}
    except Exception:
        return {}


def manifest_path(output_dir: Path) -> Path:
    """Return the path to the enrichment completion manifest."""
    return output_dir / _MANIFEST_FILENAME


def write_manifest(
    output_dir: Path,
    dir_path: str,
    fingerprints: dict[str, str],
    result: dict,
    file_fingerprints: dict[str, str] | None = None,
) -> None:
    """Write a completion manifest after a successful enrichment run.

    Args:
        output_dir: Output directory for the manifest file.
        dir_path: The data directory that was enriched.
        fingerprints: Mapping of table_name → fingerprint hash (profile-based).
        result: The enrichment result dict (stored for cache retrieval).
        file_fingerprints: Mapping of file_stem → hash(size, mtime).
            Used by check_enrichment_status for lightweight change detection
            without needing to profile.
    """
    path = manifest_path(output_dir)
    path.parent.mkdir(parents=True, exist_ok=True)

    # Strip the large 'enrichment' text from the cached result to keep
    # the manifest compact — it can be re-read from enriched_er_diagram.md.
    cached_result = {k: v for k, v in result.items() if k != "enrichment"}

    manifest_data = {
        "dir_path": dir_path,
        "fingerprints": fingerprints,
        "result": cached_result,
        "ts": time.time(),
    }
    if file_fingerprints:
        manifest_data["file_fingerprints"] = file_fingerprints

    payload = json.dumps(manifest_data, indent=2)
    try:
        path.write_text(payload, encoding="utf-8")
        log.info("Enrichment manifest written: %d tables", len(fingerprints))
    except Exception as exc:
        log.warning("Could not write enrichment manifest: %s", exc)


def read_manifest(output_dir: Path) -> dict | None:
    """Read the enrichment completion manifest.

    Returns None if the manifest doesn't exist.
    """
    path = manifest_path(output_dir)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def check_enrichment_complete(
    output_dir: Path,
    dir_path: str,
    current_fingerprints: dict[str, str],
) -> dict:
    """Check if a previous enrichment run is still valid.

    Compares the current table fingerprints against the stored manifest.

    Returns:
        Dict with:
        - status: "complete" | "stale" | "none"
        - reason: Human-readable explanation
        - cached_result: The stored result dict (only if status == "complete")
        - changes: Details of what changed (only if status == "stale")
    """
    manifest = read_manifest(output_dir)
    if manifest is None:
        return {"status": "none", "reason": "No previous enrichment found."}

    stored_dir = manifest.get("dir_path", "")
    # Prefer file-level fingerprints (stat-based, no profiling needed);
    # fall back to profile-based fingerprints for older manifests.
    stored_fps = manifest.get("file_fingerprints") or manifest.get("fingerprints", {})

    # Check if it's the same directory
    if stored_dir != dir_path:
        return {
            "status": "stale",
            "reason": f"Previous enrichment was for '{stored_dir}', not '{dir_path}'.",
        }

    # Check for new, removed, or changed tables
    current_tables = set(current_fingerprints.keys())
    stored_tables = set(stored_fps.keys())

    new_tables = sorted(current_tables - stored_tables)
    removed_tables = sorted(stored_tables - current_tables)
    changed_tables = sorted(
        t for t in current_tables & stored_tables
        if current_fingerprints[t] != stored_fps[t]
    )

    if new_tables or removed_tables or changed_tables:
        return {
            "status": "stale",
            "reason": "Data has changed since last enrichment.",
            "changes": {
                "new_tables": new_tables,
                "removed_tables": removed_tables,
                "changed_tables": changed_tables,
            },
        }

    # Also verify key output files exist
    required_files = ["enriched_profiles.json", "enriched_er_diagram.md"]
    missing = [f for f in required_files if not (output_dir / f).exists()]
    if missing:
        return {
            "status": "stale",
            "reason": f"Output files missing: {', '.join(missing)}",
        }

    # Everything matches — enrichment is still valid
    cached_result = manifest.get("result", {})

    # Re-read the enrichment text from disk
    er_path = output_dir / "enriched_er_diagram.md"
    if er_path.exists():
        try:
            cached_result["enrichment"] = er_path.read_text(encoding="utf-8")
        except Exception:
            pass

    return {
        "status": "complete",
        "reason": "Previous enrichment is up-to-date. All table fingerprints match.",
        "cached_result": cached_result,
        "tables": len(current_fingerprints),
        "enriched_at": manifest.get("ts"),
    }


# ---------------------------------------------------------------------------
# Dead-letter persistence — failed MAP tables
# ---------------------------------------------------------------------------

def dead_letters_path(output_dir: Path) -> Path:
    """Return the path to the dead letters file."""
    return output_dir / _DEAD_LETTERS_FILENAME


def write_dead_letters(output_dir: Path, dead_letters: list[dict]) -> None:
    """Persist dead letter entries for later retry.

    Args:
        dead_letters: List of dicts (from DeadLetterEntry.to_dict()).
    """
    path = dead_letters_path(output_dir)
    path.parent.mkdir(parents=True, exist_ok=True)
    try:
        path.write_text(
            json.dumps(dead_letters, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        log.info("Dead letters written: %d failed tables", len(dead_letters))
    except Exception as exc:
        log.warning("Could not write dead letters: %s", exc)


def read_dead_letters(output_dir: Path) -> list[dict]:
    """Read persisted dead letter entries.

    Returns:
        List of dead letter dicts, or empty list if none exist.
    """
    path = dead_letters_path(output_dir)
    if not path.exists():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, list) else []
    except Exception:
        return []


def clear_dead_letters(output_dir: Path) -> None:
    """Remove the dead letters file."""
    path = dead_letters_path(output_dir)
    try:
        path.unlink(missing_ok=True)
    except Exception:
        pass
