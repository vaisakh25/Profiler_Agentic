"""
File resolver — validates and resolves paths for MCP tool handlers.

All tool handlers call resolve_path() before passing to the pipeline.
This keeps path validation, security checks, and upload directory
management out of the tool handler code.
"""

from __future__ import annotations

import base64
import logging
import shutil
import time
import uuid
from pathlib import Path

from file_profiler.config.env import DATA_DIR, UPLOAD_DIR, MAX_UPLOAD_SIZE_MB, UPLOAD_TTL_HOURS

log = logging.getLogger(__name__)


class PathSecurityError(Exception):
    """Raised when a resolved path falls outside allowed directories."""


def resolve_path(path: str) -> Path:
    """
    Resolve a user-provided path string to a validated local Path.

    Security: the resolved path must be under DATA_DIR or UPLOAD_DIR.
    This prevents directory traversal attacks (../../etc/passwd).

    Raises:
        FileNotFoundError:  path does not exist.
        PathSecurityError:  path resolves outside allowed directories.
    """
    resolved = Path(path).resolve()

    allowed_roots = (DATA_DIR.resolve(), UPLOAD_DIR.resolve())
    if not any(_is_subpath(resolved, root) for root in allowed_roots):
        raise PathSecurityError(
            f"Access denied: '{path}' resolves outside allowed directories. "
            f"Files must be under {DATA_DIR} or {UPLOAD_DIR}."
        )

    if not resolved.exists():
        raise FileNotFoundError(f"Path not found: {path}")

    return resolved


def save_upload(file_name: str, content_base64: str) -> Path:
    """
    Decode a base64-encoded file and write it to the upload directory.

    Each upload gets a UUID-isolated subdirectory to prevent name
    collisions and simplify cleanup.

    Returns:
        The server-side Path where the file was written.

    Raises:
        ValueError: content exceeds MAX_UPLOAD_SIZE_MB or base64 is invalid.
    """
    try:
        raw = base64.b64decode(content_base64, validate=True)
    except Exception as exc:
        raise ValueError(f"Invalid base64 content: {exc}") from exc

    size_mb = len(raw) / (1024 * 1024)
    if size_mb > MAX_UPLOAD_SIZE_MB:
        raise ValueError(
            f"Upload too large: {size_mb:.1f} MB exceeds limit of "
            f"{MAX_UPLOAD_SIZE_MB} MB"
        )

    upload_id = uuid.uuid4().hex[:12]
    dest_dir = UPLOAD_DIR / upload_id
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest = dest_dir / file_name
    dest.write_bytes(raw)

    log.info("Upload saved: %s (%d bytes)", dest, len(raw))
    return dest


def cleanup_expired_uploads() -> int:
    """
    Remove upload subdirectories older than UPLOAD_TTL_HOURS.

    Each upload lives in UPLOAD_DIR/<uuid>/.  We check directory mtime
    against the TTL and remove the entire subdirectory if expired.

    Returns:
        Number of directories removed.
    """
    if not UPLOAD_DIR.exists():
        return 0

    ttl_seconds = UPLOAD_TTL_HOURS * 3600
    cutoff = time.time() - ttl_seconds
    removed = 0

    for entry in UPLOAD_DIR.iterdir():
        if not entry.is_dir():
            continue
        try:
            if entry.stat().st_mtime < cutoff:
                shutil.rmtree(entry)
                removed += 1
                log.info("Upload expired, removed: %s", entry.name)
        except OSError as exc:
            log.warning("Could not remove expired upload %s: %s", entry.name, exc)

    if removed:
        log.info("Upload cleanup: removed %d expired director(ies)", removed)
    return removed


def resolve_source(path_or_uri: str):
    """Resolve a user input to either a local Path or a SourceDescriptor.

    If the input is a remote URI (s3://, abfss://, gs://, snowflake://,
    postgresql://), returns a SourceDescriptor for the connector layer.
    Otherwise, applies existing local path security checks and returns
    a Path.

    Returns:
        Path | SourceDescriptor
    """
    from file_profiler.connectors.uri_parser import is_remote_uri, parse_uri
    if is_remote_uri(path_or_uri):
        return parse_uri(path_or_uri)
    return resolve_path(path_or_uri)


def _is_subpath(child: Path, parent: Path) -> bool:
    """Check if child is equal to or a subpath of parent."""
    try:
        child.relative_to(parent)
        return True
    except ValueError:
        return False
