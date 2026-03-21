"""Session persistence for the Data Profiler chat UI.

CRUD operations on the ``sessions`` PostgreSQL table.  Falls back to
an in-memory dict when PostgreSQL is unavailable so the UI keeps working.
"""

from __future__ import annotations

import logging
import time
from typing import Optional

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# In-memory fallback (used when PostgreSQL is not configured)
# ---------------------------------------------------------------------------

_memory_sessions: dict[str, dict] = {}  # session_id → {label, created_at, ...}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def touch_session(session_id: str, label: str = "") -> dict:
    """Create a session if it doesn't exist, or bump its updated_at.

    Returns the session dict. Falls back to in-memory on any DB error.
    """
    from file_profiler.config.database import get_pool

    pool = await get_pool()
    if pool is None:
        return _memory_touch(session_id, label)

    try:
        async with pool.connection() as conn:
            row = await conn.execute(
                """
                INSERT INTO sessions (session_id, label, updated_at)
                VALUES (%s, %s, NOW())
                ON CONFLICT (session_id) DO UPDATE
                    SET updated_at = NOW(),
                        label = CASE WHEN sessions.label = '' AND %s != ''
                                     THEN %s ELSE sessions.label END
                RETURNING session_id, label, created_at, updated_at, message_count, metadata
                """,
                (session_id, label, label, label),
            )
            result = await row.fetchone()
            await conn.commit()
        return _row_to_dict(result)
    except Exception as exc:
        log.warning("touch_session DB error (%s) — using in-memory fallback", exc)
        return _memory_touch(session_id, label)


async def update_session(
    session_id: str,
    label: str | None = None,
    message_count: int | None = None,
    metadata: dict | None = None,
) -> Optional[dict]:
    """Update session fields. Returns updated session or None if not found."""
    from file_profiler.config.database import get_pool

    pool = await get_pool()
    if pool is None:
        return _memory_update(session_id, label, message_count)

    sets: list[str] = ["updated_at = NOW()"]
    params: list = []

    if label is not None:
        sets.append("label = %s")
        params.append(label)
    if message_count is not None:
        sets.append("message_count = %s")
        params.append(message_count)
    if metadata is not None:
        sets.append("metadata = metadata || %s::jsonb")
        import json
        params.append(json.dumps(metadata))

    params.append(session_id)

    try:
        async with pool.connection() as conn:
            row = await conn.execute(
                f"UPDATE sessions SET {', '.join(sets)} "
                f"WHERE session_id = %s "
                f"RETURNING session_id, label, created_at, updated_at, message_count, metadata",
                params,
            )
            result = await row.fetchone()
            await conn.commit()
        return _row_to_dict(result) if result else None
    except Exception as exc:
        log.warning("update_session DB error (%s) — using in-memory fallback", exc)
        return _memory_update(session_id, label, message_count)


async def list_sessions(limit: int = 30) -> list[dict]:
    """Return the most recent sessions, newest first."""
    from file_profiler.config.database import get_pool

    pool = await get_pool()
    if pool is None:
        return _memory_list(limit)

    try:
        async with pool.connection() as conn:
            cur = await conn.execute(
                "SELECT session_id, label, created_at, updated_at, message_count, metadata "
                "FROM sessions ORDER BY updated_at DESC LIMIT %s",
                (limit,),
            )
            rows = await cur.fetchall()
        return [_row_to_dict(r) for r in rows]
    except Exception as exc:
        log.warning("list_sessions DB error (%s) — using in-memory fallback", exc)
        return _memory_list(limit)


async def delete_session(session_id: str) -> bool:
    """Delete a session. Returns True if it existed."""
    from file_profiler.config.database import get_pool

    pool = await get_pool()
    if pool is None:
        return _memory_sessions.pop(session_id, None) is not None

    try:
        async with pool.connection() as conn:
            cur = await conn.execute(
                "DELETE FROM sessions WHERE session_id = %s", (session_id,)
            )
            await conn.commit()
            return cur.rowcount > 0
    except Exception as exc:
        log.warning("delete_session DB error (%s) — using in-memory fallback", exc)
        return _memory_sessions.pop(session_id, None) is not None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _row_to_dict(row) -> dict:
    """Convert a database row to a session dict."""
    if row is None:
        return {}
    return {
        "session_id": row[0],
        "label": row[1],
        "created_at": row[2].isoformat() if hasattr(row[2], "isoformat") else str(row[2]),
        "updated_at": row[3].isoformat() if hasattr(row[3], "isoformat") else str(row[3]),
        "message_count": row[4],
        "metadata": row[5] if row[5] else {},
    }


# ---------------------------------------------------------------------------
# In-memory fallback implementations
# ---------------------------------------------------------------------------

def _memory_touch(session_id: str, label: str = "") -> dict:
    now = time.time()
    if session_id in _memory_sessions:
        s = _memory_sessions[session_id]
        s["updated_at"] = now
        if label and not s.get("label"):
            s["label"] = label
    else:
        _memory_sessions[session_id] = {
            "session_id": session_id,
            "label": label,
            "created_at": now,
            "updated_at": now,
            "message_count": 0,
            "metadata": {},
        }
    return _memory_sessions[session_id]


def _memory_update(
    session_id: str,
    label: str | None,
    message_count: int | None,
) -> Optional[dict]:
    s = _memory_sessions.get(session_id)
    if not s:
        return None
    s["updated_at"] = time.time()
    if label is not None:
        s["label"] = label
    if message_count is not None:
        s["message_count"] = message_count
    return s


def _memory_list(limit: int) -> list[dict]:
    sessions = sorted(
        _memory_sessions.values(),
        key=lambda s: s.get("updated_at", 0),
        reverse=True,
    )
    return sessions[:limit]
