"""PostgreSQL connection management for chat persistence.

Provides:
  - Async connection pool (psycopg_pool) for session CRUD
  - Schema initialization (sessions table)
  - PostgresSaver factory for LangGraph checkpointing

Falls back gracefully when POSTGRES_HOST is not configured.
"""

from __future__ import annotations

import asyncio
import logging
import sys
from typing import Optional

# psycopg3 async requires SelectorEventLoop on Windows (ProactorEventLoop
# is the default on Python 3.10+ / Windows and is not supported).
if sys.platform == "win32":
    import selectors
    asyncio.set_event_loop_policy(
        asyncio.WindowsSelectorEventLoopPolicy()  # type: ignore[attr-defined]
    )

from file_profiler.config.env import (
    POSTGRES_POOL_MAX,
    POSTGRES_POOL_MIN,
    get_postgres_dsn,
)

log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Singleton connection pool
# ---------------------------------------------------------------------------

_pool: Optional["psycopg_pool.AsyncConnectionPool"] = None


async def get_pool() -> Optional["psycopg_pool.AsyncConnectionPool"]:
    """Return the shared async connection pool, creating it on first call.

    Returns None if PostgreSQL is not configured (POSTGRES_HOST empty)
    or if the connection cannot be established within the timeout.
    """
    global _pool

    dsn = get_postgres_dsn()
    if not dsn:
        return None

    if _pool is not None:
        return _pool

    from psycopg_pool import AsyncConnectionPool

    try:
        _pool = AsyncConnectionPool(
            conninfo=dsn,
            min_size=POSTGRES_POOL_MIN,
            max_size=POSTGRES_POOL_MAX,
            open=False,
            timeout=10,          # max seconds to wait for a connection
            reconnect_timeout=5, # max seconds to reconnect a broken connection
        )
        await asyncio.wait_for(_pool.open(), timeout=15)
        log.info("PostgreSQL connection pool opened (min=%d, max=%d)",
                 POSTGRES_POOL_MIN, POSTGRES_POOL_MAX)

        # Initialize schema on first connect
        await _init_schema(_pool)
    except (asyncio.TimeoutError, Exception) as exc:
        log.warning("PostgreSQL pool failed to open (%s) — database features disabled", exc)
        _pool = None
        return None

    return _pool


async def close_pool() -> None:
    """Close the connection pool (call on shutdown)."""
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
        log.info("PostgreSQL connection pool closed")


# ---------------------------------------------------------------------------
# Schema initialization
# ---------------------------------------------------------------------------

_SCHEMA_SQL = """\
CREATE TABLE IF NOT EXISTS sessions (
    session_id   TEXT PRIMARY KEY,
    label        VARCHAR(500) NOT NULL DEFAULT '',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    message_count INT NOT NULL DEFAULT 0,
    metadata     JSONB NOT NULL DEFAULT '{}'
);

CREATE INDEX IF NOT EXISTS idx_sessions_updated
    ON sessions (updated_at DESC);
"""


async def _init_schema(pool: "psycopg_pool.AsyncConnectionPool") -> None:
    """Create application tables if they don't exist."""
    async with pool.connection() as conn:
        await conn.execute(_SCHEMA_SQL)
        await conn.commit()
    log.info("PostgreSQL schema initialized (sessions table)")


# ---------------------------------------------------------------------------
# LangGraph checkpointer factory
# ---------------------------------------------------------------------------

async def get_checkpointer():
    """Return a PostgresSaver checkpointer, or MemorySaver if PG unavailable.

    PostgresSaver.setup() creates its own tables (checkpoints,
    checkpoint_blobs, checkpoint_writes) automatically.
    """
    dsn = get_postgres_dsn()
    if not dsn:
        from langgraph.checkpoint.memory import MemorySaver
        log.debug("POSTGRES_HOST not set — using in-memory checkpointer")
        return MemorySaver()

    try:
        from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

        pool = await get_pool()
        if not pool:
            from langgraph.checkpoint.memory import MemorySaver
            return MemorySaver()

        # setup() uses CREATE INDEX CONCURRENTLY which cannot run inside a
        # transaction block.  Run it with a dedicated autocommit connection,
        # then create the actual checkpointer against the pool.
        import psycopg

        dsn = get_postgres_dsn()
        async with await asyncio.wait_for(
            psycopg.AsyncConnection.connect(dsn, autocommit=True),
            timeout=10,
        ) as setup_conn:
            setup_cp = AsyncPostgresSaver(setup_conn)
            await asyncio.wait_for(setup_cp.setup(), timeout=15)

        checkpointer = AsyncPostgresSaver(pool)
        log.info("PostgresSaver checkpointer ready")
        return checkpointer
    except Exception as exc:
        from langgraph.checkpoint.memory import MemorySaver
        log.warning("PostgreSQL checkpointer failed (%s) — falling back to MemorySaver", exc)
        return MemorySaver()
