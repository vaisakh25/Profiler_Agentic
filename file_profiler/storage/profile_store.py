"""Profile storage backends: file-based (default) and PostgreSQL.

Both backends store serialised FileProfile dicts (JSON-compatible).
The PostgreSQL backend uses JSONB for fast querying and GIN indexing.

Usage:
    store = await get_profile_store()
    await store.save_profile("customers", profile_dict, fingerprint="abc123")
    data = await store.load_profile("customers")
"""

from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)


class ProfileStore(ABC):
    """Abstract interface for profile persistence."""

    @abstractmethod
    async def save_profile(self, table_name: str, profile_data: dict, fingerprint: str = "") -> None:
        """Save or update a profile."""

    @abstractmethod
    async def load_profile(self, table_name: str) -> Optional[dict]:
        """Load a single profile by table name. Returns None if not found."""

    @abstractmethod
    async def load_all_profiles(self) -> dict[str, dict]:
        """Load all stored profiles. Returns {table_name: profile_data}."""

    @abstractmethod
    async def list_table_names(self) -> list[str]:
        """List all stored table names."""

    @abstractmethod
    async def delete_profile(self, table_name: str) -> bool:
        """Delete a profile. Returns True if it existed."""

    @abstractmethod
    async def get_fingerprint(self, table_name: str) -> Optional[str]:
        """Get the stored fingerprint for a table. Returns None if not found."""


class FileProfileStore(ProfileStore):
    """File-based profile store — wraps existing JSON file I/O."""

    def __init__(self, output_dir: Path) -> None:
        self._dir = output_dir
        self._dir.mkdir(parents=True, exist_ok=True)

    def _path(self, table_name: str) -> Path:
        return self._dir / f"{table_name}_profile.json"

    async def save_profile(self, table_name: str, profile_data: dict, fingerprint: str = "") -> None:
        import tempfile, os
        path = self._path(table_name)
        if fingerprint:
            profile_data["_fingerprint"] = fingerprint
        data = json.dumps(profile_data, indent=2, ensure_ascii=False, default=str) + "\n"
        fd, tmp = tempfile.mkstemp(dir=str(self._dir), suffix=".tmp")
        try:
            os.write(fd, data.encode("utf-8"))
            os.close(fd)
            os.replace(tmp, str(path))
        except Exception:
            os.close(fd) if not os.get_inheritable(fd) else None
            if Path(tmp).exists():
                os.unlink(tmp)
            raise

    async def load_profile(self, table_name: str) -> Optional[dict]:
        path = self._path(table_name)
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            log.warning("Failed to load profile %s: %s", table_name, exc)
            return None

    async def load_all_profiles(self) -> dict[str, dict]:
        result = {}
        for p in self._dir.glob("*_profile.json"):
            table_name = p.stem.removesuffix("_profile")
            try:
                result[table_name] = json.loads(p.read_text(encoding="utf-8"))
            except Exception as exc:
                log.warning("Failed to load profile %s: %s", p.name, exc)
        return result

    async def list_table_names(self) -> list[str]:
        return sorted(
            p.stem.removesuffix("_profile")
            for p in self._dir.glob("*_profile.json")
        )

    async def delete_profile(self, table_name: str) -> bool:
        path = self._path(table_name)
        if path.exists():
            path.unlink()
            return True
        return False

    async def get_fingerprint(self, table_name: str) -> Optional[str]:
        data = await self.load_profile(table_name)
        if data:
            return data.get("_fingerprint")
        return None


class PostgresProfileStore(ProfileStore):
    """PostgreSQL-backed profile store using JSONB."""

    def __init__(self, pool) -> None:
        self._pool = pool

    async def save_profile(self, table_name: str, profile_data: dict, fingerprint: str = "") -> None:
        import json as _json
        async with self._pool.connection() as conn:
            await conn.execute(
                """
                INSERT INTO profiles (table_name, profile_data, fingerprint, updated_at)
                VALUES (%s, %s::jsonb, %s, NOW())
                ON CONFLICT (table_name)
                DO UPDATE SET profile_data = EXCLUDED.profile_data,
                              fingerprint = EXCLUDED.fingerprint,
                              updated_at = NOW()
                """,
                (table_name, _json.dumps(profile_data, default=str), fingerprint),
            )
            await conn.commit()

    async def load_profile(self, table_name: str) -> Optional[dict]:
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                "SELECT profile_data FROM profiles WHERE table_name = %s",
                (table_name,),
            )
            row = await cur.fetchone()
            return row[0] if row else None

    async def load_all_profiles(self) -> dict[str, dict]:
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                "SELECT table_name, profile_data FROM profiles ORDER BY table_name"
            )
            rows = await cur.fetchall()
            return {row[0]: row[1] for row in rows}

    async def list_table_names(self) -> list[str]:
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                "SELECT table_name FROM profiles ORDER BY table_name"
            )
            rows = await cur.fetchall()
            return [row[0] for row in rows]

    async def delete_profile(self, table_name: str) -> bool:
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                "DELETE FROM profiles WHERE table_name = %s", (table_name,),
            )
            await conn.commit()
            return cur.rowcount > 0

    async def get_fingerprint(self, table_name: str) -> Optional[str]:
        async with self._pool.connection() as conn:
            cur = await conn.execute(
                "SELECT fingerprint FROM profiles WHERE table_name = %s",
                (table_name,),
            )
            row = await cur.fetchone()
            return row[0] if row else None


# ---------------------------------------------------------------------------
# Factory
# ---------------------------------------------------------------------------

_store: Optional[ProfileStore] = None


async def get_profile_store() -> ProfileStore:
    """Return the profile store singleton.

    Uses PostgreSQL if configured and available, otherwise file-based.
    """
    global _store
    if _store is not None:
        return _store

    from file_profiler.config.env import OUTPUT_DIR

    try:
        from file_profiler.config.database import get_pool
        pool = await get_pool()
        if pool:
            _store = PostgresProfileStore(pool)
            log.info("Using PostgreSQL profile store")
            return _store
    except Exception as exc:
        log.debug("PostgreSQL profile store unavailable: %s", exc)

    _store = FileProfileStore(OUTPUT_DIR)
    log.info("Using file-based profile store at %s", OUTPUT_DIR)
    return _store
