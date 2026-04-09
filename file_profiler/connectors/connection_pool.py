"""
PostgreSQL connection pooling — efficient connection reuse.

Uses psycopg's built-in ConnectionPool for managing a pool of connections
per connection_id. Pools are lazily created on first use and reused for
subsequent queries.

Pool configuration (optional in credentials dict):
    - pool_min_size: Minimum connections to maintain (default: 2)
    - pool_max_size: Maximum connections in pool (default: 10)
    - pool_timeout: Wait timeout for available connection (default: 30.0 seconds)
    - pool_max_idle: Close idle connections after this duration (default: 600.0 seconds)
    - pool_max_lifetime: Recycle connections after this duration (default: 3600.0 seconds)

Usage:
    from file_profiler.connectors.connection_pool import get_pool_manager
    
    pool_mgr = get_pool_manager()
    with pool_mgr.get_connection(connection_id, conninfo) as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
"""

from __future__ import annotations

import atexit
import logging
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Optional

log = logging.getLogger(__name__)

# Check psycopg version and availability
try:
    import psycopg
    from psycopg_pool import ConnectionPool
    POOL_AVAILABLE = True
except ImportError:
    POOL_AVAILABLE = False
    log.warning(
        "psycopg_pool not available. Connection pooling disabled. "
        "Install with: pip install 'psycopg[pool]'"
    )


@dataclass
class PoolStats:
    """Statistics for a connection pool."""
    connection_id: str
    pool_size: int          # Current total connections
    idle_count: int         # Idle connections available
    active_count: int       # Connections in use
    waiting_count: int      # Clients waiting for connection
    min_size: int
    max_size: int
    timeout: float
    created_at: float
    total_requests: int     # Total connection requests


class PostgreSQLConnectionPool:
    """Wrapper around psycopg ConnectionPool for a single connection_id.
    
    Maintains a pool of connections with automatic reconnection on failure.
    """
    
    def __init__(
        self,
        connection_id: str,
        conninfo: str,
        min_size: int = 2,
        max_size: int = 10,
        timeout: float = 30.0,
        max_idle: float = 600.0,
        max_lifetime: float = 3600.0,
    ):
        """Initialize connection pool.
        
        Args:
            connection_id: Unique identifier for this pool.
            conninfo: libpq connection string.
            min_size: Minimum connections to maintain.
            max_size: Maximum connections in pool.
            timeout: Seconds to wait for available connection.
            max_idle: Close idle connections after this many seconds.
            max_lifetime: Recycle connections after this many seconds.
        """
        if not POOL_AVAILABLE:
            raise RuntimeError(
                "psycopg_pool is required for connection pooling. "
                "Install with: pip install 'psycopg[pool]'"
            )
        
        self.connection_id = connection_id
        self.conninfo = conninfo
        self.min_size = min_size
        self.max_size = max_size
        self.timeout = timeout
        self.max_idle = max_idle
        self.max_lifetime = max_lifetime
        self.created_at = time.time()
        self.total_requests = 0
        self._lock = threading.Lock()
        
        # Create the pool
        try:
            self._pool = ConnectionPool(
                conninfo=conninfo,
                min_size=min_size,
                max_size=max_size,
                timeout=timeout,
                max_idle=max_idle,
                max_lifetime=max_lifetime,
                open=True,  # Open connections immediately
            )
            log.info(
                "PostgreSQL pool created: %s (min=%d, max=%d)",
                connection_id, min_size, max_size
            )
        except Exception as exc:
            log.error("Failed to create connection pool for %s: %s", connection_id, exc)
            raise
    
    @contextmanager
    def connection(self):
        """Get a connection from the pool (context manager).
        
        Yields a psycopg connection that is automatically returned to the
        pool when the context exits.
        
        Example:
            with pool.connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
        """
        with self._lock:
            self.total_requests += 1
        
        try:
            with self._pool.connection() as conn:
                yield conn
        except Exception as exc:
            log.error("Pool connection error for %s: %s", self.connection_id, exc)
            raise
    
    def get_stats(self) -> PoolStats:
        """Get current pool statistics."""
        pool_stats = self._pool.get_stats()
        
        return PoolStats(
            connection_id=self.connection_id,
            pool_size=pool_stats.get("pool_size", 0),
            idle_count=pool_stats.get("pool_available", 0),
            active_count=pool_stats.get("pool_size", 0) - pool_stats.get("pool_available", 0),
            waiting_count=pool_stats.get("requests_waiting", 0),
            min_size=self.min_size,
            max_size=self.max_size,
            timeout=self.timeout,
            created_at=self.created_at,
            total_requests=self.total_requests,
        )
    
    def close(self):
        """Close all connections in the pool."""
        try:
            self._pool.close()
            log.info("Connection pool closed: %s", self.connection_id)
        except Exception as exc:
            log.warning("Error closing pool %s: %s", self.connection_id, exc)


class PoolManager:
    """Singleton manager for all PostgreSQL connection pools.
    
    Maintains one pool per connection_id. Pools are created lazily on first
    connection request and reused for subsequent requests.
    """
    
    def __init__(self):
        self._pools: dict[str, PostgreSQLConnectionPool] = {}
        self._lock = threading.Lock()
        self._closed = False
    
    def get_pool(
        self,
        connection_id: str,
        conninfo: str,
        pool_config: Optional[dict] = None,
    ) -> PostgreSQLConnectionPool:
        """Get or create a connection pool for the given connection_id.
        
        Args:
            connection_id: Unique identifier for this connection.
            conninfo: libpq connection string.
            pool_config: Optional pool configuration overrides.
        
        Returns:
            PostgreSQLConnectionPool instance.
        """
        if self._closed:
            raise RuntimeError("PoolManager has been closed")
        
        # Check if pool already exists
        with self._lock:
            if connection_id in self._pools:
                return self._pools[connection_id]
        
        # Extract pool config
        cfg = pool_config or {}
        min_size = cfg.get("pool_min_size", 2)
        max_size = cfg.get("pool_max_size", 10)
        timeout = cfg.get("pool_timeout", 30.0)
        max_idle = cfg.get("pool_max_idle", 600.0)
        max_lifetime = cfg.get("pool_max_lifetime", 3600.0)
        
        # Create new pool
        pool = PostgreSQLConnectionPool(
            connection_id=connection_id,
            conninfo=conninfo,
            min_size=min_size,
            max_size=max_size,
            timeout=timeout,
            max_idle=max_idle,
            max_lifetime=max_lifetime,
        )
        
        # Store it
        with self._lock:
            # Double-check another thread didn't create it
            if connection_id not in self._pools:
                self._pools[connection_id] = pool
            return self._pools[connection_id]
    
    @contextmanager
    def get_connection(
        self,
        connection_id: str,
        conninfo: str,
        pool_config: Optional[dict] = None,
    ):
        """Get a connection from the pool (context manager).
        
        Automatically creates the pool if it doesn't exist yet.
        
        Args:
            connection_id: Unique identifier for this connection.
            conninfo: libpq connection string.
            pool_config: Optional pool configuration overrides.
        
        Yields:
            psycopg connection from the pool.
        
        Example:
            with pool_mgr.get_connection("my_pg", conninfo) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
        """
        pool = self.get_pool(connection_id, conninfo, pool_config)
        with pool.connection() as conn:
            yield conn
    
    def get_stats(self, connection_id: Optional[str] = None) -> list[PoolStats]:
        """Get statistics for one or all pools.
        
        Args:
            connection_id: Optional ID to get stats for one pool.
                          If None, returns stats for all pools.
        
        Returns:
            List of PoolStats objects.
        """
        with self._lock:
            if connection_id:
                pool = self._pools.get(connection_id)
                return [pool.get_stats()] if pool else []
            else:
                return [pool.get_stats() for pool in self._pools.values()]
    
    def close_pool(self, connection_id: str) -> bool:
        """Close and remove a specific pool.
        
        Args:
            connection_id: ID of the pool to close.
        
        Returns:
            True if pool was found and closed, False otherwise.
        """
        with self._lock:
            pool = self._pools.pop(connection_id, None)
            if pool:
                pool.close()
                return True
            return False
    
    def close_all(self):
        """Close all connection pools."""
        with self._lock:
            if self._closed:
                return
            
            self._closed = True
            for pool in self._pools.values():
                pool.close()
            self._pools.clear()
            log.info("All connection pools closed")


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

_pool_manager: Optional[PoolManager] = None
_manager_lock = threading.Lock()


def get_pool_manager() -> PoolManager:
    """Get the singleton PoolManager instance.
    
    Creates the manager on first call. Thread-safe.
    """
    global _pool_manager
    
    if _pool_manager is None:
        with _manager_lock:
            if _pool_manager is None:
                _pool_manager = PoolManager()
                # Register cleanup on exit
                atexit.register(_cleanup_pools)
    
    return _pool_manager


def _cleanup_pools():
    """Cleanup function called on program exit."""
    global _pool_manager
    if _pool_manager:
        log.debug("Cleaning up connection pools on exit")
        _pool_manager.close_all()


# Fallback: direct connection without pooling
@contextmanager
def direct_connection(conninfo: str):
    """Get a direct psycopg connection without pooling.
    
    Used as fallback when pool is unavailable or for one-off connections.
    
    Args:
        conninfo: libpq connection string.
    
    Yields:
        psycopg connection (auto-closed on exit).
    """
    try:
        import psycopg
    except ImportError:
        raise RuntimeError(
            "psycopg is required for PostgreSQL connections. "
            "Install with: pip install 'psycopg[binary]'"
        )
    
    conn = psycopg.connect(conninfo, autocommit=True)
    try:
        yield conn
    finally:
        conn.close()
