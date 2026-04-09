"""
Connection Manager — credential store for remote data sources.

Credentials flow directly from the UI to REST endpoints, never through
the LLM.  They are encrypted at rest using Fernet (PROFILER_SECRET_KEY)
and optionally persisted to disk.

Resolution priority:
    1. Explicit ``connection_id`` → stored credentials
    2. Environment variables → scheme-specific defaults
    3. SDK default chains (e.g. boto3 credential chain, ADC for GCS)
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from typing import Optional

from file_profiler.connectors.base import ConnectorError, SourceDescriptor

log = logging.getLogger(__name__)


@dataclass
class ConnectionInfo:
    """Full credential bundle for a registered connection."""
    connection_id: str
    scheme: str
    credentials: dict           # plaintext in memory, encrypted on disk
    display_name: str = ""
    created_at: float = field(default_factory=time.time)
    last_tested: Optional[float] = None
    is_healthy: Optional[bool] = None


@dataclass
class ConnectionSummary:
    """Safe-to-serialize view of a connection (no secrets)."""
    connection_id: str
    scheme: str
    display_name: str
    created_at: float
    last_tested: Optional[float]
    is_healthy: Optional[bool]


@dataclass
class TestResult:
    """Result of a connection test."""
    success: bool
    message: str
    latency_ms: float = 0.0


class ConnectionManager:
    """Credential store for remote data sources.

    Credentials are held in plaintext in memory for active use, but
    encrypted when persisted to disk.  The LLM never sees credentials —
    they flow directly from the frontend to REST endpoints.
    """

    def __init__(self) -> None:
        self._connections: dict[str, ConnectionInfo] = {}
        self._load_persisted()

    def _load_persisted(self) -> None:
        """Load previously persisted connections from encrypted storage."""
        try:
            from file_profiler.connectors.credential_store import get_credential_store
            store = get_credential_store()
            if not store.persistence_enabled:
                return
            stored = store.load_from_file()
            for cid, sc in stored.items():
                try:
                    creds = store.decrypt_credentials(sc.encrypted_credentials)
                    self._connections[cid] = ConnectionInfo(
                        connection_id=sc.connection_id,
                        scheme=sc.scheme,
                        credentials=creds,
                        display_name=sc.display_name,
                        created_at=sc.created_at,
                        last_tested=sc.last_tested,
                        is_healthy=sc.is_healthy,
                    )
                except Exception as exc:
                    log.warning("Could not decrypt connection '%s': %s", cid, exc)
        except Exception as exc:
            log.debug("No persisted connections loaded: %s", exc)

    def _persist(self) -> None:
        """Persist all connections to encrypted storage."""
        try:
            from file_profiler.connectors.credential_store import (
                StoredConnection,
                get_credential_store,
            )
            store = get_credential_store()
            if not store.persistence_enabled:
                return
            stored = {}
            for cid, info in self._connections.items():
                stored[cid] = StoredConnection(
                    connection_id=info.connection_id,
                    scheme=info.scheme,
                    display_name=info.display_name,
                    encrypted_credentials=store.encrypt_credentials(info.credentials),
                    created_at=info.created_at,
                    last_tested=info.last_tested,
                    is_healthy=info.is_healthy,
                )
            store.save_to_file(stored)
        except Exception as exc:
            log.warning("Could not persist connections: %s", exc)

    def register(
        self,
        connection_id: str,
        scheme: str,
        credentials: dict,
        display_name: str = "",
    ) -> ConnectionInfo:
        """Register (or overwrite) credentials for a connection.

        Args:
            connection_id: Unique name (e.g. "prod-s3", "analytics-pg").
            scheme: Source type: "s3", "abfss", "gs", "snowflake", "postgresql".
            credentials: Auth credentials (scheme-specific).
            display_name: Human-readable label for UI.

        Returns:
            The stored ConnectionInfo.
        """
        if not connection_id or not connection_id.strip():
            raise ConnectorError("connection_id must not be empty")

        scheme = scheme.lower()
        if scheme == "postgres":
            scheme = "postgresql"

        info = ConnectionInfo(
            connection_id=connection_id.strip(),
            scheme=scheme,
            credentials=credentials,
            display_name=display_name or connection_id,
        )
        self._connections[info.connection_id] = info
        self._persist()
        log.info(
            "Connection registered: %s (scheme=%s)",
            info.connection_id, info.scheme,
        )
        return info

    def get(self, connection_id: str) -> ConnectionInfo:
        """Retrieve a stored connection.

        Raises ConnectorError if not found.
        """
        info = self._connections.get(connection_id)
        if info is None:
            raise ConnectorError(
                f"Connection '{connection_id}' not found. "
                f"Available: {', '.join(self._connections) or '(none)'}"
            )
        return info

    def remove(self, connection_id: str) -> bool:
        """Remove a connection.  Returns True if it existed."""
        removed = self._connections.pop(connection_id, None)
        if removed:
            self._persist()
            log.info("Connection removed: %s", connection_id)
        return removed is not None

    def list_connections(self) -> list[ConnectionSummary]:
        """Return all connections as safe-to-serialize summaries."""
        return [
            ConnectionSummary(
                connection_id=info.connection_id,
                scheme=info.scheme,
                display_name=info.display_name,
                created_at=info.created_at,
                last_tested=info.last_tested,
                is_healthy=info.is_healthy,
            )
            for info in self._connections.values()
        ]

    def test(self, connection_id: str) -> TestResult:
        """Test a stored connection.

        Delegates to the appropriate connector's test_connection().
        Updates the connection's last_tested and is_healthy fields.
        """
        from file_profiler.connectors.registry import registry

        info = self.get(connection_id)
        connector = registry.get(info.scheme)

        descriptor = SourceDescriptor(
            scheme=info.scheme,
            bucket_or_host="",
            path="",
            raw_uri="",
            connection_id=connection_id,
        )

        start = time.time()
        try:
            connector.test_connection(descriptor, info.credentials)
            latency = (time.time() - start) * 1000
            info.last_tested = time.time()
            info.is_healthy = True
            self._persist()
            return TestResult(success=True, message="OK", latency_ms=latency)
        except Exception as exc:
            latency = (time.time() - start) * 1000
            info.last_tested = time.time()
            info.is_healthy = False
            self._persist()
            return TestResult(
                success=False,
                message=str(exc),
                latency_ms=latency,
            )

    def resolve_credentials(self, descriptor: SourceDescriptor) -> dict:
        """Look up credentials for a source descriptor.

        Priority:
            1. Explicit connection_id → stored credentials
            2. Environment variables → scheme-specific defaults
            3. Empty dict → let SDK default chains handle auth

        Returns:
            Credential dict (may be empty).
        """
        # 1. Stored connection
        if descriptor.connection_id:
            info = self.get(descriptor.connection_id)
            return info.credentials

        # 2. Environment variable defaults
        return _env_credentials(descriptor.scheme)

    def has_connection(self, connection_id: str) -> bool:
        return connection_id in self._connections


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------
_manager = ConnectionManager()


def get_connection_manager() -> ConnectionManager:
    """Return the module-level ConnectionManager singleton."""
    return _manager


# ---------------------------------------------------------------------------
# Env-var credential fallbacks
# ---------------------------------------------------------------------------

def _env_credentials(scheme: str) -> dict:
    """Build a credential dict from environment variables.

    Returns only keys that are actually set (non-empty).
    """
    creds: dict = {}

    if scheme == "s3":
        _add_if_set(creds, "aws_access_key_id", "AWS_ACCESS_KEY_ID")
        _add_if_set(creds, "aws_secret_access_key", "AWS_SECRET_ACCESS_KEY")
        _add_if_set(creds, "region", "AWS_DEFAULT_REGION")
        _add_if_set(creds, "profile_name", "AWS_PROFILE")

    elif scheme == "abfss":
        _add_if_set(creds, "connection_string", "AZURE_STORAGE_CONNECTION_STRING")
        _add_if_set(creds, "tenant_id", "AZURE_TENANT_ID")
        _add_if_set(creds, "client_id", "AZURE_CLIENT_ID")
        _add_if_set(creds, "client_secret", "AZURE_CLIENT_SECRET")
        _add_if_set(creds, "account_name", "AZURE_STORAGE_ACCOUNT")

    elif scheme == "gs":
        _add_if_set(creds, "service_account_json", "GOOGLE_APPLICATION_CREDENTIALS")

    elif scheme == "snowflake":
        _add_if_set(creds, "account", "SNOWFLAKE_ACCOUNT")
        _add_if_set(creds, "user", "SNOWFLAKE_USER")
        _add_if_set(creds, "password", "SNOWFLAKE_PASSWORD")
        _add_if_set(creds, "warehouse", "SNOWFLAKE_WAREHOUSE")
        _add_if_set(creds, "role", "SNOWFLAKE_ROLE")

    elif scheme == "postgresql":
        _add_if_set(creds, "connection_string", "PROFILER_PG_CONNSTRING")
        _add_if_set(creds, "host", "PROFILER_PG_HOST")
        _add_if_set(creds, "port", "PROFILER_PG_PORT")
        _add_if_set(creds, "user", "PROFILER_PG_USER")
        _add_if_set(creds, "password", "PROFILER_PG_PASSWORD")
        _add_if_set(creds, "dbname", "PROFILER_PG_DBNAME")
        # SSL/TLS parameters
        _add_if_set(creds, "sslmode", "PROFILER_PG_SSLMODE")
        _add_if_set(creds, "sslcert", "PROFILER_PG_SSLCERT")
        _add_if_set(creds, "sslkey", "PROFILER_PG_SSLKEY")
        _add_if_set(creds, "sslrootcert", "PROFILER_PG_SSLROOTCERT")
        _add_if_set(creds, "sslcrl", "PROFILER_PG_SSLCRL")
        # Fallback to standard PostgreSQL environment variables
        _add_if_set(creds, "password", "PGPASSWORD")  # PGPASSWORD overrides if set
        _add_if_set(creds, "sslmode", "PGSSLMODE")
        _add_if_set(creds, "sslcert", "PGSSLCERT")
        _add_if_set(creds, "sslkey", "PGSSLKEY")
        _add_if_set(creds, "sslrootcert", "PGSSLROOTCERT")

    return creds


def _add_if_set(creds: dict, key: str, env_var: str) -> None:
    """Add env var to creds dict only if it has a non-empty value."""
    val = os.getenv(env_var, "")
    if val:
        creds[key] = val
