"""
Connector registry — maps URI schemes to connector instances.

Connectors are registered lazily at first use to avoid importing
heavy SDKs (boto3, azure, snowflake) when only local profiling is needed.

Usage:
    from file_profiler.connectors.registry import registry

    connector = registry.get("s3")
    connector.test_connection(descriptor, credentials)
"""

from __future__ import annotations

import logging
from typing import Optional

from file_profiler.connectors.base import BaseConnector, ConnectorError

log = logging.getLogger(__name__)


class ConnectorRegistry:
    """Maps URI schemes to BaseConnector instances.

    Supports both eager registration (for testing / custom connectors)
    and lazy registration via factory callables that are invoked on
    first access.
    """

    def __init__(self) -> None:
        self._connectors: dict[str, BaseConnector] = {}
        self._factories: dict[str, callable] = {}
        self._registered = False

    def register(self, scheme: str, connector: BaseConnector) -> None:
        """Register a connector instance for a scheme."""
        self._connectors[scheme.lower()] = connector

    def register_lazy(self, scheme: str, factory: callable) -> None:
        """Register a factory that creates the connector on first access.

        The factory is called with no arguments and must return a
        BaseConnector instance.
        """
        self._factories[scheme.lower()] = factory

    def get(self, scheme: str) -> BaseConnector:
        """Get the connector for a scheme.

        Raises ConnectorError if no connector is registered.
        """
        key = scheme.lower()
        if key == "postgres":
            key = "postgresql"

        # Already instantiated
        if key in self._connectors:
            return self._connectors[key]

        # Lazy instantiation
        if key in self._factories:
            try:
                connector = self._factories[key]()
                self._connectors[key] = connector
                del self._factories[key]
                return connector
            except Exception as exc:
                raise ConnectorError(
                    f"Failed to initialize connector for '{key}': {exc}"
                ) from exc

        # Auto-register built-in connectors on first miss
        if not self._registered:
            self._register_builtins()
            self._registered = True
            if key in self._connectors or key in self._factories:
                return self.get(key)

        raise ConnectorError(
            f"No connector registered for scheme '{scheme}'. "
            f"Available: {', '.join(self.supported_schemes)}"
        )

    @property
    def supported_schemes(self) -> list[str]:
        """List all registered scheme names."""
        return sorted(set(self._connectors) | set(self._factories))

    def supports(self, scheme: str) -> bool:
        key = scheme.lower()
        if key == "postgres":
            key = "postgresql"
        return key in self._connectors or key in self._factories

    def _register_builtins(self) -> None:
        """Register built-in connectors using lazy factories.

        Each factory imports its module only when first accessed,
        keeping startup fast for local-only usage.
        """
        def _cloud(provider):
            def factory():
                from file_profiler.connectors.cloud_storage import CloudStorageConnector
                return CloudStorageConnector(provider)
            return factory

        def _database(db_type):
            def factory():
                from file_profiler.connectors.database import DatabaseConnector
                return DatabaseConnector(db_type)
            return factory

        self.register_lazy("s3", _cloud("s3"))
        self.register_lazy("minio", _cloud("minio"))
        self.register_lazy("gs", _cloud("gcs"))
        self.register_lazy("abfss", _cloud("adls"))
        self.register_lazy("snowflake", _database("snowflake"))
        self.register_lazy("postgresql", _database("postgresql"))

        log.debug("Built-in connectors registered (lazy): %s", self.supported_schemes)


# Module-level singleton
registry = ConnectorRegistry()
