"""
Remote data source connectors — S3, MinIO, ADLS, GCS, Snowflake, PostgreSQL.

Provides URI-based routing, credential management, and DuckDB-backed
remote data access.  The existing local-file pipeline is untouched;
remote sources enter at the RawColumnData level.

Public API:
    parse_uri(uri)          → SourceDescriptor
    is_remote_uri(uri)      → bool
    resolve_source(path)    → Path | SourceDescriptor
    ConnectionManager       — in-memory credential store
    registry                — ConnectorRegistry singleton
"""

from file_profiler.connectors.uri_parser import is_remote_uri, parse_uri
from file_profiler.connectors.base import (
    BaseConnector,
    RemoteObject,
    SourceDescriptor,
)
from file_profiler.connectors.connection_manager import ConnectionManager
from file_profiler.connectors.registry import registry

__all__ = [
    "BaseConnector",
    "ConnectionManager",
    "RemoteObject",
    "SourceDescriptor",
    "is_remote_uri",
    "parse_uri",
    "registry",
]
