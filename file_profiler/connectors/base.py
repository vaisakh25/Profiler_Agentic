"""
Core abstractions for remote data source connectors.

SourceDescriptor  — parsed URI representation
RemoteObject      — a file or table discovered at a remote location
BaseConnector     — interface every connector implements
ConnectorError    — raised on connection / auth failures
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional

import duckdb


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------

class ConnectorError(Exception):
    """Raised when a connector operation fails (auth, network, config)."""


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class SourceDescriptor:
    """Parsed representation of a remote URI or local path.

    Produced by ``uri_parser.parse_uri()``.  Carries enough information
    for a connector to reach the data without re-parsing the URI.
    """
    scheme: str                         # "s3", "abfss", "gs", "snowflake", "postgresql", "file"
    bucket_or_host: str                 # bucket name, storage account, or host:port
    path: str                           # object key, prefix, or /database/schema/table
    raw_uri: str                        # original URI string (for logging / display)
    connection_id: Optional[str] = None # reference to stored credentials

    # Database-specific fields (parsed from URI)
    database: Optional[str] = None
    schema_name: Optional[str] = None   # "schema" is a builtin, avoid shadowing
    table_name: Optional[str] = None

    # Query params (e.g. ?warehouse=COMPUTE_WH)
    params: dict = field(default_factory=dict)

    @property
    def is_remote(self) -> bool:
        return self.scheme != "file"

    @property
    def is_object_storage(self) -> bool:
        return self.scheme in ("s3", "abfss", "gs")

    @property
    def is_database(self) -> bool:
        return self.scheme in ("snowflake", "postgresql")

    @property
    def is_directory_like(self) -> bool:
        """True if the path is a prefix / directory (not a single file)."""
        if self.is_database:
            # No specific table → list all tables
            return not self.table_name
        # Object storage: ends with / or has no file extension
        if self.path.endswith("/"):
            return True
        last_segment = self.path.rsplit("/", 1)[-1] if "/" in self.path else self.path
        return "." not in last_segment

    @property
    def display_name(self) -> str:
        """Short human-readable label for UI display."""
        if self.is_database:
            parts = [self.scheme, self.database or ""]
            if self.schema_name:
                parts.append(self.schema_name)
            if self.table_name:
                parts.append(self.table_name)
            return "://".join(parts[:1]) + "/" + "/".join(parts[1:])
        return self.raw_uri


@dataclass
class RemoteObject:
    """A file or table discovered at a remote location."""
    name: str                           # filename or table name
    uri: str                            # full URI to this object
    size_bytes: Optional[int] = None    # None for database tables
    file_format: Optional[str] = None   # "parquet", "csv", etc.  None for databases


# ---------------------------------------------------------------------------
# Connector interface
# ---------------------------------------------------------------------------

class BaseConnector(ABC):
    """Interface that all source connectors implement.

    Each connector knows how to:
    - Test connectivity with given credentials
    - Configure a DuckDB connection with the right extensions and auth
    - List objects (files or tables) at a location
    - Produce a DuckDB SQL expression to read data

    Connectors are stateless — credentials are passed per-call from
    the ConnectionManager.
    """

    @abstractmethod
    def test_connection(
        self,
        descriptor: SourceDescriptor,
        credentials: dict,
    ) -> bool:
        """Validate credentials and reachability.

        Returns True on success, raises ConnectorError on failure.
        """

    @abstractmethod
    def configure_duckdb(
        self,
        con: duckdb.DuckDBPyConnection,
        descriptor: SourceDescriptor,
        credentials: dict,
    ) -> None:
        """Install/load DuckDB extensions and SET credential parameters.

        Called once per DuckDB connection before any scan queries.
        """

    @abstractmethod
    def list_objects(
        self,
        descriptor: SourceDescriptor,
        credentials: dict,
    ) -> list[RemoteObject]:
        """List files (object storage) or tables (database) at the path.

        For object storage: lists objects under the prefix.
        For databases: lists tables in the schema/database.
        """

    @abstractmethod
    def duckdb_scan_expression(
        self,
        descriptor: SourceDescriptor,
        object_uri: Optional[str] = None,
    ) -> str:
        """Return the DuckDB SQL expression to read from this source.

        Examples:
            "read_parquet('s3://bucket/path/file.parquet')"
            "postgres_scan('host=... dbname=...', 'public', 'users')"

        Args:
            descriptor: The parsed source descriptor.
            object_uri: Override URI for a specific object within a
                        directory listing.  If None, uses descriptor.raw_uri.
        """

    def list_schemas(
        self,
        descriptor: SourceDescriptor,
        credentials: dict,
    ) -> list[str]:
        """List available schemas in a database.

        Only applicable to database connectors. Object storage connectors
        should leave the default implementation which raises NotImplementedError.
        """
        raise NotImplementedError(
            f"{type(self).__name__} does not support listing schemas"
        )

    def supports_duckdb(self, descriptor: SourceDescriptor) -> bool:
        """Whether DuckDB can handle this source directly.

        Override to return False for sources that require native SDKs
        (e.g. Snowflake).  Default is True.
        """
        return True
