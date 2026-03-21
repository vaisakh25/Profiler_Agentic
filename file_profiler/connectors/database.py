"""
Remote database connector — PostgreSQL and Snowflake.

PostgreSQL: uses DuckDB ``postgres_scanner`` extension for counting and
sampling.  Falls back to ``psycopg`` if the extension is unavailable.

Snowflake: always uses the native ``snowflake-connector-python`` SDK
because DuckDB's Snowflake support is experimental and unreliable.
"""

from __future__ import annotations

import logging
from typing import Optional

from file_profiler.connectors.base import (
    BaseConnector,
    ConnectorError,
    RemoteObject,
    SourceDescriptor,
)

log = logging.getLogger(__name__)


class DatabaseConnector(BaseConnector):
    """Connector for PostgreSQL and Snowflake remote databases.

    Stateless — credentials are passed per-call from ConnectionManager.
    """

    def __init__(self, db_type: str) -> None:
        if db_type not in ("postgresql", "snowflake"):
            raise ValueError(f"Unknown database type: {db_type}")
        self.db_type = db_type

    def supports_duckdb(self, descriptor: SourceDescriptor) -> bool:
        """PostgreSQL via postgres_scanner; Snowflake via native SDK."""
        return self.db_type == "postgresql"

    def test_connection(
        self,
        descriptor: SourceDescriptor,
        credentials: dict,
    ) -> bool:
        if self.db_type == "postgresql":
            return self._test_postgresql(descriptor, credentials)
        else:
            return self._test_snowflake(descriptor, credentials)

    def configure_duckdb(self, con, descriptor, credentials) -> None:
        if self.db_type == "postgresql":
            from file_profiler.connectors.duckdb_remote import _configure_postgres
            _configure_postgres(con)
        else:
            raise ConnectorError(
                "Snowflake does not support DuckDB access. "
                "Use the native SDK path instead."
            )

    def list_objects(
        self,
        descriptor: SourceDescriptor,
        credentials: dict,
    ) -> list[RemoteObject]:
        """List tables in the database/schema."""
        if self.db_type == "postgresql":
            return self._list_postgresql(descriptor, credentials)
        else:
            return self._list_snowflake(descriptor, credentials)

    def duckdb_scan_expression(
        self,
        descriptor: SourceDescriptor,
        object_uri: Optional[str] = None,
    ) -> str:
        """Build a DuckDB postgres_scan() expression."""
        if self.db_type != "postgresql":
            raise ConnectorError(
                "duckdb_scan_expression not supported for Snowflake"
            )
        conninfo = self._pg_conninfo(descriptor)
        schema = descriptor.schema_name or "public"
        table = descriptor.table_name or ""
        if object_uri:
            # object_uri is the table name when iterating list results
            table = object_uri
        return f"postgres_scan('{conninfo}', '{schema}', '{table}')"

    # -------------------------------------------------------------------
    # PostgreSQL implementation
    # -------------------------------------------------------------------

    def _pg_conninfo(self, descriptor: SourceDescriptor) -> str:
        """Build a libpq connection string from descriptor + credentials."""
        from file_profiler.connectors.connection_manager import get_connection_manager
        creds = get_connection_manager().resolve_credentials(descriptor)

        # If a full connection string is provided, use it directly
        if creds.get("connection_string"):
            return creds["connection_string"]

        # Build from components
        host, _, port = descriptor.bucket_or_host.partition(":")
        port = port or creds.get("port", "5432")
        parts = {
            "host": creds.get("host", host),
            "port": port,
            "user": creds.get("user", ""),
            "password": creds.get("password", ""),
            "dbname": creds.get("dbname", descriptor.database or ""),
        }
        # Only include non-empty values
        return " ".join(f"{k}={v}" for k, v in parts.items() if v)

    def _test_postgresql(
        self,
        descriptor: SourceDescriptor,
        credentials: dict,
    ) -> bool:
        """Test PostgreSQL connection via psycopg."""
        try:
            import psycopg
        except ImportError:
            raise ConnectorError(
                "psycopg is required for PostgreSQL connection testing. "
                "Install it with: pip install 'psycopg[binary]'"
            )

        conninfo = self._pg_conninfo(descriptor)
        try:
            with psycopg.connect(conninfo, autocommit=True) as conn:
                conn.execute("SELECT 1")
            return True
        except Exception as exc:
            raise ConnectorError(f"PostgreSQL connection failed: {exc}") from exc

    def _list_postgresql(
        self,
        descriptor: SourceDescriptor,
        credentials: dict,
    ) -> list[RemoteObject]:
        """List tables in a PostgreSQL database."""
        try:
            import psycopg
        except ImportError:
            raise ConnectorError(
                "psycopg is required for PostgreSQL table listing. "
                "Install it with: pip install 'psycopg[binary]'"
            )

        conninfo = self._pg_conninfo(descriptor)
        schema = descriptor.schema_name or "public"

        try:
            with psycopg.connect(conninfo, autocommit=True) as conn:
                rows = conn.execute(
                    "SELECT table_name FROM information_schema.tables "
                    "WHERE table_schema = %s AND table_type = 'BASE TABLE' "
                    "ORDER BY table_name",
                    (schema,),
                ).fetchall()
        except Exception as exc:
            raise ConnectorError(f"Failed to list PostgreSQL tables: {exc}") from exc

        return [
            RemoteObject(
                name=row[0],
                uri=row[0],  # table name used as identifier
                file_format="postgresql",
            )
            for row in rows
        ]

    # -------------------------------------------------------------------
    # Snowflake implementation
    # -------------------------------------------------------------------

    def _test_snowflake(
        self,
        descriptor: SourceDescriptor,
        credentials: dict,
    ) -> bool:
        """Test Snowflake connection via native SDK."""
        con = self._snowflake_connect(descriptor, credentials)
        try:
            con.cursor().execute("SELECT 1")
            return True
        except Exception as exc:
            raise ConnectorError(f"Snowflake connection failed: {exc}") from exc
        finally:
            con.close()

    def _list_snowflake(
        self,
        descriptor: SourceDescriptor,
        credentials: dict,
    ) -> list[RemoteObject]:
        """List tables in a Snowflake database/schema."""
        con = self._snowflake_connect(descriptor, credentials)
        try:
            cursor = con.cursor()

            # Set context
            if descriptor.database:
                cursor.execute(f"USE DATABASE {descriptor.database}")
            if descriptor.schema_name:
                cursor.execute(f"USE SCHEMA {descriptor.schema_name}")

            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()

            return [
                RemoteObject(
                    name=row[1],  # TABLE_NAME is column index 1
                    uri=row[1],
                    file_format="snowflake",
                )
                for row in tables
            ]
        except Exception as exc:
            raise ConnectorError(f"Failed to list Snowflake tables: {exc}") from exc
        finally:
            con.close()

    def snowflake_count_and_sample(
        self,
        descriptor: SourceDescriptor,
        credentials: dict,
        table_name: str,
        sample_size: int = 10_000,
    ) -> tuple[int, list[str], list[list[str]]]:
        """Count rows and sample from a Snowflake table.

        Returns:
            (row_count, column_names, rows) where rows is list of
            lists of strings.
        """
        con = self._snowflake_connect(descriptor, credentials)
        try:
            cursor = con.cursor()

            if descriptor.database:
                cursor.execute(f"USE DATABASE {descriptor.database}")
            if descriptor.schema_name:
                cursor.execute(f"USE SCHEMA {descriptor.schema_name}")

            # Row count
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cursor.fetchone()[0]

            # Sample
            cursor.execute(
                f"SELECT * FROM {table_name} SAMPLE ({sample_size} ROWS)"
            )
            headers = [desc[0] for desc in cursor.description]
            rows = [
                [str(v) if v is not None else None for v in row]
                for row in cursor.fetchall()
            ]

            return row_count, headers, rows
        except Exception as exc:
            raise ConnectorError(
                f"Snowflake count/sample failed for {table_name}: {exc}"
            ) from exc
        finally:
            con.close()

    def snowflake_schema(
        self,
        descriptor: SourceDescriptor,
        credentials: dict,
        table_name: str,
    ) -> list[tuple[str, str]]:
        """Get column names and types from Snowflake INFORMATION_SCHEMA.

        Returns:
            List of (column_name, data_type) tuples.
        """
        con = self._snowflake_connect(descriptor, credentials)
        try:
            cursor = con.cursor()

            if descriptor.database:
                cursor.execute(f"USE DATABASE {descriptor.database}")

            schema = descriptor.schema_name or "PUBLIC"
            cursor.execute(
                "SELECT column_name, data_type "
                "FROM information_schema.columns "
                "WHERE table_schema = %s AND table_name = %s "
                "ORDER BY ordinal_position",
                (schema, table_name.upper()),
            )
            return [(row[0], row[1]) for row in cursor.fetchall()]
        except Exception as exc:
            raise ConnectorError(
                f"Snowflake schema read failed for {table_name}: {exc}"
            ) from exc
        finally:
            con.close()

    def _snowflake_connect(self, descriptor, credentials):
        """Create a Snowflake connection using native SDK."""
        try:
            import snowflake.connector
        except ImportError:
            raise ConnectorError(
                "snowflake-connector-python is required for Snowflake. "
                "Install it with: pip install snowflake-connector-python"
            )

        from file_profiler.connectors.connection_manager import get_connection_manager
        creds = get_connection_manager().resolve_credentials(descriptor)

        connect_kwargs = {
            "account": creds.get("account", descriptor.bucket_or_host),
            "user": creds.get("user", ""),
            "password": creds.get("password", ""),
        }
        if creds.get("warehouse"):
            connect_kwargs["warehouse"] = creds["warehouse"]
        if creds.get("role"):
            connect_kwargs["role"] = creds["role"]
        if descriptor.database:
            connect_kwargs["database"] = descriptor.database
        if descriptor.schema_name:
            connect_kwargs["schema"] = descriptor.schema_name

        try:
            return snowflake.connector.connect(**connect_kwargs)
        except Exception as exc:
            raise ConnectorError(f"Snowflake connection failed: {exc}") from exc
