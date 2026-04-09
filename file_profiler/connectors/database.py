"""
Remote database connector — PostgreSQL and Snowflake.

PostgreSQL: uses DuckDB ``postgres_scanner`` extension for counting and
sampling.  Falls back to ``psycopg`` if the extension is unavailable.

Snowflake: always uses the native ``snowflake-connector-python`` SDK
because DuckDB's Snowflake support is experimental and unreliable.
"""

from __future__ import annotations

import logging
import os
import re
from pathlib import Path
from typing import Optional

from file_profiler.connectors.base import (
    BaseConnector,
    ConnectorError,
    RemoteObject,
    SourceDescriptor,
)

log = logging.getLogger(__name__)

# Default timeouts (seconds) for PostgreSQL connections
_PG_CONNECT_TIMEOUT = 15
_PG_STATEMENT_TIMEOUT_MS = 120_000  # 2 minutes

# Snowflake identifier quoting — allow only safe characters
_SF_IDENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_$.]*$")


def _escape_libpq_value(value: str) -> str:
    """Escape a value for use in a libpq key=value connection string.

    Per the libpq docs, values containing spaces, backslashes or
    single-quotes must be single-quoted, with internal backslashes
    and single-quotes escaped by preceding backslash.
    """
    if not value:
        return value
    needs_quoting = any(ch in value for ch in (" ", "'", "\\", "="))
    if not needs_quoting:
        return value
    escaped = value.replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def _escape_sql_string(value: str) -> str:
    """Escape a value for embedding inside a DuckDB SQL single-quoted string.

    DuckDB (like standard SQL) uses doubled single-quotes to represent
    a literal single-quote inside a string: ``'it''s'``.
    """
    return value.replace("'", "''")


def _quote_snowflake_identifier(name: str) -> str:
    """Quote a Snowflake identifier to prevent SQL injection.

    If the identifier is a simple name (alphanumeric + underscore),
    return it as-is.  Otherwise, double-quote it with internal
    double-quotes escaped.
    """
    if _SF_IDENT_RE.match(name):
        return name
    escaped = name.replace('"', '""')
    return f'"{escaped}"'


def _read_pgpass(host: str, port: str, database: str, user: str) -> Optional[str]:
    """Read password from .pgpass file if it exists.
    
    .pgpass format (one entry per line):
        hostname:port:database:username:password
    
    Wildcards (*) match any value. Lines starting with # are comments.
    
    File locations:
        - Unix/Linux/Mac: ~/.pgpass (mode 0600 required)
        - Windows: %APPDATA%\\postgresql\\pgpass.conf
    
    Args:
        host: Hostname to match
        port: Port to match (as string)
        database: Database name to match
        user: Username to match
    
    Returns:
        Password if found, None otherwise
    """
    # Determine pgpass file location
    if os.name == 'nt':  # Windows
        appdata = os.getenv('APPDATA')
        if not appdata:
            return None
        pgpass_path = Path(appdata) / 'postgresql' / 'pgpass.conf'
    else:  # Unix/Linux/Mac
        home = os.getenv('HOME')
        if not home:
            return None
        pgpass_path = Path(home) / '.pgpass'
    
    if not pgpass_path.exists():
        return None
    
    # On Unix, verify file permissions (should be 0600)
    if os.name != 'nt':
        try:
            stat_info = pgpass_path.stat()
            mode = stat_info.st_mode & 0o777
            if mode != 0o600:
                log.warning(
                    ".pgpass file has incorrect permissions (%o). "
                    "Should be 0600. Ignoring file.", mode
                )
                return None
        except Exception as exc:
            log.debug("Could not check .pgpass permissions: %s", exc)
            return None
    
    # Read and parse the file
    try:
        with open(pgpass_path, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                
                parts = line.split(':', maxsplit=4)
                if len(parts) != 5:
                    continue
                
                file_host, file_port, file_db, file_user, file_pass = parts
                
                # Match with wildcards (*) support
                if (file_host in (host, '*') and
                    file_port in (port, '*') and
                    file_db in (database, '*') and
                    file_user in (user, '*')):
                    return file_pass
    except Exception as exc:
        log.debug("Error reading .pgpass file: %s", exc)
    
    return None


class DatabaseConnector(BaseConnector):
    """Connector for PostgreSQL and Snowflake remote databases.

    Stateless — credentials are passed per-call from ConnectionManager.
    
    PostgreSQL Authentication Methods:
    ----------------------------------
    PostgreSQL supports multiple authentication methods. The method used is
    determined by the server's pg_hba.conf configuration. This connector
    supports all standard authentication methods:
    
    1. **password**: Clear-text password (not recommended, use scram-sha-256)
    2. **md5**: MD5-hashed password authentication (legacy, less secure)
    3. **scram-sha-256**: SCRAM-SHA-256 challenge-response (recommended)
    4. **trust**: No authentication required (local development only)
    5. **peer**: System user authentication (Unix sockets only)
    6. **cert**: SSL client certificate authentication (requires sslcert/sslkey)
    7. **gss/sspi**: Kerberos/Windows authentication
    
    The client (psycopg) automatically negotiates the appropriate method
    based on server requirements. To use certificate-based authentication,
    provide sslcert and sslkey in credentials or URI parameters.
    
    Connection Pooling:
    -------------------
    Connections are pooled by default for performance. Pool configuration:
        - pool_min_size: Minimum connections (default: 2)
        - pool_max_size: Maximum connections (default: 10)
        - pool_timeout: Connection wait timeout (default: 30s)
        - pool_max_idle: Idle connection lifetime (default: 600s)
        - pool_max_lifetime: Connection recycle time (default: 3600s)
        - use_pooling: Enable/disable pooling (default: True)
    
    To disable pooling for a specific connection, set use_pooling=False
    in the credentials dict.
    
    SSL/TLS Configuration:
    ----------------------
    SSL is enabled by default with sslmode='prefer' (try SSL, fall back).
    For production use, set sslmode='require' or 'verify-full':
        - disable: No SSL
        - allow: Use SSL if server supports it
        - prefer: Try SSL first, fall back to unencrypted (default)
        - require: Require SSL, fail if unavailable
        - verify-ca: Require SSL and verify server certificate
        - verify-full: Require SSL, verify server cert and hostname
    
    For certificate-based auth or verification:
        - sslcert: Path to client certificate file (.crt)
        - sslkey: Path to client private key file (.key)
        - sslrootcert: Path to CA root certificate for server verification
        - sslcrl: Path to certificate revocation list (optional)
    
    Password Resolution Priority:
    -----------------------------
    Passwords are resolved in the following order:
        1. Explicit credentials dict (from ConnectionManager)
        2. URI password (postgresql://user:pass@host/db)
        3. Keyword/value connstring (password=secret)
        4. Environment variables (PROFILER_PG_PASSWORD or PGPASSWORD)
        5. .pgpass file (~/.pgpass or %APPDATA%\\postgresql\\pgpass.conf)
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
        # Escape for SQL string literals — conninfo may contain libpq-quoted
        # values with single-quotes that would break the outer SQL string.
        return (
            f"postgres_scan('{_escape_sql_string(conninfo)}', "
            f"'{_escape_sql_string(schema)}', '{_escape_sql_string(table)}')"
        )

    # -------------------------------------------------------------------
    # PostgreSQL implementation
    # -------------------------------------------------------------------

    def _pg_conninfo(
        self,
        descriptor: SourceDescriptor,
        credentials: dict | None = None,
    ) -> str:
        """Build a libpq connection string from descriptor + credentials.

        Supports SSL/TLS parameters, connection timeouts, and custom options.
        Merges credentials from ConnectionManager and descriptor.params (URI query string).

        SSL parameters:
            - sslmode: disable, allow, prefer (default), require, verify-ca, verify-full
            - sslcert: path to client certificate file
            - sslkey: path to client private key file
            - sslrootcert: path to root certificate file
            - sslcrl: path to certificate revocation list file

        Args:
            descriptor:  Parsed source descriptor (contains URI params).
            credentials: Pre-resolved credentials dict.  When *None*,
                         credentials are resolved from ConnectionManager.
        """
        if credentials is None:
            from file_profiler.connectors.connection_manager import get_connection_manager
            credentials = get_connection_manager().resolve_credentials(descriptor)

        # If a full connection string is provided, use it directly
        if credentials.get("connection_string"):
            return credentials["connection_string"]

        # Merge credentials with descriptor.params (URI query string parameters)
        # Priority: credentials dict > descriptor.params > defaults
        merged = {}
        
        # Extract host and port from descriptor
        host, _, port = descriptor.bucket_or_host.partition(":")
        port = port or "5432"
        
        # Basic connection parameters
        merged["host"] = credentials.get("host") or descriptor.params.get("host") or host
        merged["port"] = credentials.get("port") or descriptor.params.get("port") or port
        merged["dbname"] = credentials.get("dbname") or descriptor.database or ""
        
        # Authentication
        merged["user"] = (
            credentials.get("user") 
            or descriptor.params.get("user") 
            or ""
        )
        merged["password"] = (
            credentials.get("password") 
            or descriptor.params.get("password") 
            or ""
        )
        
        # Fallback to .pgpass file if no password provided
        if not merged["password"] and merged["user"]:
            pgpass_password = _read_pgpass(
                host=merged["host"],
                port=str(merged["port"]),
                database=merged["dbname"],
                user=merged["user"],
            )
            if pgpass_password:
                merged["password"] = pgpass_password
                log.debug("Using password from .pgpass file for user %s", merged["user"])
        
        # SSL/TLS parameters
        # Default to 'prefer' for security (try SSL, fall back to unencrypted)
        merged["sslmode"] = (
            credentials.get("sslmode") 
            or descriptor.params.get("sslmode") 
            or "prefer"
        )
        
        # Client certificates for SSL authentication
        if credentials.get("sslcert") or descriptor.params.get("sslcert"):
            merged["sslcert"] = credentials.get("sslcert") or descriptor.params.get("sslcert")
        if credentials.get("sslkey") or descriptor.params.get("sslkey"):
            merged["sslkey"] = credentials.get("sslkey") or descriptor.params.get("sslkey")
        if credentials.get("sslrootcert") or descriptor.params.get("sslrootcert"):
            merged["sslrootcert"] = credentials.get("sslrootcert") or descriptor.params.get("sslrootcert")
        if credentials.get("sslcrl") or descriptor.params.get("sslcrl"):
            merged["sslcrl"] = credentials.get("sslcrl") or descriptor.params.get("sslcrl")
        
        # Connection timeouts
        merged["connect_timeout"] = str(
            credentials.get("connect_timeout") 
            or descriptor.params.get("connect_timeout")
            or _PG_CONNECT_TIMEOUT
        )
        
        # Application name for connection tracking
        merged["application_name"] = (
            credentials.get("application_name")
            or descriptor.params.get("application_name")
            or "file_profiler"
        )
        
        # Server-side options (e.g., statement_timeout)
        options = credentials.get("options") or descriptor.params.get("options") or ""
        if options:
            merged["options"] = options
        else:
            # Default: set statement timeout to prevent runaway queries
            merged["options"] = f"-c statement_timeout={_PG_STATEMENT_TIMEOUT_MS}"
        
        # Build connection string, escaping all values for libpq safety
        # Only include non-empty values
        return " ".join(
            f"{k}={_escape_libpq_value(str(v))}" for k, v in merged.items() if v
        )

    def _test_postgresql(
        self,
        descriptor: SourceDescriptor,
        credentials: dict,
    ) -> bool:
        """Test PostgreSQL connection via psycopg with pooling support.
        
        Uses connection pooling if available, otherwise falls back to
        direct connection.
        """
        conninfo = self._pg_conninfo(descriptor, credentials)
        
        try:
            # Try using connection pool if available
            from file_profiler.connectors.connection_pool import (
                get_pool_manager,
                POOL_AVAILABLE,
            )
            
            if POOL_AVAILABLE and credentials.get("use_pooling", True):
                # Use pooled connection
                pool_mgr = get_pool_manager()
                connection_id = descriptor.connection_id or "test_connection"
                with pool_mgr.get_connection(connection_id, conninfo, credentials) as conn:
                    conn.execute("SELECT 1")
                return True
        except ImportError:
            pass  # Fall through to direct connection
        
        # Fallback: direct connection without pooling
        try:
            import psycopg
        except ImportError:
            raise ConnectorError(
                "psycopg is required for PostgreSQL connection testing. "
                "Install it with: pip install 'psycopg[binary]'"
            )
        
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
        """List tables in a PostgreSQL database with pooling support."""
        conninfo = self._pg_conninfo(descriptor, credentials)
        schema = descriptor.schema_name or "public"
        
        try:
            # Try using connection pool if available
            from file_profiler.connectors.connection_pool import (
                get_pool_manager,
                POOL_AVAILABLE,
            )
            
            if POOL_AVAILABLE and credentials.get("use_pooling", True):
                # Use pooled connection
                pool_mgr = get_pool_manager()
                connection_id = descriptor.connection_id or "list_tables"
                with pool_mgr.get_connection(connection_id, conninfo, credentials) as conn:
                    rows = conn.execute(
                        "SELECT table_name FROM information_schema.tables "
                        "WHERE table_schema = %s AND table_type = 'BASE TABLE' "
                        "ORDER BY table_name",
                        (schema,),
                    ).fetchall()
                    return [
                        RemoteObject(
                            name=row[0],
                            uri=row[0],  # table name used as identifier
                            file_format="postgresql",
                        )
                        for row in rows
                    ]
        except ImportError:
            pass  # Fall through to direct connection
        
        # Fallback: direct connection without pooling
        try:
            import psycopg
        except ImportError:
            raise ConnectorError(
                "psycopg is required for PostgreSQL table listing. "
                "Install it with: pip install 'psycopg[binary]'"
            )

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

    def list_schemas(
        self,
        descriptor: SourceDescriptor,
        credentials: dict,
    ) -> list[str]:
        """List schemas in the database."""
        if self.db_type == "postgresql":
            return self._list_schemas_postgresql(descriptor, credentials)
        else:
            return self._list_schemas_snowflake(descriptor, credentials)

    def _list_schemas_postgresql(
        self,
        descriptor: SourceDescriptor,
        credentials: dict,
    ) -> list[str]:
        """List schemas in a PostgreSQL database with pooling support."""
        conninfo = self._pg_conninfo(descriptor, credentials)
        
        try:
            # Try using connection pool if available
            from file_profiler.connectors.connection_pool import (
                get_pool_manager,
                POOL_AVAILABLE,
            )
            
            if POOL_AVAILABLE and credentials.get("use_pooling", True):
                # Use pooled connection
                pool_mgr = get_pool_manager()
                connection_id = descriptor.connection_id or "list_schemas"
                with pool_mgr.get_connection(connection_id, conninfo, credentials) as conn:
                    rows = conn.execute(
                        "SELECT schema_name FROM information_schema.schemata "
                        "WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast') "
                        "ORDER BY schema_name",
                    ).fetchall()
                    return [row[0] for row in rows]
        except ImportError:
            pass  # Fall through to direct connection
        
        # Fallback: direct connection without pooling
        try:
            import psycopg
        except ImportError:
            raise ConnectorError(
                "psycopg is required for PostgreSQL schema listing. "
                "Install it with: pip install 'psycopg[binary]'"
            )
        
        try:
            with psycopg.connect(conninfo, autocommit=True) as conn:
                rows = conn.execute(
                    "SELECT schema_name FROM information_schema.schemata "
                    "WHERE schema_name NOT IN ('pg_catalog', 'information_schema', 'pg_toast') "
                    "ORDER BY schema_name",
                ).fetchall()
        except Exception as exc:
            raise ConnectorError(f"Failed to list PostgreSQL schemas: {exc}") from exc

        return [row[0] for row in rows]

    def _list_schemas_snowflake(
        self,
        descriptor: SourceDescriptor,
        credentials: dict,
    ) -> list[str]:
        """List schemas in a Snowflake database."""
        con = self._snowflake_connect(descriptor, credentials)
        try:
            cursor = con.cursor()
            if descriptor.database:
                cursor.execute(
                    f"USE DATABASE {_quote_snowflake_identifier(descriptor.database)}"
                )
            cursor.execute("SHOW SCHEMAS")
            return [row[1] for row in cursor.fetchall()]
        except Exception as exc:
            raise ConnectorError(f"Failed to list Snowflake schemas: {exc}") from exc
        finally:
            con.close()

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

            # Set context — identifiers are quoted to prevent injection
            if descriptor.database:
                cursor.execute(
                    f"USE DATABASE {_quote_snowflake_identifier(descriptor.database)}"
                )
            if descriptor.schema_name:
                cursor.execute(
                    f"USE SCHEMA {_quote_snowflake_identifier(descriptor.schema_name)}"
                )

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
                cursor.execute(
                    f"USE DATABASE {_quote_snowflake_identifier(descriptor.database)}"
                )
            if descriptor.schema_name:
                cursor.execute(
                    f"USE SCHEMA {_quote_snowflake_identifier(descriptor.schema_name)}"
                )

            # Row count — quoted identifier prevents injection
            safe_table = _quote_snowflake_identifier(table_name)
            cursor.execute(f"SELECT COUNT(*) FROM {safe_table}")
            row_count = cursor.fetchone()[0]

            # Sample
            cursor.execute(
                f"SELECT * FROM {safe_table} SAMPLE ({int(sample_size)} ROWS)"
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
                cursor.execute(
                    f"USE DATABASE {_quote_snowflake_identifier(descriptor.database)}"
                )

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
