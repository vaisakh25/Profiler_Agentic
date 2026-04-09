"""
URI parser — converts user-provided strings into SourceDescriptor objects.

Handles:
    s3://bucket/key/path.parquet
    abfss://container@account.dfs.core.windows.net/path/
    gs://bucket/prefix/
    snowflake://account/database/schema/table
    snowflake://account/database/schema?warehouse=WH
    postgresql://user:pass@host:5432/dbname
    postgresql://user:pass@host:5432/dbname?table=mytable&schema=public&sslmode=require
    host=localhost port=5432 dbname=mydb user=postgres password=secret sslmode=require
    /local/path/to/file.csv   → scheme="file"
    C:\\local\\path\\file.csv  → scheme="file"
"""

from __future__ import annotations

from urllib.parse import parse_qs, urlparse

from file_profiler.connectors.base import SourceDescriptor

# Schemes we recognise as remote sources.
_REMOTE_SCHEMES = frozenset({"s3", "abfss", "gs", "snowflake", "postgresql", "postgres"})

# PostgreSQL keyword/value connection string keywords
_PG_CONNSTRING_KEYWORDS = frozenset({
    "host", "hostaddr", "port", "dbname", "user", "password",
    "connect_timeout", "client_encoding", "options", "application_name",
    "fallback_application_name", "keepalives", "keepalives_idle",
    "keepalives_interval", "keepalives_count", "tcp_user_timeout",
    "sslmode", "sslcert", "sslkey", "sslrootcert", "sslcrl", "sslcompression",
    "requiressl", "gssencmode", "krbsrvname", "gsslib", "service",
    "target_session_attrs", "channel_binding"
})


def is_remote_uri(path_or_uri: str) -> bool:
    """Quick check: does this string look like a remote URI?

    Returns True for s3://, abfss://, gs://, snowflake://, postgresql://.
    Returns False for local paths (absolute or relative).
    """
    lower = path_or_uri.strip().lower()
    return any(lower.startswith(f"{s}://") for s in _REMOTE_SCHEMES)


def parse_uri(
    uri: str,
    connection_id: str | None = None,
) -> SourceDescriptor:
    """Parse a URI string into a SourceDescriptor.

    Local paths (no scheme or ``file://``) return a descriptor with
    ``scheme="file"`` and the path set to the original string.

    PostgreSQL supports both URI and keyword/value formats:
        - URI: postgresql://user:pass@host:5432/dbname?sslmode=require
        - Keyword/value: host=localhost port=5432 dbname=mydb user=postgres

    Args:
        uri:            Raw URI or local path from the user.
        connection_id:  Optional reference to stored credentials.
    """
    stripped = uri.strip()

    # Check for PostgreSQL keyword/value format (libpq connection string)
    if _is_pg_keyword_format(stripped):
        return _parse_pg_keyword_value(stripped, connection_id)

    if not is_remote_uri(stripped):
        return SourceDescriptor(
            scheme="file",
            bucket_or_host="",
            path=stripped,
            raw_uri=stripped,
            connection_id=connection_id,
        )

    parsed = urlparse(stripped)
    scheme = parsed.scheme.lower()

    # Normalise "postgres" → "postgresql"
    if scheme == "postgres":
        scheme = "postgresql"

    # Parse query params
    params = {k: v[0] if len(v) == 1 else v for k, v in parse_qs(parsed.query).items()}

    if scheme in ("s3", "gs"):
        return _parse_object_storage(scheme, parsed, stripped, connection_id, params)
    elif scheme == "abfss":
        return _parse_adls(parsed, stripped, connection_id, params)
    elif scheme == "snowflake":
        return _parse_snowflake(parsed, stripped, connection_id, params)
    elif scheme == "postgresql":
        return _parse_postgresql(parsed, stripped, connection_id, params)
    else:
        return SourceDescriptor(
            scheme=scheme,
            bucket_or_host=parsed.hostname or "",
            path=parsed.path,
            raw_uri=stripped,
            connection_id=connection_id,
            params=params,
        )


# ---------------------------------------------------------------------------
# PostgreSQL keyword/value format helpers
# ---------------------------------------------------------------------------

def _is_pg_keyword_format(uri: str) -> bool:
    """Check if a string is a PostgreSQL keyword/value connection string.
    
    Detects formats like: host=localhost port=5432 dbname=mydb user=postgres
    
    Returns True if the string contains PostgreSQL connection keywords and
    does NOT contain a scheme (no ://).
    """
    if "://" in uri:
        return False
    
    # Check if string contains any PostgreSQL keywords with = assignment
    lower = uri.lower()
    for keyword in _PG_CONNSTRING_KEYWORDS:
        if f"{keyword}=" in lower:
            return True
    
    return False


def _parse_pg_keyword_value(
    connstring: str,
    connection_id: str | None,
) -> SourceDescriptor:
    """Parse PostgreSQL keyword/value connection string.
    
    Format: host=localhost port=5432 dbname=mydb user=postgres password=secret
    
    Values can be:
        - Unquoted: key=value
        - Single-quoted: key='value with spaces'
        - Escaped: key='it\\'s value'
    """
    params = {}
    host = "localhost"
    port = 5432
    database = None
    schema_name = "public"
    table_name = None
    
    # Simple parser for key=value pairs
    # This handles basic cases; for production use, consider using libpq's parser
    i = 0
    while i < len(connstring):
        # Skip whitespace
        while i < len(connstring) and connstring[i].isspace():
            i += 1
        
        if i >= len(connstring):
            break
        
        # Read key
        key_start = i
        while i < len(connstring) and connstring[i] not in "= \t":
            i += 1
        key = connstring[key_start:i].strip().lower()
        
        if not key:
            break
        
        # Skip to =
        while i < len(connstring) and connstring[i] in " \t":
            i += 1
        
        if i >= len(connstring) or connstring[i] != "=":
            break
        
        i += 1  # Skip =
        
        # Skip whitespace after =
        while i < len(connstring) and connstring[i] in " \t":
            i += 1
        
        # Read value
        if i < len(connstring) and connstring[i] == "'":
            # Single-quoted value
            i += 1  # Skip opening quote
            value_start = i
            value = ""
            while i < len(connstring):
                if connstring[i] == "\\" and i + 1 < len(connstring):
                    # Escaped character
                    value += connstring[i + 1]
                    i += 2
                elif connstring[i] == "'":
                    i += 1  # Skip closing quote
                    break
                else:
                    value += connstring[i]
                    i += 1
        else:
            # Unquoted value (read until whitespace)
            value_start = i
            while i < len(connstring) and not connstring[i].isspace():
                i += 1
            value = connstring[value_start:i]
        
        # Store the key-value pair
        if key == "host":
            host = value
        elif key == "port":
            try:
                port = int(value)
            except ValueError:
                port = 5432
        elif key == "dbname":
            database = value
        elif key == "schema":
            schema_name = value
        elif key == "table":
            table_name = value
        else:
            params[key] = value
    
    host_port = f"{host}:{port}"
    
    return SourceDescriptor(
        scheme="postgresql",
        bucket_or_host=host_port,
        path=f"/{database}" if database else "/",
        raw_uri=connstring,
        connection_id=connection_id,
        database=database,
        schema_name=schema_name,
        table_name=table_name,
        params=params,
    )


# ---------------------------------------------------------------------------
# Scheme-specific parsers
# ---------------------------------------------------------------------------

def _parse_object_storage(
    scheme: str,
    parsed,
    raw_uri: str,
    connection_id: str | None,
    params: dict,
) -> SourceDescriptor:
    """Parse s3://bucket/key or gs://bucket/key."""
    bucket = parsed.hostname or parsed.netloc or ""
    path = parsed.path.lstrip("/")
    return SourceDescriptor(
        scheme=scheme,
        bucket_or_host=bucket,
        path=path,
        raw_uri=raw_uri,
        connection_id=connection_id,
        params=params,
    )


def _parse_adls(
    parsed,
    raw_uri: str,
    connection_id: str | None,
    params: dict,
) -> SourceDescriptor:
    """Parse abfss://container@account.dfs.core.windows.net/path/.

    The ADLS Gen2 URI format puts the container as the "username" part
    and the storage account as the hostname.
    """
    container = parsed.username or ""
    account_host = parsed.hostname or ""
    path = parsed.path.lstrip("/")

    # Combine container and host for the bucket_or_host field
    bucket_or_host = f"{container}@{account_host}" if container else account_host

    return SourceDescriptor(
        scheme="abfss",
        bucket_or_host=bucket_or_host,
        path=path,
        raw_uri=raw_uri,
        connection_id=connection_id,
        params=params,
    )


def _parse_snowflake(
    parsed,
    raw_uri: str,
    connection_id: str | None,
    params: dict,
) -> SourceDescriptor:
    """Parse snowflake://account/database/schema/table.

    Path segments:
        /database                → list all schemas
        /database/schema         → list all tables in schema
        /database/schema/table   → profile specific table
    """
    account = parsed.hostname or ""
    segments = [s for s in parsed.path.strip("/").split("/") if s]

    database = segments[0] if len(segments) > 0 else None
    schema_name = segments[1] if len(segments) > 1 else None
    table_name = segments[2] if len(segments) > 2 else params.get("table")

    return SourceDescriptor(
        scheme="snowflake",
        bucket_or_host=account,
        path=parsed.path,
        raw_uri=raw_uri,
        connection_id=connection_id,
        database=database,
        schema_name=schema_name,
        table_name=table_name,
        params=params,
    )


def _parse_postgresql(
    parsed,
    raw_uri: str,
    connection_id: str | None,
    params: dict,
) -> SourceDescriptor:
    """Parse postgresql://user:pass@host:port/dbname?table=t&schema=s&sslmode=require.

    The database name comes from the path.  Table and schema can be
    specified as query parameters or as path segments:
        postgresql://host/dbname                        → list all tables
        postgresql://host/dbname?table=users            → profile specific table
        postgresql://host/dbname/schema/table           → profile specific table
        postgresql://host/db?sslmode=require            → SSL connection
        postgresql://host/db?sslmode=verify-full&sslcert=/path/to/cert

    SSL parameters are extracted from query string and stored in params:
        - sslmode: disable, allow, prefer, require, verify-ca, verify-full
        - sslcert: path to client certificate
        - sslkey: path to client key
        - sslrootcert: path to root certificate
        - sslcrl: path to certificate revocation list
    """
    host = parsed.hostname or "localhost"
    port = parsed.port or 5432
    host_port = f"{host}:{port}"

    segments = [s for s in parsed.path.strip("/").split("/") if s]
    database = segments[0] if segments else None
    schema_name = segments[1] if len(segments) > 1 else params.get("schema", "public")
    table_name = segments[2] if len(segments) > 2 else params.get("table")

    # Extract username/password if present in URI
    if parsed.username:
        params["user"] = parsed.username
    if parsed.password:
        params["password"] = parsed.password

    # Build a conninfo string (without embedding password — that comes
    # from the ConnectionManager or env vars at query time).
    return SourceDescriptor(
        scheme="postgresql",
        bucket_or_host=host_port,
        path=parsed.path,
        raw_uri=raw_uri,
        connection_id=connection_id,
        database=database,
        schema_name=schema_name,
        table_name=table_name,
        params=params,
    )
