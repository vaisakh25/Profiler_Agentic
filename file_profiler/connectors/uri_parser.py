"""
URI parser — converts user-provided strings into SourceDescriptor objects.

Handles:
    s3://bucket/key/path.parquet
    abfss://container@account.dfs.core.windows.net/path/
    gs://bucket/prefix/
    snowflake://account/database/schema/table
    snowflake://account/database/schema?warehouse=WH
    postgresql://user:pass@host:5432/dbname
    postgresql://user:pass@host:5432/dbname?table=mytable&schema=public
    /local/path/to/file.csv   → scheme="file"
    C:\\local\\path\\file.csv  → scheme="file"
"""

from __future__ import annotations

from urllib.parse import parse_qs, urlparse

from file_profiler.connectors.base import SourceDescriptor

# Schemes we recognise as remote sources.
_REMOTE_SCHEMES = frozenset({"s3", "abfss", "gs", "snowflake", "postgresql", "postgres"})


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

    Args:
        uri:            Raw URI or local path from the user.
        connection_id:  Optional reference to stored credentials.
    """
    stripped = uri.strip()

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
    """Parse postgresql://user:pass@host:port/dbname?table=t&schema=s.

    The database name comes from the path.  Table and schema can be
    specified as query parameters or as path segments:
        postgresql://host/dbname                 → list all tables
        postgresql://host/dbname?table=users     → profile specific table
        postgresql://host/dbname/schema/table    → profile specific table
    """
    host = parsed.hostname or "localhost"
    port = parsed.port or 5432
    host_port = f"{host}:{port}"

    segments = [s for s in parsed.path.strip("/").split("/") if s]
    database = segments[0] if segments else None
    schema_name = segments[1] if len(segments) > 1 else params.get("schema", "public")
    table_name = segments[2] if len(segments) > 2 else params.get("table")

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
