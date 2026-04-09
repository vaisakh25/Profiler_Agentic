"""
DuckDB remote access layer — extension-aware connections for remote sources.

Creates DuckDB in-memory connections with the right extensions loaded
and credentials configured for cloud storage and remote databases.
Provides count and sample functions that work identically to the local
``duckdb_sampler.py`` but target remote data.

This is the shared layer that all object-storage connectors and
PostgreSQL funnel through.
"""

from __future__ import annotations

import logging
from typing import Optional
from urllib.parse import urlsplit

import duckdb

from file_profiler.config.env import DUCKDB_MEMORY_LIMIT, DUCKDB_THREADS
from file_profiler.connectors.base import ConnectorError, SourceDescriptor

log = logging.getLogger(__name__)


def create_remote_connection(
    descriptor: SourceDescriptor,
    credentials: dict,
) -> duckdb.DuckDBPyConnection:
    """Create a DuckDB in-memory connection configured for a remote source.

    Installs and loads the required extension (httpfs, azure, postgres_scanner)
    and sets credential parameters via SET statements.

    Args:
        descriptor:  Parsed source descriptor.
        credentials: Auth credentials from ConnectionManager.

    Returns:
        A ready-to-query DuckDB connection.

    Raises:
        ConnectorError: if extension loading or credential config fails.
    """
    try:
        con = duckdb.connect(":memory:")
        con.execute(f"SET memory_limit = '{DUCKDB_MEMORY_LIMIT}'")
        con.execute(f"SET threads = {DUCKDB_THREADS}")
    except Exception as exc:
        raise ConnectorError(f"Failed to create DuckDB connection: {exc}") from exc

    try:
        if descriptor.scheme == "s3":
            _configure_s3(con, credentials)
        elif descriptor.scheme == "minio":
            _configure_minio(con, credentials)
        elif descriptor.scheme == "gs":
            _configure_gcs(con, credentials)
        elif descriptor.scheme == "abfss":
            _configure_adls(con, credentials)
        elif descriptor.scheme == "postgresql":
            _configure_postgres(con)
        else:
            log.warning("No DuckDB extension config for scheme '%s'", descriptor.scheme)
    except Exception as exc:
        con.close()
        raise ConnectorError(
            f"Failed to configure DuckDB for {descriptor.scheme}: {exc}"
        ) from exc

    return con


def remote_count(
    con: duckdb.DuckDBPyConnection,
    scan_expr: str,
) -> int:
    """Count rows via DuckDB from a remote scan expression.

    Args:
        con:       Configured DuckDB connection.
        scan_expr: SQL expression like "read_parquet('s3://...')".

    Returns:
        Row count.
    """
    try:
        result = con.execute(f"SELECT COUNT(*) FROM {scan_expr}").fetchone()
        return result[0] if result else 0
    except Exception as exc:
        raise ConnectorError(f"Remote count failed: {exc}") from exc


def remote_sample(
    con: duckdb.DuckDBPyConnection,
    scan_expr: str,
    sample_size: int = 10_000,
) -> tuple[list[str], list[list[str]]]:
    """Reservoir-sample rows from a remote source via DuckDB.

    Args:
        con:         Configured DuckDB connection.
        scan_expr:   SQL expression to read from.
        sample_size: Max rows to return.

    Returns:
        (column_names, rows) where rows is a list of lists of strings.
    """
    try:
        query = (
            f"SELECT * FROM {scan_expr} "
            f"USING SAMPLE {sample_size} ROWS (reservoir, 42)"
        )
        result = con.execute(query)
        headers = [desc[0] for desc in result.description]
        rows = [
            [str(v) if v is not None else None for v in row]
            for row in result.fetchall()
        ]
        return headers, rows
    except Exception as exc:
        raise ConnectorError(f"Remote sample failed: {exc}") from exc


def remote_schema(
    con: duckdb.DuckDBPyConnection,
    scan_expr: str,
) -> list[tuple[str, str]]:
    """Get column names and types from a remote scan expression.

    Returns:
        List of (column_name, column_type) tuples.
    """
    try:
        result = con.execute(f"SELECT * FROM {scan_expr} LIMIT 0")
        return [(desc[0], desc[1]) for desc in result.description]
    except Exception as exc:
        raise ConnectorError(f"Remote schema read failed: {exc}") from exc


# ---------------------------------------------------------------------------
# Extension configuration per scheme
# ---------------------------------------------------------------------------

def _configure_s3(con: duckdb.DuckDBPyConnection, credentials: dict) -> None:
    """Load httpfs and set AWS S3 credentials."""
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")

    if credentials.get("aws_access_key_id"):
        con.execute(f"SET s3_access_key_id = '{credentials['aws_access_key_id']}'")
        con.execute(f"SET s3_secret_access_key = '{credentials['aws_secret_access_key']}'")
    if credentials.get("region"):
        con.execute(f"SET s3_region = '{credentials['region']}'")

    # If no explicit keys, DuckDB will use the default credential chain
    # (env vars, instance profile, etc.)
    log.debug("DuckDB S3 extension configured")


def _configure_minio(con: duckdb.DuckDBPyConnection, credentials: dict) -> None:
    """Load httpfs and configure DuckDB for a MinIO endpoint."""
    endpoint_url = credentials.get("endpoint_url", "").strip()
    access_key = credentials.get("access_key", "")
    secret_key = credentials.get("secret_key", "")

    if not endpoint_url:
        raise ConnectorError("MinIO requires credentials['endpoint_url']")
    if not access_key or not secret_key:
        raise ConnectorError(
            "MinIO requires both credentials['access_key'] and "
            "credentials['secret_key']"
        )

    endpoint, use_ssl = _parse_s3_compatible_endpoint(endpoint_url)
    region = credentials.get("region") or "us-east-1"

    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    con.execute(f"SET s3_endpoint = '{endpoint}'")
    con.execute("SET s3_url_style = 'path'")
    con.execute(f"SET s3_use_ssl = {'true' if use_ssl else 'false'}")
    con.execute(f"SET s3_access_key_id = '{access_key}'")
    con.execute(f"SET s3_secret_access_key = '{secret_key}'")
    con.execute(f"SET s3_region = '{region}'")
    log.debug("DuckDB MinIO extension configured for endpoint %s", endpoint)


def _configure_gcs(con: duckdb.DuckDBPyConnection, credentials: dict) -> None:
    """Load httpfs and configure for Google Cloud Storage.

    GCS is accessed through the S3-compatible API with a custom endpoint.
    """
    con.execute("INSTALL httpfs")
    con.execute("LOAD httpfs")
    con.execute("SET s3_endpoint = 'storage.googleapis.com'")
    con.execute("SET s3_url_style = 'path'")

    # If a service account key is provided, use HMAC-style auth
    if credentials.get("access_key"):
        con.execute(f"SET s3_access_key_id = '{credentials['access_key']}'")
        con.execute(f"SET s3_secret_access_key = '{credentials['secret_key']}'")

    log.debug("DuckDB GCS extension configured")


def _configure_adls(con: duckdb.DuckDBPyConnection, credentials: dict) -> None:
    """Load azure extension and set ADLS credentials."""
    con.execute("INSTALL azure")
    con.execute("LOAD azure")

    if credentials.get("connection_string"):
        con.execute(
            f"SET azure_storage_connection_string = '{credentials['connection_string']}'"
        )
    elif credentials.get("tenant_id"):
        con.execute(f"SET azure_tenant_id = '{credentials['tenant_id']}'")
        con.execute(f"SET azure_client_id = '{credentials['client_id']}'")
        con.execute(f"SET azure_client_secret = '{credentials['client_secret']}'")

    if credentials.get("account_name"):
        con.execute(f"SET azure_account_name = '{credentials['account_name']}'")

    log.debug("DuckDB Azure extension configured")


def _configure_postgres(con: duckdb.DuckDBPyConnection) -> None:
    """Load postgres_scanner extension.

    Credentials are embedded in the scan expression conninfo string,
    not SET globally.
    """
    con.execute("INSTALL postgres_scanner")
    con.execute("LOAD postgres_scanner")
    log.debug("DuckDB postgres_scanner extension configured")


def _parse_s3_compatible_endpoint(endpoint_url: str) -> tuple[str, bool]:
    """Convert an HTTP(S) endpoint URL to DuckDB's s3_endpoint format."""
    parsed = urlsplit(endpoint_url.strip())
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ConnectorError(
            "MinIO endpoint_url must be a full http:// or https:// URL"
        )
    if parsed.path not in ("", "/") or parsed.query or parsed.fragment:
        raise ConnectorError(
            "MinIO endpoint_url must not include a path, query string, or fragment"
        )
    return parsed.netloc, parsed.scheme == "https"
