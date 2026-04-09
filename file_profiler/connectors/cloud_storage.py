"""
Cloud storage connector — S3, Azure ADLS Gen2, Google Cloud Storage.

Uses DuckDB extensions (httpfs, azure) for reading files and native SDKs
(boto3, azure-storage, google-cloud-storage) for listing objects.  The
SDKs are optional — if not installed, listing falls back to DuckDB's
glob support where possible.
"""

from __future__ import annotations

import logging
from pathlib import PurePosixPath
from typing import Optional
from urllib.parse import urlsplit

from file_profiler.connectors.base import (
    BaseConnector,
    ConnectorError,
    RemoteObject,
    SourceDescriptor,
)

log = logging.getLogger(__name__)

# File extensions we recognise as profilable
_PROFILABLE_EXTENSIONS = frozenset({
    ".csv", ".tsv", ".parquet", ".pq", ".parq",
    ".json", ".jsonl", ".ndjson",
    ".gz", ".zip",
})


class CloudStorageConnector(BaseConnector):
    """Connector for S3, Azure ADLS Gen2, and Google Cloud Storage.

    DuckDB handles actual data reading via httpfs/azure extensions.
    Native SDKs handle object listing (DuckDB can't list bucket contents).
    """

    def __init__(self, provider: str) -> None:
        """
        Args:
            provider: "s3", "minio", "adls", or "gcs".
        """
        if provider not in ("s3", "minio", "adls", "gcs"):
            raise ValueError(f"Unknown cloud provider: {provider}")
        self.provider = provider

    def test_connection(
        self,
        descriptor: SourceDescriptor,
        credentials: dict,
    ) -> bool:
        """Test connectivity by attempting to list the root/bucket."""
        try:
            objects = self.list_objects(descriptor, credentials)
            log.info(
                "Connection test OK for %s (%d objects found)",
                descriptor.raw_uri, len(objects),
            )
            return True
        except Exception as exc:
            raise ConnectorError(
                f"Connection test failed for {descriptor.raw_uri}: {exc}"
            ) from exc

    def configure_duckdb(self, con, descriptor, credentials) -> None:
        """Delegate to duckdb_remote module."""
        from file_profiler.connectors.duckdb_remote import create_remote_connection
        # configure_duckdb is called on an existing connection, but
        # create_remote_connection creates a new one.  Instead, apply
        # the scheme-specific config directly.
        from file_profiler.connectors import duckdb_remote
        if self.provider == "s3":
            duckdb_remote._configure_s3(con, credentials)
        elif self.provider == "minio":
            duckdb_remote._configure_minio(con, credentials)
        elif self.provider == "gcs":
            duckdb_remote._configure_gcs(con, credentials)
        elif self.provider == "adls":
            duckdb_remote._configure_adls(con, credentials)

    def list_objects(
        self,
        descriptor: SourceDescriptor,
        credentials: dict,
    ) -> list[RemoteObject]:
        """List profilable files at the given location.

        Uses native SDKs for listing (boto3/azure/gcs), falling back
        to a ConnectorError if the SDK is not installed.
        """
        if self.provider == "s3":
            return self._list_s3(descriptor, credentials)
        elif self.provider == "minio":
            return self._list_minio(descriptor, credentials)
        elif self.provider == "adls":
            return self._list_adls(descriptor, credentials)
        elif self.provider == "gcs":
            return self._list_gcs(descriptor, credentials)
        return []

    def duckdb_scan_expression(
        self,
        descriptor: SourceDescriptor,
        object_uri: Optional[str] = None,
    ) -> str:
        """Return DuckDB read expression for a cloud file.

        Auto-detects format from file extension and uses the
        appropriate reader (read_parquet, read_csv, read_json).
        """
        uri = object_uri or descriptor.raw_uri
        if self.provider == "minio" and uri.startswith("minio://"):
            uri = f"s3://{uri[len('minio://'):]}"
        ext = PurePosixPath(uri).suffix.lower()

        if ext in (".parquet", ".pq", ".parq"):
            return f"read_parquet('{uri}')"
        elif ext in (".csv", ".tsv"):
            return f"read_csv('{uri}', auto_detect=true)"
        elif ext in (".json", ".jsonl", ".ndjson"):
            return f"read_json('{uri}', auto_detect=true)"
        elif ext == ".gz":
            # Assume gzipped CSV
            return f"read_csv('{uri}', auto_detect=true, compression='gzip')"
        else:
            # Default: let DuckDB auto-detect
            return f"read_csv('{uri}', auto_detect=true)"

    # -------------------------------------------------------------------
    # Native SDK listing implementations
    # -------------------------------------------------------------------

    def _list_s3(
        self,
        descriptor: SourceDescriptor,
        credentials: dict,
    ) -> list[RemoteObject]:
        """List objects in an S3 bucket/prefix using boto3."""
        return self._list_s3_compatible(descriptor, credentials, uri_scheme="s3")

    def _list_minio(
        self,
        descriptor: SourceDescriptor,
        credentials: dict,
    ) -> list[RemoteObject]:
        """List objects in a MinIO bucket/prefix using boto3."""
        return self._list_s3_compatible(descriptor, credentials, uri_scheme="minio")

    def _list_s3_compatible(
        self,
        descriptor: SourceDescriptor,
        credentials: dict,
        *,
        uri_scheme: str,
    ) -> list[RemoteObject]:
        """List objects in an S3-compatible bucket/prefix using boto3."""
        try:
            import boto3
            from botocore.config import Config
        except ImportError:
            raise ConnectorError(
                "boto3 is required for S3 object listing. "
                "Install it with: pip install boto3"
            )

        session_kwargs, client_kwargs = _s3_client_options(
            credentials,
            provider=self.provider,
            config_factory=Config,
        )

        session = boto3.Session(**session_kwargs)
        s3 = session.client("s3", **client_kwargs)

        bucket = descriptor.bucket_or_host
        prefix = descriptor.path
        if prefix and not prefix.endswith("/"):
            prefix += "/"

        objects: list[RemoteObject] = []
        paginator = s3.get_paginator("list_objects_v2")

        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                ext = PurePosixPath(key).suffix.lower()
                if ext not in _PROFILABLE_EXTENSIONS:
                    continue
                objects.append(RemoteObject(
                    name=PurePosixPath(key).name,
                    uri=f"{uri_scheme}://{bucket}/{key}",
                    size_bytes=obj.get("Size"),
                    file_format=_ext_to_format(ext),
                ))

        return objects

    def _list_adls(
        self,
        descriptor: SourceDescriptor,
        credentials: dict,
    ) -> list[RemoteObject]:
        """List objects in an ADLS Gen2 container using azure SDK."""
        try:
            from azure.storage.filedatalake import DataLakeServiceClient
        except ImportError:
            raise ConnectorError(
                "azure-storage-file-datalake is required for ADLS listing. "
                "Install it with: pip install azure-storage-file-datalake"
            )

        if credentials.get("connection_string"):
            client = DataLakeServiceClient.from_connection_string(
                credentials["connection_string"]
            )
        elif credentials.get("tenant_id"):
            from azure.identity import ClientSecretCredential
            cred = ClientSecretCredential(
                credentials["tenant_id"],
                credentials["client_id"],
                credentials["client_secret"],
            )
            account_name = credentials.get("account_name", "")
            account_url = f"https://{account_name}.dfs.core.windows.net"
            client = DataLakeServiceClient(account_url, credential=cred)
        else:
            raise ConnectorError(
                "ADLS credentials required: provide connection_string or "
                "tenant_id + client_id + client_secret"
            )

        # Parse container from bucket_or_host
        container = descriptor.bucket_or_host.split("@")[0]
        fs_client = client.get_file_system_client(container)

        objects: list[RemoteObject] = []
        prefix = descriptor.path or ""

        for path_item in fs_client.get_paths(path=prefix):
            if path_item.is_directory:
                continue
            name = PurePosixPath(path_item.name).name
            ext = PurePosixPath(name).suffix.lower()
            if ext not in _PROFILABLE_EXTENSIONS:
                continue
            objects.append(RemoteObject(
                name=name,
                uri=f"abfss://{descriptor.bucket_or_host}/{path_item.name}",
                size_bytes=path_item.content_length,
                file_format=_ext_to_format(ext),
            ))

        return objects

    def _list_gcs(
        self,
        descriptor: SourceDescriptor,
        credentials: dict,
    ) -> list[RemoteObject]:
        """List objects in a GCS bucket/prefix using google-cloud-storage."""
        try:
            from google.cloud import storage as gcs_storage
        except ImportError:
            raise ConnectorError(
                "google-cloud-storage is required for GCS listing. "
                "Install it with: pip install google-cloud-storage"
            )

        client_kwargs = {}
        if credentials.get("service_account_json"):
            # Could be a path or inline JSON
            import json
            import os
            svc = credentials["service_account_json"]
            if os.path.isfile(svc):
                client = gcs_storage.Client.from_service_account_json(svc)
            else:
                info = json.loads(svc)
                client = gcs_storage.Client.from_service_account_info(info)
        else:
            # Application Default Credentials
            client = gcs_storage.Client(**client_kwargs)

        bucket = client.bucket(descriptor.bucket_or_host)
        prefix = descriptor.path or ""
        if prefix and not prefix.endswith("/"):
            prefix += "/"

        objects: list[RemoteObject] = []
        for blob in bucket.list_blobs(prefix=prefix):
            name = PurePosixPath(blob.name).name
            ext = PurePosixPath(name).suffix.lower()
            if ext not in _PROFILABLE_EXTENSIONS:
                continue
            objects.append(RemoteObject(
                name=name,
                uri=f"gs://{descriptor.bucket_or_host}/{blob.name}",
                size_bytes=blob.size,
                file_format=_ext_to_format(ext),
            ))

        return objects


def _ext_to_format(ext: str) -> str:
    """Map file extension to format string."""
    mapping = {
        ".csv": "csv", ".tsv": "csv",
        ".parquet": "parquet", ".pq": "parquet", ".parq": "parquet",
        ".json": "json", ".jsonl": "json", ".ndjson": "json",
        ".gz": "csv",  # assume gzipped CSV
    }
    return mapping.get(ext, "unknown")


def _s3_client_options(
    credentials: dict,
    *,
    provider: str,
    config_factory,
) -> tuple[dict, dict]:
    """Build boto3 session/client options for S3-compatible providers."""
    if provider == "minio":
        endpoint_url = _normalise_minio_endpoint_url(credentials.get("endpoint_url", ""))
        access_key = credentials.get("access_key", "")
        secret_key = credentials.get("secret_key", "")
        if not endpoint_url:
            raise ConnectorError("MinIO requires credentials['endpoint_url']")
        if not access_key or not secret_key:
            raise ConnectorError(
                "MinIO requires both credentials['access_key'] and "
                "credentials['secret_key']"
            )
        region = credentials.get("region") or "us-east-1"
        session_kwargs = {
            "aws_access_key_id": access_key,
            "aws_secret_access_key": secret_key,
            "region_name": region,
        }
        client_kwargs = {
            "endpoint_url": endpoint_url,
            "config": config_factory(s3={"addressing_style": "path"}),
        }
        return session_kwargs, client_kwargs

    session_kwargs = {}
    if credentials.get("aws_access_key_id"):
        session_kwargs["aws_access_key_id"] = credentials["aws_access_key_id"]
        session_kwargs["aws_secret_access_key"] = credentials["aws_secret_access_key"]
    if credentials.get("region"):
        session_kwargs["region_name"] = credentials["region"]
    if credentials.get("profile_name"):
        session_kwargs["profile_name"] = credentials["profile_name"]
    return session_kwargs, {}


def _normalise_minio_endpoint_url(endpoint_url: str) -> str:
    """Validate and normalise a MinIO endpoint URL for boto3."""
    raw = endpoint_url.strip().rstrip("/")
    if not raw:
        return ""

    parsed = urlsplit(raw)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ConnectorError(
            "MinIO endpoint_url must be a full http:// or https:// URL"
        )
    if parsed.path not in ("", "/") or parsed.query or parsed.fragment:
        raise ConnectorError(
            "MinIO endpoint_url must not include a path, query string, or fragment"
        )
    return f"{parsed.scheme}://{parsed.netloc}"
