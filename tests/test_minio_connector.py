"""Tests for first-class MinIO connector support."""

from __future__ import annotations

import importlib
import sys
from types import SimpleNamespace

import pytest

from file_profiler.connectors.base import SourceDescriptor
from file_profiler.connectors.cloud_storage import CloudStorageConnector
from file_profiler.connectors.connection_manager import ConnectionManager
from file_profiler.connectors.duckdb_remote import _configure_minio
from file_profiler.connectors.uri_parser import is_remote_uri, parse_uri

registry_module = importlib.import_module("file_profiler.connectors.registry")


class FakeDuckDBConnection:
    """Collect SET statements emitted during DuckDB configuration."""

    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, statement: str) -> "FakeDuckDBConnection":
        self.statements.append(statement)
        return self


class FakeMinioConnector:
    """Capture descriptor/credential inputs from ConnectionManager.test()."""

    def __init__(self) -> None:
        self.calls: list[tuple[SourceDescriptor, dict]] = []

    def test_connection(self, descriptor: SourceDescriptor, credentials: dict) -> bool:
        self.calls.append((descriptor, dict(credentials)))
        return True


class FakePaginator:
    def __init__(self, pages: list[dict]) -> None:
        self.pages = pages
        self.calls: list[dict] = []

    def paginate(self, **kwargs):
        self.calls.append(kwargs)
        return iter(self.pages)


class FakeS3Client:
    def __init__(self, paginator: FakePaginator) -> None:
        self.paginator = paginator
        self.requested_names: list[str] = []

    def get_paginator(self, name: str) -> FakePaginator:
        self.requested_names.append(name)
        return self.paginator


class FakeSession:
    def __init__(self, recorder: dict, **kwargs) -> None:
        recorder["session_kwargs"] = kwargs
        self._recorder = recorder

    def client(self, service_name: str, **kwargs) -> FakeS3Client:
        self._recorder["service_name"] = service_name
        self._recorder["client_kwargs"] = kwargs
        return self._recorder["client"]


class FakeConfig:
    def __init__(self, **kwargs) -> None:
        self.kwargs = kwargs


def test_minio_uri_is_detected_and_parsed() -> None:
    assert is_remote_uri("minio://bucket/path/data.parquet")

    descriptor = parse_uri("minio://bucket/path/data.parquet")

    assert descriptor.scheme == "minio"
    assert descriptor.bucket_or_host == "bucket"
    assert descriptor.path == "path/data.parquet"
    assert descriptor.is_object_storage is True


def test_minio_duckdb_scan_expression_uses_s3_scheme() -> None:
    connector = CloudStorageConnector("minio")
    descriptor = SourceDescriptor(
        scheme="minio",
        bucket_or_host="bucket",
        path="folder/data.parquet",
        raw_uri="minio://bucket/folder/data.parquet",
    )

    expr = connector.duckdb_scan_expression(descriptor)

    assert expr == "read_parquet('s3://bucket/folder/data.parquet')"


def test_configure_minio_sets_endpoint_path_style_and_ssl_mode() -> None:
    con = FakeDuckDBConnection()

    _configure_minio(con, {
        "endpoint_url": "http://localhost:9000/",
        "access_key": "minioadmin",
        "secret_key": "miniosecret",
    })

    assert "INSTALL httpfs" in con.statements
    assert "LOAD httpfs" in con.statements
    assert "SET s3_endpoint = 'localhost:9000'" in con.statements
    assert "SET s3_url_style = 'path'" in con.statements
    assert "SET s3_use_ssl = false" in con.statements
    assert "SET s3_access_key_id = 'minioadmin'" in con.statements
    assert "SET s3_secret_access_key = 'miniosecret'" in con.statements
    assert "SET s3_region = 'us-east-1'" in con.statements


def test_minio_listing_uses_endpoint_url_and_path_style(monkeypatch: pytest.MonkeyPatch) -> None:
    recorder: dict = {}
    paginator = FakePaginator([
        {
            "Contents": [
                {"Key": "reports/customers.parquet", "Size": 128},
                {"Key": "reports/readme.txt", "Size": 8},
            ]
        }
    ])
    recorder["client"] = FakeS3Client(paginator)

    monkeypatch.setitem(
        sys.modules,
        "boto3",
        SimpleNamespace(Session=lambda **kwargs: FakeSession(recorder, **kwargs)),
    )
    monkeypatch.setitem(
        sys.modules,
        "botocore",
        SimpleNamespace(config=SimpleNamespace(Config=FakeConfig)),
    )
    monkeypatch.setitem(
        sys.modules,
        "botocore.config",
        SimpleNamespace(Config=FakeConfig),
    )

    connector = CloudStorageConnector("minio")
    descriptor = SourceDescriptor(
        scheme="minio",
        bucket_or_host="warehouse",
        path="reports",
        raw_uri="minio://warehouse/reports/",
    )

    objects = connector.list_objects(descriptor, {
        "endpoint_url": "http://localhost:9000",
        "access_key": "minioadmin",
        "secret_key": "miniosecret",
        "region": "us-east-1",
    })

    assert recorder["session_kwargs"] == {
        "aws_access_key_id": "minioadmin",
        "aws_secret_access_key": "miniosecret",
        "region_name": "us-east-1",
    }
    assert recorder["service_name"] == "s3"
    assert recorder["client_kwargs"]["endpoint_url"] == "http://localhost:9000"
    assert recorder["client_kwargs"]["config"].kwargs == {
        "s3": {"addressing_style": "path"}
    }
    assert paginator.calls == [{"Bucket": "warehouse", "Prefix": "reports/"}]
    assert [obj.uri for obj in objects] == [
        "minio://warehouse/reports/customers.parquet"
    ]


def test_connection_manager_minio_test_requires_test_bucket(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(ConnectionManager, "_load_persisted", lambda self: None)
    monkeypatch.setattr(ConnectionManager, "_persist", lambda self: None)
    monkeypatch.setattr(registry_module.registry, "get", lambda scheme: FakeMinioConnector())

    mgr = ConnectionManager()
    mgr.register("minio-dev", "minio", {
        "endpoint_url": "http://localhost:9000",
        "access_key": "minioadmin",
        "secret_key": "miniosecret",
    })

    result = mgr.test("minio-dev")

    assert result.success is False
    assert "test_bucket" in result.message


def test_connection_manager_minio_test_uses_bucket_and_default_region(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    fake_connector = FakeMinioConnector()
    monkeypatch.setattr(ConnectionManager, "_load_persisted", lambda self: None)
    monkeypatch.setattr(ConnectionManager, "_persist", lambda self: None)
    monkeypatch.setattr(registry_module.registry, "get", lambda scheme: fake_connector)

    mgr = ConnectionManager()
    mgr.register("minio-dev", "minio", {
        "endpoint_url": "https://minio.example.com",
        "access_key": "minioadmin",
        "secret_key": "miniosecret",
        "test_bucket": "warehouse/reports",
    })

    result = mgr.test("minio-dev")

    assert result.success is True
    descriptor, credentials = fake_connector.calls[0]
    assert descriptor.scheme == "minio"
    assert descriptor.bucket_or_host == "warehouse"
    assert descriptor.path == "reports"
    assert descriptor.raw_uri == "minio://warehouse/reports"
    assert credentials["region"] == "us-east-1"
    assert credentials["endpoint_url"] == "https://minio.example.com"
