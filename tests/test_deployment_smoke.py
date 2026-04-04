from __future__ import annotations

import os
import socket
import subprocess
import sys
from pathlib import Path

import pytest


def _pick_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        sock.listen(1)
        return int(sock.getsockname()[1])


def _terminate_process(proc: subprocess.Popen) -> None:
    proc.terminate()
    try:
        proc.wait(timeout=8)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=8)


def _base_env(project_root: Path) -> dict[str, str]:
    env = os.environ.copy()
    env.setdefault("PROFILER_DATA_DIR", str(project_root / "data" / "files"))
    env.setdefault("PROFILER_OUTPUT_DIR", str(project_root / "data" / "output"))
    env.setdefault("PROFILER_UPLOAD_DIR", str(project_root / "data" / "uploads"))
    env.setdefault("PYTHONDONTWRITEBYTECODE", "1")
    return env


@pytest.mark.smoke
@pytest.mark.integration
def test_file_profiler_mcp_health(project_root: Path, wait_for_http_ok) -> None:
    port = _pick_free_port()
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "file_profiler",
            "--transport",
            "sse",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ],
        cwd=str(project_root),
        env=_base_env(project_root),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    try:
        wait_for_http_ok(f"http://127.0.0.1:{port}/health", timeout_seconds=30.0)
    finally:
        _terminate_process(proc)


@pytest.mark.smoke
@pytest.mark.integration
def test_connector_mcp_health(project_root: Path, wait_for_http_ok) -> None:
    port = _pick_free_port()
    proc = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "file_profiler.connectors",
            "--transport",
            "sse",
            "--host",
            "127.0.0.1",
            "--port",
            str(port),
        ],
        cwd=str(project_root),
        env=_base_env(project_root),
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    try:
        wait_for_http_ok(f"http://127.0.0.1:{port}/health", timeout_seconds=30.0)
    finally:
        _terminate_process(proc)


@pytest.mark.smoke
@pytest.mark.docker
def test_docker_compose_health(project_root: Path, wait_for_http_ok, docker_available: bool) -> None:
    assert docker_available, "Docker CLI not available"

    daemon = subprocess.run(
        ["docker", "info"],
        capture_output=True,
        text=True,
        check=False,
    )
    assert daemon.returncode == 0, f"Docker daemon unavailable: {daemon.stderr.strip()}"

    manage_compose = os.getenv("SMOKE_MANAGE_DOCKER_COMPOSE", "0") == "1"
    compose_profile = os.getenv("SMOKE_DOCKER_COMPOSE_PROFILE", "").strip()
    verify_routed = os.getenv("SMOKE_DOCKER_VERIFY_ROUTED", "1") == "1"
    compose_cmd = ["docker", "compose"]
    if compose_profile:
        compose_cmd.extend(["--profile", compose_profile])

    if manage_compose:
        up = subprocess.run(
            compose_cmd + ["up", "-d", "--build"],
            cwd=str(project_root),
            capture_output=True,
            text=True,
        )
        assert up.returncode == 0, f"docker compose up failed: {up.stdout}\n{up.stderr}"

    try:
        health_url = os.getenv("SMOKE_DOCKER_HEALTH_URL", "http://127.0.0.1:9050/health")
        wait_for_http_ok(health_url, timeout_seconds=120.0)
        if verify_routed:
            file_health_url = os.getenv(
                "SMOKE_DOCKER_FILE_HEALTH_URL",
                "http://127.0.0.1:9050/mcp/file/health",
            )
            connector_health_url = os.getenv(
                "SMOKE_DOCKER_CONNECTOR_HEALTH_URL",
                "http://127.0.0.1:9050/mcp/connector/health",
            )
            wait_for_http_ok(file_health_url, timeout_seconds=240.0)
            wait_for_http_ok(connector_health_url, timeout_seconds=240.0)
    finally:
        if manage_compose:
            subprocess.run(
                compose_cmd + ["down", "--remove-orphans"],
                cwd=str(project_root),
                capture_output=True,
                text=True,
                check=False,
            )
