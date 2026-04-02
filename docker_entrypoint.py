from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from typing import Dict, List, Tuple


def env_enabled(name: str, default: str) -> bool:
    value = os.getenv(name, default).strip().lower()
    return value in {"1", "true", "yes", "on"}


def build_service_commands() -> List[Tuple[str, List[str]]]:
    transport = os.getenv("MCP_TRANSPORT", "sse")
    host = os.getenv("MCP_HOST", "0.0.0.0")
    profiler_port = os.getenv("MCP_PORT", "8080")
    connector_port = os.getenv("CONNECTOR_MCP_PORT", "8081")

    commands: List[Tuple[str, List[str]]] = []

    if env_enabled("ENABLE_PROFILER_MCP", "1"):
        commands.append(
            (
                "profiler-mcp",
                [
                    "python",
                    "-m",
                    "file_profiler",
                    "--transport",
                    transport,
                    "--host",
                    host,
                    "--port",
                    profiler_port,
                ],
            )
        )

    if env_enabled("ENABLE_CONNECTOR_MCP", "1"):
        commands.append(
            (
                "connector-mcp",
                [
                    "python",
                    "-m",
                    "file_profiler.connectors",
                    "--transport",
                    transport,
                    "--host",
                    host,
                    "--port",
                    connector_port,
                ],
            )
        )

    if env_enabled("ENABLE_WEB_UI", "0"):
        web_port = os.getenv("WEB_PORT", "8501")
        web_mcp_url = os.getenv("WEB_MCP_URL", f"http://localhost:{profiler_port}/sse")
        web_connector_url = os.getenv(
            "WEB_CONNECTOR_MCP_URL", f"http://localhost:{connector_port}/sse"
        )
        commands.append(
            (
                "web-ui",
                [
                    "python",
                    "-m",
                    "file_profiler.agent",
                    "--web",
                    "--web-port",
                    web_port,
                    "--mcp-url",
                    web_mcp_url,
                    "--connector-mcp-url",
                    web_connector_url,
                ],
            )
        )

    return commands


def terminate_all(processes: Dict[str, subprocess.Popen]) -> None:
    for proc in processes.values():
        if proc.poll() is None:
            proc.terminate()

    deadline = time.time() + 15
    while time.time() < deadline:
        alive = [proc for proc in processes.values() if proc.poll() is None]
        if not alive:
            return
        time.sleep(0.5)

    for proc in processes.values():
        if proc.poll() is None:
            proc.kill()


def main() -> int:
    commands = build_service_commands()
    if not commands:
        print("No services enabled. Set ENABLE_PROFILER_MCP=1 and/or ENABLE_CONNECTOR_MCP=1")
        return 1

    processes: Dict[str, subprocess.Popen] = {}

    def handle_signal(signum, frame) -> None:  # type: ignore[no-untyped-def]
        print(f"Received signal {signum}, shutting down child services...")
        terminate_all(processes)
        raise SystemExit(0)

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    try:
        for name, cmd in commands:
            print(f"Starting {name}: {' '.join(cmd)}")
            processes[name] = subprocess.Popen(cmd)
            time.sleep(1)

        while True:
            for name, proc in list(processes.items()):
                exit_code = proc.poll()
                if exit_code is not None:
                    print(f"Service {name} exited with code {exit_code}; stopping remaining services")
                    terminate_all(processes)
                    return exit_code
            time.sleep(1)
    finally:
        terminate_all(processes)


if __name__ == "__main__":
    raise SystemExit(main())
