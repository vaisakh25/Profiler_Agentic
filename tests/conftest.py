from __future__ import annotations

import hashlib
import shutil
import time
import urllib.request
from pathlib import Path

import pytest


_GATE_MARKERS = {"unit", "integration", "smoke", "docker", "live", "manual"}
_AUTO_FILE_MARKS: dict[str, tuple[str, ...]] = {
    "test_build_graph.py": ("integration", "smoke"),
    "test_chatbot_e2e.py": ("integration", "live"),
    "test_chatbot_progress_e2e.py": ("integration", "live"),
    "test_enrichment_e2e.py": ("integration", "live"),
    "test_llm_factory.py": ("integration", "live"),
    "test_ws.py": ("integration", "live"),
    "test_e2e_run.py": ("manual", "live"),
    "test_mcp_connect.py": ("manual", "integration"),
    "test_pg.py": ("manual", "live"),
    "test_er_improvements.py": ("manual", "integration"),
}


def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--run-live",
        action="store_true",
        default=True,
        help="Legacy compatibility flag; live tests are included by default.",
    )
    parser.addoption(
        "--run-manual",
        action="store_true",
        default=True,
        help="Legacy compatibility flag; manual tests are included by default.",
    )
    parser.addoption(
        "--run-docker",
        action="store_true",
        default=True,
        help="Legacy compatibility flag; docker tests are included by default.",
    )


def pytest_collection_modifyitems(config: pytest.Config, items: list[pytest.Item]) -> None:
    for item in items:
        test_file = Path(str(item.fspath)).name
        for marker_name in _AUTO_FILE_MARKS.get(test_file, ()):
            item.add_marker(getattr(pytest.mark, marker_name))

        if not any(marker in item.keywords for marker in _GATE_MARKERS):
            item.add_marker(pytest.mark.unit)


@pytest.fixture(scope="session")
def project_root() -> Path:
    return Path(__file__).resolve().parent.parent


@pytest.fixture(scope="session")
def docker_available() -> bool:
    return shutil.which("docker") is not None


@pytest.fixture
def wait_for_http_ok():
    def _wait(url: str, timeout_seconds: float = 30.0) -> None:
        deadline = time.monotonic() + timeout_seconds
        last_error = ""

        while time.monotonic() < deadline:
            try:
                with urllib.request.urlopen(url, timeout=2.0) as resp:
                    if 200 <= resp.status < 300:
                        return
                    last_error = f"HTTP {resp.status}"
            except Exception as exc:  # pragma: no cover - network race tolerant
                last_error = str(exc)
            time.sleep(0.5)

        raise AssertionError(f"Timed out waiting for {url}: {last_error}")

    return _wait


@pytest.fixture(autouse=True)
def mock_nvidia_embeddings(monkeypatch: pytest.MonkeyPatch):
    """Use deterministic test embeddings instead of live NVIDIA API calls."""

    def _embed_one(text: str, dims: int = 64) -> list[float]:
        digest = hashlib.sha256((text or "").encode("utf-8", errors="ignore")).digest()
        values = list(digest)
        expanded: list[int] = []
        while len(expanded) < dims:
            expanded.extend(values)
        return [v / 255.0 for v in expanded[:dims]]

    class _DeterministicEmbeddings:
        def embed_documents(self, texts: list[str]) -> list[list[float]]:
            return [_embed_one(text) for text in texts]

        def embed_query(self, text: str) -> list[float]:
            return _embed_one(text)

    deterministic_embeddings = _DeterministicEmbeddings()
    monkeypatch.setattr(
        "file_profiler.agent.vector_store.get_embeddings",
        lambda: deterministic_embeddings,
    )
