"""Tests for optional LangSmith observability integration."""

from __future__ import annotations

import importlib
import sys
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest


def _reload_observability(monkeypatch, **env):
    keys = {
        "LANGSMITH_TRACING",
        "LANGSMITH_PROJECT",
        "LANGSMITH_ENDPOINT",
        "LANGSMITH_SAMPLE_RATE",
        "LANGSMITH_HIDE_SAMPLE_VALUES",
        "LANGSMITH_PROMPTS_ENABLED",
        "LANGSMITH_PROMPT_TAG",
        "LANGCHAIN_TRACING_V2",
        "LANGCHAIN_PROJECT",
    }
    for key in keys:
        monkeypatch.delenv(key, raising=False)
    for key, value in env.items():
        monkeypatch.setenv(key, str(value))

    import file_profiler.observability.langsmith as obs

    return importlib.reload(obs)


def test_privacy_helpers_hash_sensitive_identifiers(monkeypatch) -> None:
    obs = _reload_observability(
        monkeypatch,
        LANGSMITH_HIDE_SAMPLE_VALUES="true",
    )

    raw = "customers_secret_table"
    safe = obs.safe_name(raw, kind="table")

    assert safe.startswith("table:")
    assert raw not in safe
    assert obs.stable_hash(raw) == obs.stable_hash(raw)
    assert obs.safe_host("https://integrate.api.nvidia.com/v1") == "integrate.api.nvidia.com"


def test_safe_name_can_expose_values_when_privacy_disabled(monkeypatch) -> None:
    obs = _reload_observability(
        monkeypatch,
        LANGSMITH_HIDE_SAMPLE_VALUES="false",
    )

    assert obs.safe_name("orders", kind="table") == "orders"


def test_traceable_is_noop_when_langsmith_unavailable(monkeypatch) -> None:
    obs = _reload_observability(monkeypatch, LANGSMITH_TRACING="true")
    monkeypatch.setattr(obs, "_load_langsmith", lambda: None)

    def original(value):
        return value + 1

    wrapped = obs.traceable(name="unit.test")(original)

    assert wrapped is original
    assert wrapped(1) == 2


def test_trace_context_is_noop_when_langsmith_unavailable(monkeypatch) -> None:
    obs = _reload_observability(monkeypatch, LANGSMITH_TRACING="true")
    monkeypatch.setattr(obs, "_load_langsmith", lambda: None)

    with obs.trace_context(surface="test", flow="unit"):
        value = "ok"

    assert value == "ok"


def test_configure_langsmith_env_sets_langchain_vars_when_enabled(monkeypatch) -> None:
    obs = _reload_observability(
        monkeypatch,
        LANGSMITH_TRACING="true",
        LANGSMITH_PROJECT="profiler-tests",
        LANGSMITH_ENDPOINT="https://api.smith.langchain.com",
    )

    obs.configure_langsmith_env()

    assert obs.LANGSMITH_TRACING is True
    assert obs.LANGSMITH_PROJECT == "profiler-tests"
    assert obs.LANGSMITH_ENDPOINT == "https://api.smith.langchain.com"
    assert obs.os.environ["LANGSMITH_TRACING"] == "true"
    assert obs.os.environ["LANGCHAIN_TRACING_V2"] == "true"
    assert obs.os.environ["LANGSMITH_PROJECT"] == "profiler-tests"
    assert obs.os.environ["LANGCHAIN_PROJECT"] == "profiler-tests"


def test_langsmith_defaults_are_safe(monkeypatch) -> None:
    obs = _reload_observability(monkeypatch)

    assert obs.LANGSMITH_TRACING is False
    assert obs.LANGSMITH_HIDE_SAMPLE_VALUES is True
    assert obs.LANGSMITH_PROMPTS_ENABLED is False


def test_resolve_prompt_returns_default_unless_prompt_pulling_enabled(monkeypatch) -> None:
    obs = _reload_observability(
        monkeypatch,
        LANGSMITH_PROMPTS_ENABLED="false",
    )

    assert obs.resolve_prompt("file-profiler/map", "local prompt") == "local prompt"


def test_resolve_prompt_pulls_fake_langsmith_prompt_when_enabled(monkeypatch) -> None:
    class _FakePrompt:
        template = "remote prompt"

    class _FakeClient:
        requested: list[str] = []

        def pull_prompt(self, name: str):
            self.requested.append(name)
            return _FakePrompt()

    fake_langsmith = SimpleNamespace(Client=_FakeClient)
    obs = _reload_observability(
        monkeypatch,
        LANGSMITH_PROMPTS_ENABLED="true",
        LANGSMITH_PROMPT_TAG="staging",
    )
    monkeypatch.setattr(obs, "_load_langsmith", lambda: fake_langsmith)

    assert obs.resolve_prompt("file-profiler/map", "local prompt") == "remote prompt"
    assert _FakeClient.requested == ["file-profiler/map:staging"]


def test_compact_outputs_and_profiles_do_not_leak_raw_values(monkeypatch) -> None:
    obs = _reload_observability(
        monkeypatch,
        LANGSMITH_HIDE_SAMPLE_VALUES="true",
    )
    profiles = [
        SimpleNamespace(
            table_name="customers_sensitive",
            row_count=10,
            columns=[SimpleNamespace(name="secret_column")],
        )
    ]

    text_summary = obs.compact_text_output("very secret model output")
    vector_summary = obs.compact_vector_output([[0.1, 0.2, 0.3], [0.4, 0.5, 0.6]])
    profile_summary = obs.describe_profiles(profiles)

    assert text_summary == {"output_chars": 24}
    assert vector_summary == {"vectors": 2, "dimensions": 3}
    assert profile_summary["table_count"] == 1
    assert profile_summary["row_count"] == 10
    assert profile_summary["column_count"] == 1
    assert "customers_sensitive" not in str(profile_summary)
    assert "secret_column" not in str(profile_summary)


def test_llm_invoke_trace_input_summary_does_not_include_prompt() -> None:
    from file_profiler.agent.enrichment_mapreduce import _trace_llm_invoke_inputs

    secret_prompt = "prompt with raw sample value password=super-secret"
    summary = _trace_llm_invoke_inputs({"prompt": secret_prompt, "max_retries": 2})

    assert summary["prompt_chars"] == len(secret_prompt)
    assert summary["estimated_tokens"] > 0
    assert "super-secret" not in str(summary)
    assert "prompt with raw sample" not in str(summary)


@pytest.mark.asyncio
async def test_decorated_map_phase_still_works_with_mock_llm(monkeypatch) -> None:
    from tests.test_enrichment_mapreduce import _make_profile
    from file_profiler.agent.enrichment_mapreduce import map_phase

    monkeypatch.setenv("LANGSMITH_TRACING", "false")
    llm = AsyncMock()
    llm.ainvoke = AsyncMock(
        return_value=MagicMock(
            content=(
                '{"summary":"Customer table.",'
                '"column_descriptions":{'
                '"customers_id":{"type":"int","role":"PK","description":"Identifier."},'
                '"description":{"type":"string","role":"regular","description":"Description."}'
                "}}"
            )
        )
    )

    summaries, descriptions = await map_phase(
        [_make_profile("customers")],
        llm,
        max_workers=1,
    )

    assert summaries == {"customers": "Customer table."}
    assert set(descriptions["customers"]) == {"customers_id", "description"}


@pytest.mark.asyncio
async def test_decorated_reduce_phases_still_work_with_mock_llm(tmp_path) -> None:
    from tests.test_enrichment_mapreduce import _make_profile, _make_report
    from file_profiler.agent.enrichment_mapreduce import (
        embed_phase,
        meta_reduce_phase,
        reduce_cluster_phase,
        reduce_phase,
    )

    profiles = [_make_profile("customers"), _make_profile("orders")]
    report = _make_report(profiles)
    store, _ = embed_phase(
        {"customers": "Customer master.", "orders": "Order facts."},
        profiles,
        report,
        {},
        tmp_path,
    )
    llm = AsyncMock()
    llm.ainvoke = AsyncMock(return_value=MagicMock(content="Analysis output."))

    reduced = await reduce_phase(store, report, profiles, llm)
    clusters = await reduce_cluster_phase({0: ["customers"], 1: ["orders"]}, store, report, llm)
    meta = await meta_reduce_phase(
        {0: ["customers"], 1: ["orders"]},
        clusters,
        report,
        llm,
    )

    assert reduced == "Analysis output."
    assert clusters == {0: "Analysis output.", 1: "Analysis output."}
    assert meta == "Analysis output."


def test_decorated_nvidia_embedding_wrapper_returns_vectors(monkeypatch) -> None:
    from file_profiler.agent.embedding_factory import NvidiaOpenAIEmbeddings

    class _EmbeddingItem:
        def __init__(self, index: int, embedding: list[float]) -> None:
            self.index = index
            self.embedding = embedding

    class _EmbeddingResponse:
        data = [
            _EmbeddingItem(1, [0.3, 0.4]),
            _EmbeddingItem(0, [0.1, 0.2]),
        ]

    class _Embeddings:
        def create(self, **kwargs):
            self.kwargs = kwargs
            return _EmbeddingResponse()

    class _Client:
        def __init__(self, **kwargs) -> None:
            self.embeddings = _Embeddings()

    fake_openai = ModuleType("openai")
    fake_openai.OpenAI = _Client
    monkeypatch.setitem(sys.modules, "openai", fake_openai)

    embeddings = NvidiaOpenAIEmbeddings(api_key="test-key", batch_size=10)
    vectors = embeddings._embed(["a", "b"], input_type="passage")

    assert vectors == [[0.1, 0.2], [0.3, 0.4]]


def test_decorated_llm_factory_preserves_provider_behavior(monkeypatch) -> None:
    from file_profiler.agent import llm_factory

    class _DummyLLM:
        def __init__(self, model_name: str) -> None:
            self.model_name = model_name
            self.model = model_name

    monkeypatch.setenv("GOOGLE_API_KEY", "test-google-key")
    monkeypatch.setattr(
        llm_factory,
        "_make_google",
        lambda model, temperature, timeout=0: _DummyLLM(model or "google-dummy"),
    )

    llm = llm_factory.get_llm(provider="google", model="gemini-test")

    assert llm.model_name == "gemini-test"
