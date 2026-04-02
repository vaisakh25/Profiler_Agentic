"""Integration tests for LLM factory providers."""

import pytest

from file_profiler.agent import llm_factory


@pytest.fixture
def provider_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("GOOGLE_API_KEY", "test-google-key")
    monkeypatch.setenv("GROQ_API_KEY", "test-groq-key")
    monkeypatch.setenv("OPENAI_API_KEY", "test-openai-key")
    monkeypatch.setenv("ANTHROPIC_API_KEY", "test-anthropic-key")


@pytest.fixture
def stubbed_providers(monkeypatch: pytest.MonkeyPatch) -> None:
    class _DummyLLM:
        def __init__(self, model_name: str) -> None:
            self.model_name = model_name
            self.model = model_name

    monkeypatch.setattr(
        llm_factory,
        "_make_google",
        lambda model, temperature, timeout=0: _DummyLLM(model or "google-dummy"),
    )
    monkeypatch.setattr(
        llm_factory,
        "_make_groq",
        lambda model, temperature, timeout=0: _DummyLLM(model or "groq-dummy"),
    )


def test_groq_provider(provider_env, stubbed_providers) -> None:
    llm = llm_factory.get_llm(provider="groq")
    assert getattr(llm, "model_name", None)


def test_google_provider(provider_env, stubbed_providers) -> None:
    llm = llm_factory.get_llm(provider="google")
    assert getattr(llm, "model", None) or getattr(llm, "model_name", None)


def test_google_fallback_provider(provider_env, stubbed_providers) -> None:
    llm = llm_factory.get_llm_with_fallback(provider="google")
    assert llm is not None


def test_groq_with_fallback(provider_env, stubbed_providers) -> None:
    llm = llm_factory.get_llm_with_fallback(provider="groq")
    assert llm is not None
