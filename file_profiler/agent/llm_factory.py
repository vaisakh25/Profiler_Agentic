"""Configurable LLM factory for the profiling agent.

Reads ``LLM_PROVIDER`` and ``LLM_MODEL`` from the environment.
Supported providers:

* ``anthropic`` (default) — requires ``langchain-anthropic`` + ``ANTHROPIC_API_KEY``
* ``openai``              — requires ``langchain-openai``    + ``OPENAI_API_KEY``
* ``google``              — requires ``langchain-google-genai`` + ``GOOGLE_API_KEY``
* ``groq``                — requires ``langchain-groq``      + ``GROQ_API_KEY``

When ``provider="google"``, if the Google call fails (e.g. quota exhausted)
and ``GROQ_API_KEY`` is set, the factory can be re-invoked with
``provider="groq"`` to fall back automatically.
"""

from __future__ import annotations

import logging
import os
from typing import Optional

from langchain_core.language_models.chat_models import BaseChatModel

log = logging.getLogger(__name__)

# Default models per provider
_DEFAULT_MODELS: dict[str, str] = {
    "anthropic": "claude-sonnet-4-20250514",
    "openai": "gpt-4o",
    "google": "gemini-3.1-flash-lite-preview",
    "groq": "llama-3.3-70b-versatile",
}

# Fallback chain: if a provider fails, try the next one
_FALLBACK_CHAIN: dict[str, str] = {
    "google": "groq",
}


def get_llm(
    provider: Optional[str] = None,
    model: Optional[str] = None,
    temperature: float = 0.0,
) -> BaseChatModel:
    """Create a chat model instance.

    Args:
        provider: One of ``"anthropic"``, ``"openai"``, ``"google"``, ``"groq"``.
                  Falls back to the ``LLM_PROVIDER`` env var, then ``"anthropic"``.
        model:    Model name override.  Falls back to ``LLM_MODEL`` env var,
                  then a sensible default per provider.
        temperature: Sampling temperature (default 0 for deterministic profiling).

    Returns:
        A LangChain ``BaseChatModel`` instance ready for ``.bind_tools()``.

    Raises:
        ImportError: If the required provider package is not installed.
        ValueError:  If the provider name is not recognised.
    """
    env_provider = os.getenv("LLM_PROVIDER", "google").lower()
    provider = (provider or env_provider).lower()
    # Only honour LLM_MODEL env var when the provider also comes from env
    # (avoids e.g. a Groq model name being sent to the Google API).
    if model:
        pass  # explicit caller override — always use it
    elif provider == env_provider:
        model = os.getenv("LLM_MODEL") or _DEFAULT_MODELS.get(provider)
    else:
        model = _DEFAULT_MODELS.get(provider)

    if provider == "anthropic":
        return _make_anthropic(model, temperature)
    if provider == "openai":
        return _make_openai(model, temperature)
    if provider == "google":
        return _make_google(model, temperature)
    if provider == "groq":
        return _make_groq(model, temperature)

    raise ValueError(
        f"Unknown LLM provider '{provider}'. "
        f"Supported: anthropic, openai, google, groq."
    )


def get_llm_with_fallback(
    provider: Optional[str] = None,
    model: Optional[str] = None,
    temperature: float = 0.0,
) -> BaseChatModel:
    """Create a chat model with automatic fallback.

    Tries the primary provider first. If it fails to instantiate (missing
    API key, import error), falls back through the chain:
    ``google → groq``.

    Returns:
        A LangChain ``BaseChatModel`` instance.
    """
    provider = (provider or os.getenv("LLM_PROVIDER", "anthropic")).lower()

    try:
        return get_llm(provider=provider, model=model, temperature=temperature)
    except (ImportError, ValueError, Exception) as exc:
        fallback = _FALLBACK_CHAIN.get(provider)
        if fallback and os.getenv(_api_key_env(fallback)):
            log.warning(
                "Primary provider '%s' failed (%s), falling back to '%s'",
                provider, exc, fallback,
            )
            return get_llm(
                provider=fallback,
                model=_DEFAULT_MODELS.get(fallback),
                temperature=temperature,
            )
        raise


def _api_key_env(provider: str) -> str:
    """Return the environment variable name for a provider's API key."""
    return {
        "anthropic": "ANTHROPIC_API_KEY",
        "openai": "OPENAI_API_KEY",
        "google": "GOOGLE_API_KEY",
        "groq": "GROQ_API_KEY",
    }.get(provider, "")


def _make_anthropic(model: str, temperature: float) -> BaseChatModel:
    try:
        from langchain_anthropic import ChatAnthropic
    except ImportError as exc:
        raise ImportError(
            "langchain-anthropic is required for the Anthropic provider. "
            "Install it with: pip install langchain-anthropic"
        ) from exc
    return ChatAnthropic(model=model, temperature=temperature)


def _make_openai(model: str, temperature: float) -> BaseChatModel:
    try:
        from langchain_openai import ChatOpenAI
    except ImportError as exc:
        raise ImportError(
            "langchain-openai is required for the OpenAI provider. "
            "Install it with: pip install langchain-openai"
        ) from exc
    return ChatOpenAI(model=model, temperature=temperature)


def _make_google(model: str, temperature: float) -> BaseChatModel:
    try:
        from langchain_google_genai import ChatGoogleGenerativeAI
    except ImportError as exc:
        raise ImportError(
            "langchain-google-genai is required for the Google provider. "
            "Install it with: pip install langchain-google-genai"
        ) from exc
    return ChatGoogleGenerativeAI(model=model, temperature=temperature)


def _make_groq(model: str, temperature: float) -> BaseChatModel:
    try:
        from langchain_groq import ChatGroq
    except ImportError as exc:
        raise ImportError(
            "langchain-groq is required for the Groq provider. "
            "Install it with: pip install langchain-groq"
        ) from exc
    return ChatGroq(model=model, temperature=temperature, api_key=os.getenv("GROQ_API_KEY"))
