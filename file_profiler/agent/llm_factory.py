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

# Default models per provider — overridable via env vars
_DEFAULT_MODELS: dict[str, str] = {
    "anthropic": os.getenv("DEFAULT_MODEL_ANTHROPIC", "claude-sonnet-4-20250514"),
    "openai": os.getenv("DEFAULT_MODEL_OPENAI", "gpt-4o"),
    "google": os.getenv("DEFAULT_MODEL_GOOGLE", "gemini-3.1-flash-lite-preview"),
    "groq": os.getenv("DEFAULT_MODEL_GROQ", "llama-3.3-70b-versatile"),
}

# Fallback chain: if a provider fails, try the next one
_FALLBACK_CHAIN: dict[str, str] = {
    "google": "groq",
}


def get_llm(
    provider: Optional[str] = None,
    model: Optional[str] = None,
    temperature: float = 0.0,
    timeout: int = 0,
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
        return _make_anthropic(model, temperature, timeout)
    if provider == "openai":
        return _make_openai(model, temperature, timeout)
    if provider == "google":
        return _make_google(model, temperature, timeout)
    if provider == "groq":
        return _make_groq(model, temperature, timeout)

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
    except (ImportError, ValueError, KeyError, OSError) as exc:
        # Only fall back on expected instantiation errors:
        #   ImportError — provider package not installed
        #   ValueError  — bad provider name or missing API key
        #   KeyError    — missing config
        #   OSError     — network/connection issues
        fallback = _FALLBACK_CHAIN.get(provider)
        if fallback and os.getenv(_api_key_env(fallback)):
            log.warning(
                "Primary provider '%s' failed (%s: %s), falling back to '%s'",
                provider, type(exc).__name__, exc, fallback,
            )
            return get_llm(
                provider=fallback,
                model=_DEFAULT_MODELS.get(fallback),
                temperature=temperature,
            )
        raise


def get_reduce_llm(
    provider: Optional[str] = None,
    model: Optional[str] = None,
    temperature: float = 0.0,
) -> BaseChatModel:
    """Create a stronger chat model for the REDUCE / META-REDUCE phases.

    Checks REDUCE_LLM_PROVIDER and REDUCE_LLM_MODEL env vars first.
    If not set, falls back to the standard get_llm_with_fallback() model.
    """
    reduce_provider = provider or os.getenv("REDUCE_LLM_PROVIDER", "").lower()
    reduce_model = model or os.getenv("REDUCE_LLM_MODEL", "")

    reduce_timeout = _get_timeout("reduce")

    if reduce_provider and reduce_model:
        log.info("REDUCE LLM: using %s / %s (timeout=%ds)", reduce_provider, reduce_model, reduce_timeout)
        return get_llm(provider=reduce_provider, model=reduce_model, temperature=temperature, timeout=reduce_timeout)

    if reduce_provider:
        log.info("REDUCE LLM: using provider %s with default model (timeout=%ds)", reduce_provider, reduce_timeout)
        return get_llm_with_fallback(provider=reduce_provider, temperature=temperature)

    # No REDUCE-specific config — use standard model with reduce timeout
    log.info("REDUCE LLM: no REDUCE-specific config, using default model (timeout=%ds)", reduce_timeout)
    return get_llm_with_fallback(provider=provider, model=model, temperature=temperature)


def _api_key_env(provider: str) -> str:
    """Return the environment variable name for a provider's API key."""
    return {
        "anthropic": "ANTHROPIC_API_KEY",
        "openai": "OPENAI_API_KEY",
        "google": "GOOGLE_API_KEY",
        "groq": "GROQ_API_KEY",
    }.get(provider, "")


def _get_timeout(phase: str = "default") -> int:
    """Return LLM request timeout in seconds, differentiated by phase.

    Args:
        phase: "map" for per-table summaries (fast, 30s default),
               "reduce" for cross-table analysis (slow, 120s default),
               "default" for general use (60s default).
    """
    from file_profiler.config.env import LLM_TIMEOUT, LLM_MAP_TIMEOUT, LLM_REDUCE_TIMEOUT
    if phase == "map":
        return LLM_MAP_TIMEOUT
    if phase == "reduce":
        return LLM_REDUCE_TIMEOUT
    return LLM_TIMEOUT


def _make_anthropic(model: str, temperature: float, timeout: int = 0) -> BaseChatModel:
    try:
        from langchain_anthropic import ChatAnthropic
    except ImportError as exc:
        raise ImportError(
            "langchain-anthropic is required for the Anthropic provider. "
            "Install it with: pip install langchain-anthropic"
        ) from exc
    return ChatAnthropic(
        model=model, temperature=temperature,
        timeout=timeout or _get_timeout(), max_retries=2,
    )


def _make_openai(model: str, temperature: float, timeout: int = 0) -> BaseChatModel:
    try:
        from langchain_openai import ChatOpenAI
    except ImportError as exc:
        raise ImportError(
            "langchain-openai is required for the OpenAI provider. "
            "Install it with: pip install langchain-openai"
        ) from exc
    return ChatOpenAI(
        model=model, temperature=temperature,
        timeout=timeout or _get_timeout(), max_retries=2,
    )


def _make_google(model: str, temperature: float, timeout: int = 0) -> BaseChatModel:
    try:
        from langchain_google_genai import ChatGoogleGenerativeAI
    except ImportError as exc:
        raise ImportError(
            "langchain-google-genai is required for the Google provider. "
            "Install it with: pip install langchain-google-genai"
        ) from exc
    return ChatGoogleGenerativeAI(
        model=model, temperature=temperature,
        timeout=timeout or _get_timeout(), max_retries=2,
    )


def _make_groq(model: str, temperature: float, timeout: int = 0) -> BaseChatModel:
    try:
        from langchain_groq import ChatGroq
    except ImportError as exc:
        raise ImportError(
            "langchain-groq is required for the Groq provider. "
            "Install it with: pip install langchain-groq"
        ) from exc
    return ChatGroq(
        model=model, temperature=temperature,
        timeout=timeout or _get_timeout(), max_retries=2,
        api_key=os.getenv("GROQ_API_KEY"),
    )
