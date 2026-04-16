"""Optional LangSmith tracing helpers.

The application should keep running when LangSmith is not installed or not
configured.  This module centralizes that behavior and keeps trace metadata
privacy-aware by default.
"""

from __future__ import annotations

from contextlib import contextmanager, nullcontext
import functools
import hashlib
import inspect
import os
import random
from typing import Any, Callable, Iterable
from urllib.parse import urlparse

from file_profiler.config.runtime_config import get_config


def _flag_enabled(name: str, default: str = "0") -> bool:
    value = get_config(name, default).strip().lower()
    return value in {"1", "true", "yes", "on"}


def _float_config(name: str, default: float) -> float:
    raw = get_config(name, str(default)).strip()
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return default
    return max(0.0, min(1.0, value))


LANGSMITH_TRACING = _flag_enabled("LANGSMITH_TRACING", "0")
LANGSMITH_PROJECT = get_config("LANGSMITH_PROJECT", "file-profiler").strip()
LANGSMITH_ENDPOINT = get_config("LANGSMITH_ENDPOINT", "").strip()
LANGSMITH_SAMPLE_RATE = _float_config("LANGSMITH_SAMPLE_RATE", 1.0)
LANGSMITH_HIDE_SAMPLE_VALUES = _flag_enabled("LANGSMITH_HIDE_SAMPLE_VALUES", "1")
LANGSMITH_PROMPTS_ENABLED = _flag_enabled("LANGSMITH_PROMPTS_ENABLED", "0")
LANGSMITH_PROMPT_TAG = get_config("LANGSMITH_PROMPT_TAG", "production").strip()
APP_ENV = get_config("APP_ENV", get_config("ENVIRONMENT", "local")).strip() or "local"


def configure_langsmith_env() -> None:
    """Expose LangSmith settings to LangChain's automatic tracing hooks."""
    if not LANGSMITH_TRACING:
        return

    os.environ.setdefault("LANGSMITH_TRACING", "true")
    # Kept for compatibility with older LangChain integrations.
    os.environ.setdefault("LANGCHAIN_TRACING_V2", "true")
    if LANGSMITH_PROJECT:
        os.environ.setdefault("LANGSMITH_PROJECT", LANGSMITH_PROJECT)
        os.environ.setdefault("LANGCHAIN_PROJECT", LANGSMITH_PROJECT)
    if LANGSMITH_ENDPOINT:
        os.environ.setdefault("LANGSMITH_ENDPOINT", LANGSMITH_ENDPOINT)


def _load_langsmith() -> Any | None:
    try:
        import langsmith as ls
    except Exception:
        return None
    return ls


def is_enabled() -> bool:
    return LANGSMITH_TRACING and _load_langsmith() is not None


def sampled() -> bool:
    return LANGSMITH_SAMPLE_RATE >= 1.0 or random.random() < LANGSMITH_SAMPLE_RATE


def stable_hash(value: Any, length: int = 12) -> str:
    raw = "" if value is None else str(value)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:length]


def safe_name(value: Any, *, kind: str = "value") -> str:
    """Return a privacy-aware identifier for trace metadata."""
    text = "" if value is None else str(value)
    if not LANGSMITH_HIDE_SAMPLE_VALUES:
        return text
    return f"{kind}:{stable_hash(text)}"


def safe_host(url: str | None) -> str:
    if not url:
        return ""
    try:
        return urlparse(url).netloc or urlparse(url).path
    except Exception:
        return ""


def base_tags(*parts: str | None) -> list[str]:
    tags = [f"env:{APP_ENV}"]
    tags.extend(part for part in parts if part)
    return tags


def compact_text_output(output: Any) -> dict[str, Any]:
    if isinstance(output, str):
        return {"output_chars": len(output)}
    if isinstance(output, dict):
        return {"keys": sorted(str(k) for k in output.keys())[:20]}
    if isinstance(output, (list, tuple)):
        return {"items": len(output)}
    return {"type": type(output).__name__}


def compact_vector_output(output: Any) -> dict[str, Any]:
    count = len(output) if hasattr(output, "__len__") else 0
    dim = 0
    try:
        if count:
            dim = len(output[0])
    except Exception:
        dim = 0
    return {"vectors": count, "dimensions": dim}


def extract_llm_usage(output: Any) -> dict[str, Any]:
    """Extract token usage from LLM response (AIMessage) if available."""
    # Try to get usage_metadata from LangChain AIMessage
    if hasattr(output, "usage_metadata") and output.usage_metadata:
        metadata = output.usage_metadata
        result = {
            "input_tokens": metadata.get("input_tokens", 0),
            "output_tokens": metadata.get("output_tokens", 0),
            "total_tokens": metadata.get("total_tokens", 0),
        }
        # Add response info
        if hasattr(output, "content"):
            result["content_length"] = len(str(output.content))
        return result
    
    # Try alternative token fields (some providers use different names)
    if hasattr(output, "response_metadata"):
        resp_meta = output.response_metadata
        if "token_usage" in resp_meta:
            token_usage = resp_meta["token_usage"]
            return {
                "input_tokens": token_usage.get("prompt_tokens", 0),
                "output_tokens": token_usage.get("completion_tokens", 0),
                "total_tokens": token_usage.get("total_tokens", 0),
            }
    
    # Fallback to compact text output
    return compact_text_output(output)


def _filter_traceable_kwargs(func: Callable[..., Any], kwargs: dict[str, Any]) -> dict[str, Any]:
    try:
        params = inspect.signature(func).parameters
    except (TypeError, ValueError):
        return kwargs
    return {key: value for key, value in kwargs.items() if key in params}


def traceable(
    *,
    name: str,
    run_type: str = "chain",
    process_inputs: Callable[[dict[str, Any]], Any] | None = None,
    process_outputs: Callable[[Any], Any] | None = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Return a LangSmith traceable decorator, or a no-op if unavailable."""
    if not LANGSMITH_TRACING:
        return lambda func: func

    ls = _load_langsmith()
    if ls is None or not hasattr(ls, "traceable"):
        return lambda func: func

    configure_langsmith_env()
    kwargs = {
        "name": name,
        "run_type": run_type,
        "process_inputs": process_inputs,
        "process_outputs": process_outputs,
    }
    kwargs = {key: value for key, value in kwargs.items() if value is not None}
    kwargs = _filter_traceable_kwargs(ls.traceable, kwargs)

    try:
        return ls.traceable(**kwargs)
    except TypeError:
        return ls.traceable(name=name, run_type=run_type)


@contextmanager
def trace_context(
    *,
    surface: str,
    flow: str,
    metadata: dict[str, Any] | None = None,
    tags: Iterable[str] = (),
    enabled: bool | None = None,
):
    """Apply LangSmith context for one request/turn if tracing is enabled."""
    configure_langsmith_env()
    should_trace = LANGSMITH_TRACING if enabled is None else enabled
    should_trace = bool(should_trace and sampled())
    ls = _load_langsmith()
    if ls is None or not hasattr(ls, "tracing_context"):
        with nullcontext():
            yield
        return

    trace_tags = base_tags(f"surface:{surface}", f"flow:{flow}", *tags)
    trace_metadata = {
        "surface": surface,
        "flow": flow,
        "sample_rate": LANGSMITH_SAMPLE_RATE,
        **(metadata or {}),
    }

    kwargs = {
        "enabled": should_trace,
        "project_name": LANGSMITH_PROJECT or None,
        "tags": trace_tags,
        "metadata": trace_metadata,
    }
    kwargs = _filter_traceable_kwargs(ls.tracing_context, kwargs)
    try:
        with ls.tracing_context(**kwargs):
            yield
    except TypeError:
        with ls.tracing_context(enabled=should_trace):
            yield


def traced_call(name: str, run_type: str = "chain") -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Convenience decorator for simple internal spans with compact outputs."""
    return traceable(
        name=name,
        run_type=run_type,
        process_outputs=compact_text_output,
    )


def _prompt_to_text(prompt_obj: Any) -> str:
    if isinstance(prompt_obj, str):
        return prompt_obj
    template = getattr(prompt_obj, "template", None)
    if isinstance(template, str):
        return template
    messages = getattr(prompt_obj, "messages", None)
    if messages:
        parts: list[str] = []
        for message in messages:
            prompt = getattr(message, "prompt", message)
            text = getattr(prompt, "template", None) or getattr(prompt, "content", None)
            if isinstance(text, str):
                parts.append(text)
        if parts:
            return "\n\n".join(parts)
    return str(prompt_obj)


def resolve_prompt(prompt_name: str, default: str) -> str:
    """Pull a LangSmith prompt by tag when enabled, otherwise return default."""
    if not LANGSMITH_PROMPTS_ENABLED:
        return default
    ls = _load_langsmith()
    if ls is None or not hasattr(ls, "Client"):
        return default

    tagged_name = prompt_name
    if LANGSMITH_PROMPT_TAG and ":" not in prompt_name:
        tagged_name = f"{prompt_name}:{LANGSMITH_PROMPT_TAG}"

    try:
        client = ls.Client()
        return _prompt_to_text(client.pull_prompt(tagged_name))
    except Exception:
        return default


def describe_profiles(profiles: list[Any]) -> dict[str, Any]:
    table_count = len(profiles)
    row_count = sum(int(getattr(profile, "row_count", 0) or 0) for profile in profiles)
    column_count = sum(len(getattr(profile, "columns", []) or []) for profile in profiles)
    names = [safe_name(getattr(profile, "table_name", ""), kind="table") for profile in profiles[:25]]
    return {
        "table_count": table_count,
        "row_count": row_count,
        "column_count": column_count,
        "tables": names,
    }


def make_traceable_partial(
    func: Callable[..., Any],
    *,
    name: str,
    run_type: str = "chain",
    process_inputs: Callable[[dict[str, Any]], Any] | None = None,
    process_outputs: Callable[[Any], Any] | None = None,
) -> Callable[..., Any]:
    """Wrap call sites without changing import-time behavior for tests."""
    decorator = traceable(
        name=name,
        run_type=run_type,
        process_inputs=process_inputs,
        process_outputs=process_outputs,
    )
    return functools.wraps(func)(decorator(func))
