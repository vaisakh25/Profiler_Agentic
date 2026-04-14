"""NVIDIA embedding provider for vector-store enrichment.

This module exposes a LangChain-compatible embeddings implementation backed
by NVIDIA's OpenAI-compatible API endpoint.
"""

from __future__ import annotations

import importlib
import logging
from typing import Any, Optional, Protocol

from file_profiler.observability.langsmith import compact_vector_output, safe_host, traceable

log = logging.getLogger(__name__)

DEFAULT_NVIDIA_BASE_URL = "https://integrate.api.nvidia.com/v1"
DEFAULT_NVIDIA_EMBEDDING_MODEL = "nvidia/llama-3.2-nemoretriever-300m-embed-v1"


class EmbeddingFunction(Protocol):
    """Minimal embedding interface expected by LangChain vector stores."""

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        ...

    def embed_query(self, text: str) -> list[float]:
        ...


def _trace_embed_inputs(inputs: dict) -> dict:
    texts = inputs.get("texts") or []
    self_obj = inputs.get("self")
    return {
        "input_type": inputs.get("input_type"),
        "text_count": len(texts),
        "total_chars": sum(len(text or "") for text in texts),
        "batch_size": getattr(self_obj, "batch_size", None),
        "model": getattr(self_obj, "model", None),
    }


class NvidiaOpenAIEmbeddings:
    """LangChain Embeddings wrapper around NVIDIA's OpenAI-compatible API."""

    def __init__(
        self,
        api_key: str,
        base_url: str = DEFAULT_NVIDIA_BASE_URL,
        model: str = DEFAULT_NVIDIA_EMBEDDING_MODEL,
        batch_size: int = 64,
        timeout: int = 60,
    ) -> None:
        if not api_key:
            raise ValueError("NVIDIA_API_KEY is required for embeddings")

        try:
            openai_module = importlib.import_module("openai")
            OpenAI = getattr(openai_module, "OpenAI", None)
            if OpenAI is None:
                raise ImportError("OpenAI client class not found")
        except ImportError as exc:
            raise ImportError(
                "The openai package is required for NVIDIA embeddings. "
                "Install it with: pip install openai"
            ) from exc

        self.model = model
        self.batch_size = max(1, batch_size)
        self._client: Any = OpenAI(
            api_key=api_key,
            base_url=base_url,
            timeout=timeout,
        )

    @traceable(
        name="embeddings.nvidia_batch",
        run_type="embedding",
        process_inputs=_trace_embed_inputs,
        process_outputs=compact_vector_output,
    )
    def _embed(self, texts: list[str], input_type: str) -> list[list[float]]:
        if not texts:
            return []

        vectors: list[list[float]] = []
        for i in range(0, len(texts), self.batch_size):
            batch = [(text or "") for text in texts[i : i + self.batch_size]]
            response = self._client.embeddings.create(
                input=batch,
                model=self.model,
                encoding_format="float",
                extra_body={
                    "input_type": input_type,
                    "truncate": "END",
                },
            )

            ordered = sorted(
                response.data,
                key=lambda item: getattr(item, "index", 0),
            )
            vectors.extend([list(item.embedding) for item in ordered])

        return vectors

    def embed_documents(self, texts: list[str]) -> list[list[float]]:
        """Embed document/passages for storage and similarity search."""
        return self._embed(texts, input_type="passage")

    def embed_query(self, text: str) -> list[float]:
        """Embed query text for retrieval."""
        vectors = self._embed([text], input_type="query")
        return vectors[0] if vectors else []


def get_embedding_function(
    api_key: Optional[str] = None,
    base_url: Optional[str] = None,
    model: Optional[str] = None,
    batch_size: Optional[int] = None,
    timeout: Optional[int] = None,
) -> EmbeddingFunction:
    """Build an embeddings instance using NVIDIA environment-backed defaults."""
    from file_profiler.config.env import (
        NVIDIA_API_KEY,
        NVIDIA_BASE_URL,
        NVIDIA_EMBED_BATCH_SIZE,
        NVIDIA_EMBED_TIMEOUT,
        NVIDIA_EMBEDDING_MODEL,
    )

    resolved_api_key = (api_key or NVIDIA_API_KEY).strip()
    resolved_base_url = (base_url or NVIDIA_BASE_URL).strip() or DEFAULT_NVIDIA_BASE_URL
    resolved_model = (model or NVIDIA_EMBEDDING_MODEL).strip() or DEFAULT_NVIDIA_EMBEDDING_MODEL
    resolved_batch_size = batch_size if batch_size is not None else NVIDIA_EMBED_BATCH_SIZE
    resolved_timeout = timeout if timeout is not None else NVIDIA_EMBED_TIMEOUT

    if not resolved_api_key:
        raise ValueError(
            "NVIDIA_API_KEY is not configured. Set NVIDIA_API_KEY to enable embeddings."
        )

    log.debug(
        "Creating NVIDIA embeddings client (model=%s, base_host=%s, batch_size=%d)",
        resolved_model,
        safe_host(resolved_base_url),
        resolved_batch_size,
    )

    return NvidiaOpenAIEmbeddings(
        api_key=resolved_api_key,
        base_url=resolved_base_url,
        model=resolved_model,
        batch_size=resolved_batch_size,
        timeout=resolved_timeout,
    )
