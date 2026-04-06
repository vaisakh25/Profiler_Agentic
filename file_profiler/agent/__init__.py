"""LangGraph agent package.

Keep imports lazy so ``python -m file_profiler.agent`` can start even when
optional runtime dependencies are temporarily unavailable.
"""

from __future__ import annotations

from typing import Any

__all__ = ["create_agent", "run_agent", "run_chatbot"]


async def create_agent(*args: Any, **kwargs: Any):
    from file_profiler.agent.graph import create_agent as _create_agent
    return await _create_agent(*args, **kwargs)


async def run_agent(*args: Any, **kwargs: Any):
    from file_profiler.agent.cli import run_agent as _run_agent
    return await _run_agent(*args, **kwargs)


async def run_chatbot(*args: Any, **kwargs: Any):
    from file_profiler.agent.chatbot import run_chatbot as _run_chatbot
    return await _run_chatbot(*args, **kwargs)
