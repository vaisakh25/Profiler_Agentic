"""LangGraph state schema for the data profiling agent."""

from __future__ import annotations

from typing import Annotated

from langchain_core.messages import BaseMessage
from langgraph.graph.message import add_messages
from typing_extensions import TypedDict


class AgentState(TypedDict):
    """State for the profiling agent graph.

    Attributes:
        messages: Conversation history (LLM + tool messages). Uses the
                  built-in ``add_messages`` reducer so new messages are
                  appended rather than overwriting.
        mode:     Execution mode — ``"autonomous"`` (default) or
                  ``"interactive"`` (human-in-the-loop).
    """

    messages: Annotated[list[BaseMessage], add_messages]
    mode: str
