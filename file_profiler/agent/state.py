"""LangGraph state schema for the data profiling agent."""

from __future__ import annotations

from typing import Annotated, Any

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


class PlannerState(TypedDict):
    """State for the two-tier planning agent graph.

    Extends AgentState with plan decomposition and specialist tracking.

    Attributes:
        messages:            Conversation history.
        mode:                Execution mode.
        plan:                List of plan steps from the planner.
        completed_steps:     Steps the planner has marked done.
        specialist_results:  Results collected from specialist sub-agents.
        current_step:        Index of the current plan step being executed.
    """

    messages: Annotated[list[BaseMessage], add_messages]
    mode: str
    plan: list[dict[str, Any]]
    completed_steps: list[str]
    specialist_results: list[dict[str, Any]]
    current_step: int
