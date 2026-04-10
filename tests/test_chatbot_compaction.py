"""Tests for tool-call-safe message compaction in chatbot/web retry paths."""

from __future__ import annotations

from langchain_core.messages import AIMessage, HumanMessage, SystemMessage, ToolMessage

from file_profiler.agent.chatbot import (
    _compact_messages_preserving_tool_pairs,
    _validate_and_recover_tool_chain,
)


def _tool_call_ai(call_id: str) -> AIMessage:
    return AIMessage(
        content="",
        tool_calls=[
            {
                "name": "detect_relationships",
                "args": {"dir_path": "./data"},
                "id": call_id,
                "type": "tool_call",
            }
        ],
    )


def test_compaction_preserves_tool_parent_for_included_tool_message() -> None:
    messages = [
        SystemMessage(content="sys"),
        HumanMessage(content="profile data"),
        _tool_call_ai("call-1"),
        ToolMessage(content="{}", tool_call_id="call-1"),
        HumanMessage(content="continue"),
        AIMessage(content="step 1"),
        HumanMessage(content="next"),
        AIMessage(content="step 2"),
        HumanMessage(content="next 2"),
        AIMessage(content="done"),
    ]

    # A naive suffix slice would keep the ToolMessage and drop its parent AI tool_call.
    compact = _compact_messages_preserving_tool_pairs(messages, max_non_system_messages=7)

    tool_idx = next(
        i for i, msg in enumerate(compact)
        if isinstance(msg, ToolMessage) and msg.tool_call_id == "call-1"
    )
    parent_idx = next(
        i for i, msg in enumerate(compact)
        if isinstance(msg, AIMessage)
        and msg.tool_calls
        and any(str(tc.get("id")) == "call-1" for tc in msg.tool_calls)
    )

    assert parent_idx < tool_idx


def test_compaction_keeps_messages_when_under_limit() -> None:
    messages = [
        SystemMessage(content="sys"),
        HumanMessage(content="hello"),
        AIMessage(content="hi"),
        HumanMessage(content="status"),
        AIMessage(content="ok"),
    ]

    compact = _compact_messages_preserving_tool_pairs(messages, max_non_system_messages=20)

    assert len(compact) == len(messages)


def test_tool_chain_recovery_trims_dangling_tool_call_and_keeps_latest_human() -> None:
    messages = [
        SystemMessage(content="sys"),
        HumanMessage(content="first"),
        _tool_call_ai("call-1"),  # missing matching ToolMessage
        HumanMessage(content="second intent"),
    ]

    recovered, changed = _validate_and_recover_tool_chain(messages)

    assert changed is True
    assert isinstance(recovered[-1], HumanMessage)
    assert recovered[-1].content == "second intent"
    assert not any(
        isinstance(msg, AIMessage) and msg.tool_calls
        for msg in recovered
    )


def test_tool_chain_validation_allows_executable_tail_tool_call() -> None:
    messages = [
        SystemMessage(content="sys"),
        HumanMessage(content="profile"),
        _tool_call_ai("call-1"),
    ]

    recovered, changed = _validate_and_recover_tool_chain(
        messages,
        allow_pending_tail_tool_calls=True,
    )

    assert changed is False
    assert recovered == messages
