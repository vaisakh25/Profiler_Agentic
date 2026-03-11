"""
Tests for file_profiler/agent/ package.

Covers:
  - LLM factory (provider selection, defaults, error handling)
  - AgentState schema
  - System prompt content
  - Graph structure (nodes, edges)
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from file_profiler.agent.state import AgentState
from file_profiler.agent.llm_factory import get_llm, _DEFAULT_MODELS
from file_profiler.agent.graph import SYSTEM_PROMPT


# ---------------------------------------------------------------------------
# AgentState
# ---------------------------------------------------------------------------

class TestAgentState:

    def test_state_has_messages_field(self):
        assert "messages" in AgentState.__annotations__

    def test_state_has_mode_field(self):
        assert "mode" in AgentState.__annotations__

    def test_state_messages_type(self):
        # With `from __future__ import annotations`, annotations are strings
        ann = AgentState.__annotations__["messages"]
        # Verify the annotation references Annotated with add_messages
        ann_str = ann if isinstance(ann, str) else str(ann)
        assert "list" in ann_str
        assert "add_messages" in ann_str


# ---------------------------------------------------------------------------
# LLM Factory
# ---------------------------------------------------------------------------

class TestLLMFactory:

    def test_default_models_defined(self):
        assert "anthropic" in _DEFAULT_MODELS
        assert "openai" in _DEFAULT_MODELS
        assert "google" in _DEFAULT_MODELS

    def test_anthropic_provider(self):
        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"}):
            llm = get_llm(provider="anthropic")
            assert llm is not None
            # Check it's a ChatAnthropic instance
            from langchain_anthropic import ChatAnthropic
            assert isinstance(llm, ChatAnthropic)

    def test_unknown_provider_raises(self):
        with pytest.raises(ValueError, match="Unknown LLM provider"):
            get_llm(provider="unknown_provider")

    def test_env_var_provider(self):
        with patch.dict("os.environ", {
            "LLM_PROVIDER": "anthropic",
            "ANTHROPIC_API_KEY": "test-key",
        }):
            llm = get_llm()
            from langchain_anthropic import ChatAnthropic
            assert isinstance(llm, ChatAnthropic)

    def test_model_override(self):
        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"}):
            llm = get_llm(provider="anthropic", model="claude-haiku-4-5-20251001")
            assert llm.model == "claude-haiku-4-5-20251001"

    def test_env_model_override(self):
        with patch.dict("os.environ", {
            "ANTHROPIC_API_KEY": "test-key",
            "LLM_PROVIDER": "anthropic",
            "LLM_MODEL": "claude-haiku-4-5-20251001",
        }):
            llm = get_llm(provider="anthropic")
            assert llm.model == "claude-haiku-4-5-20251001"

    def test_temperature_default(self):
        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"}):
            llm = get_llm(provider="anthropic")
            assert llm.temperature == 0.0

    def test_temperature_override(self):
        with patch.dict("os.environ", {"ANTHROPIC_API_KEY": "test-key"}):
            llm = get_llm(provider="anthropic", temperature=0.7)
            assert llm.temperature == 0.7

    def test_openai_import_error(self):
        """If langchain-openai is not installed, raise ImportError."""
        with patch.dict("sys.modules", {"langchain_openai": None}):
            with pytest.raises(ImportError, match="langchain-openai"):
                get_llm(provider="openai")

    def test_google_import_error(self):
        """If langchain-google-genai is not installed, raise ImportError."""
        with patch.dict("sys.modules", {"langchain_google_genai": None}):
            with pytest.raises(ImportError, match="langchain-google-genai"):
                get_llm(provider="google")


# ---------------------------------------------------------------------------
# System Prompt
# ---------------------------------------------------------------------------

class TestSystemPrompt:

    def test_prompt_mentions_workflow(self):
        assert "list_supported_files" in SYSTEM_PROMPT
        assert "profile_directory" in SYSTEM_PROMPT
        assert "detect_relationships" in SYSTEM_PROMPT
        assert "get_quality_summary" in SYSTEM_PROMPT

    def test_prompt_mentions_report(self):
        assert "Report" in SYSTEM_PROMPT

    def test_prompt_mentions_quality(self):
        assert "quality" in SYSTEM_PROMPT.lower()

    def test_prompt_mentions_reconnaissance(self):
        assert "reconnaissance" in SYSTEM_PROMPT.lower()

    def test_prompt_is_nonempty(self):
        assert len(SYSTEM_PROMPT) > 100


# ---------------------------------------------------------------------------
# Graph Structure (without MCP server)
# ---------------------------------------------------------------------------

class TestGraphStructure:

    @pytest.mark.asyncio
    async def test_create_agent_fails_without_server(self):
        """create_agent should fail when no MCP server is running."""
        from file_profiler.agent.graph import create_agent
        with pytest.raises(Exception):
            # No server running on this port
            await create_agent(
                mcp_server_url="http://localhost:19999/sse",
                provider="anthropic",
            )


# ---------------------------------------------------------------------------
# CLI Argument Parsing
# ---------------------------------------------------------------------------

class TestCLI:

    def test_cli_parser_exists(self):
        from file_profiler.agent.cli import main
        assert callable(main)

    def test_run_agent_exists(self):
        from file_profiler.agent.cli import run_agent
        assert callable(run_agent)

    def test_module_import(self):
        from file_profiler.agent import create_agent, run_agent
        assert callable(create_agent)
        assert callable(run_agent)
