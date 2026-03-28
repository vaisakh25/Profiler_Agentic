"""Allow running the agent as ``python -m file_profiler.agent``.

Modes:
  python -m file_profiler.agent --chat                     # interactive chatbot
  python -m file_profiler.agent --web                      # web UI (FastAPI)
  python -m file_profiler.agent --data-path ./data/files   # autonomous profiling
"""

import asyncio
import argparse
import sys

# psycopg3 async requires SelectorEventLoop on Windows.
# Must be set before ANY event loop is created (including by LangGraph/uvicorn).
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())


def main():
    parser = argparse.ArgumentParser(
        description="LangGraph Data Profiling Agent",
        epilog=(
            "Start both MCP servers first:\n"
            "  python -m file_profiler --transport sse --port 8080\n"
            "  python -m file_profiler.connectors --transport sse --port 8081\n\n"
            "Then run the chatbot:\n"
            "  python -m file_profiler.agent --chat\n\n"
            "Or run in autonomous mode:\n"
            "  python -m file_profiler.agent --data-path ./data/files"
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--chat",
        action="store_true",
        help="Launch the interactive chatbot.",
    )
    parser.add_argument(
        "--web",
        action="store_true",
        help="Launch the web UI (FastAPI + HTML).",
    )
    parser.add_argument(
        "--web-port",
        type=int,
        default=8501,
        help="Port for the web UI (default: 8501).",
    )
    parser.add_argument(
        "--data-path",
        default=None,
        help="Path to a data directory or file to profile (autonomous mode).",
    )
    parser.add_argument(
        "--mode",
        choices=["autonomous", "interactive"],
        default="autonomous",
        help="Execution mode for --data-path runs (default: autonomous).",
    )
    parser.add_argument(
        "--mcp-url",
        default="http://localhost:8080/sse",
        help="URL of the file-profiler MCP server (default: http://localhost:8080/sse).",
    )
    parser.add_argument(
        "--connector-mcp-url",
        default=None,
        help="URL of the connector MCP server (default: derived from --mcp-url).",
    )
    parser.add_argument(
        "--provider",
        choices=["anthropic", "openai", "google"],
        default=None,
        help="LLM provider (default: from LLM_PROVIDER env var or anthropic).",
    )
    parser.add_argument(
        "--model",
        default=None,
        help="LLM model name override (default: from LLM_MODEL env var).",
    )
    args = parser.parse_args()

    if args.chat:
        from file_profiler.agent.chatbot import main as chatbot_main
        chatbot_main(
            mcp_url=args.mcp_url,
            connector_mcp_url=args.connector_mcp_url,
            provider=args.provider,
            model=args.model,
        )
    elif args.web:
        from file_profiler.agent.web_server import run as web_run
        web_run(port=args.web_port)
    elif args.data_path:
        from file_profiler.agent.cli import main as cli_main
        # Re-inject args so the CLI parser picks them up
        sys.argv = [
            "file_profiler.agent",
            "--data-path", args.data_path,
            "--mode", args.mode,
            "--mcp-url", args.mcp_url,
        ]
        if args.connector_mcp_url:
            sys.argv += ["--connector-mcp-url", args.connector_mcp_url]
        if args.provider:
            sys.argv += ["--provider", args.provider]
        if args.model:
            sys.argv += ["--model", args.model]
        cli_main()
    else:
        parser.print_help()
        print("\nError: specify either --chat, --web, or --data-path")
        sys.exit(1)


main()
