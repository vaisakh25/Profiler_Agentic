# Deployment Readiness Guide

This project now uses layered test gates so you can validate core behavior before release and run heavier checks only when needed.

## Gate Layers

1. Deterministic gate (default CI and local pre-commit)
2. API and smoke gate (runtime-critical paths)
3. Docker health gate (containerized deployment path)
4. Extended E2E gate (manual only)

## Local Commands

Run from repository root.

```bash
# 1) Deterministic tests
pytest --maxfail=1 --ignore=tests/test_deployment_smoke.py

# 2) Runtime smoke + API integration
pytest tests/test_deployment_smoke.py::test_file_profiler_mcp_health tests/test_deployment_smoke.py::test_connector_mcp_health tests/test_web_api_integration.py --maxfail=1

# 3) Docker gate (requires Docker)
pytest tests/test_deployment_smoke.py::test_docker_compose_health --run-docker --maxfail=1

# 4) Extended E2E tests (manual only)
pytest tests/test_chatbot_e2e.py tests/test_chatbot_progress_e2e.py tests/test_enrichment_e2e.py tests/test_llm_factory.py tests/test_ws.py --maxfail=1
```

## CI Workflows

- .github/workflows/ci-gates.yml
  - deterministic-tests
  - api-smoke
  - docker-health (runs on main and workflow_dispatch)
- .github/workflows/live-provider-gate.yml
  - manual workflow_dispatch for extended end-to-end tests

## Environment Hygiene

1. Use .env.example as the baseline template.
2. Keep real secrets in deployment secret stores, not in git.
3. Rotate any key that was previously committed.
4. Set at least one provider API key matching LLM_PROVIDER.

## Pre-Deployment Checklist

1. Copy .env.example to .env and populate required values.
2. Run deterministic and smoke gates locally.
3. Build and run docker compose, then run docker smoke gate.
4. Verify /health endpoint for both MCP servers.
5. Confirm output and upload directories are writable in target environment.
6. Validate connector credentials through /api/connections endpoints.

## Latest Local Verification

Date: 2026-04-01

1. Full suite gate
  - Command: pytest --maxfail=1
  - Result: PASS (506 passed, 0 skipped, 0 deselected)

Current status: Full deployment gate is green for local execution.
