"""API key authentication middleware for FastAPI.

When PROFILER_API_KEY is set, all HTTP requests must include a valid key
via ``Authorization: Bearer <key>`` or ``X-API-Key: <key>`` header.
WebSocket connections authenticate via ``token`` query parameter.

Unauthenticated endpoints: /health, /metrics (configurable).

Multiple keys are supported via comma-separated PROFILER_API_KEY values.
Auth is disabled when PROFILER_API_KEY is empty (default) — zero impact
on local development.
"""

from __future__ import annotations

import hmac
import logging
from typing import Callable

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse

log = logging.getLogger(__name__)

# Paths that never require authentication
_PUBLIC_PATHS = {"/health", "/metrics"}


def _get_valid_keys() -> set[str]:
    """Load valid API keys from environment. Empty set = auth disabled."""
    from file_profiler.config.env import PROFILER_API_KEY
    if not PROFILER_API_KEY:
        return set()
    return {k.strip() for k in PROFILER_API_KEY.split(",") if k.strip()}


def _extract_key(request: Request) -> str | None:
    """Extract API key from request headers."""
    # Authorization: Bearer <key>
    auth = request.headers.get("authorization", "")
    if auth.lower().startswith("bearer "):
        return auth[7:].strip()
    # X-API-Key: <key>
    return request.headers.get("x-api-key")


def _is_valid_key(provided: str, valid_keys: set[str]) -> bool:
    """Constant-time comparison to prevent timing attacks."""
    return any(
        hmac.compare_digest(provided.encode(), key.encode())
        for key in valid_keys
    )


class APIKeyMiddleware(BaseHTTPMiddleware):
    """FastAPI middleware that enforces API key authentication.

    Skips auth for public paths and when no keys are configured.
    """

    async def dispatch(self, request: Request, call_next: Callable):
        valid_keys = _get_valid_keys()

        # Auth disabled — pass through
        if not valid_keys:
            return await call_next(request)

        # Public paths — no auth required
        if request.url.path in _PUBLIC_PATHS:
            return await call_next(request)

        # WebSocket upgrade — check token query param
        if request.headers.get("upgrade", "").lower() == "websocket":
            token = request.query_params.get("token", "")
            if not token or not _is_valid_key(token, valid_keys):
                return JSONResponse(
                    {"error": "Invalid or missing API key"},
                    status_code=401,
                )
            return await call_next(request)

        # Regular HTTP — check headers
        key = _extract_key(request)
        if not key or not _is_valid_key(key, valid_keys):
            return JSONResponse(
                {"error": "Invalid or missing API key. "
                 "Provide via 'Authorization: Bearer <key>' or 'X-API-Key: <key>' header."},
                status_code=401,
            )

        return await call_next(request)


def validate_ws_token(token: str) -> bool:
    """Validate a WebSocket token. For use in WebSocket handlers.

    Returns True if auth is disabled or the token is valid.
    """
    valid_keys = _get_valid_keys()
    if not valid_keys:
        return True
    if not token:
        return False
    return _is_valid_key(token, valid_keys)
