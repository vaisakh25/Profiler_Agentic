"""
Environment-based configuration for the MCP server layer.

Pipeline-internal settings remain in config/settings.py (they are tuning
constants, not deployment knobs).  This module covers deployment config
that changes between local dev, Docker, and cloud.
"""

from __future__ import annotations

import os
from pathlib import Path

# Load .env from project root before any os.getenv() calls.
# override=False means real env vars (Docker, CI) always win over .env.
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parents[2] / ".env", override=False)
except ImportError:
    pass  # python-dotenv not installed; rely on shell env

# --- File system ---------------------------------------------------------
DATA_DIR = Path(os.getenv("PROFILER_DATA_DIR", "/data"))
UPLOAD_DIR = Path(os.getenv("PROFILER_UPLOAD_DIR", str(DATA_DIR / "uploads")))
OUTPUT_DIR = Path(os.getenv("PROFILER_OUTPUT_DIR", str(DATA_DIR / "output")))

# --- Upload limits -------------------------------------------------------
MAX_UPLOAD_SIZE_MB: int = int(os.getenv("MAX_UPLOAD_SIZE_MB", "500"))
UPLOAD_TTL_HOURS: int = int(os.getenv("UPLOAD_TTL_HOURS", "1"))

# --- Server --------------------------------------------------------------
DEFAULT_TRANSPORT: str = os.getenv("MCP_TRANSPORT", "stdio")
DEFAULT_HOST: str = os.getenv("MCP_HOST", "0.0.0.0")
DEFAULT_PORT: int = int(os.getenv("MCP_PORT", "8080"))

# --- Parallelism ---------------------------------------------------------
MAX_PARALLEL_WORKERS: int = int(os.getenv("MAX_PARALLEL_WORKERS", "4"))

# --- DuckDB (STREAM_ONLY engine for >2 GB CSVs) -------------------------
DUCKDB_MEMORY_LIMIT: str = os.getenv("DUCKDB_MEMORY_LIMIT", "512MB")
DUCKDB_THREADS: int = int(os.getenv("DUCKDB_THREADS", "4"))

# --- Enrichment (map-reduce) ----------------------------------------------
VECTOR_STORE_DIR = Path(os.getenv("PROFILER_VECTOR_STORE_DIR", str(DATA_DIR / "vector_store")))
MAP_MAX_WORKERS: int = int(os.getenv("ENRICHMENT_MAP_WORKERS", "4"))
MAP_TOKEN_BUDGET: int = int(os.getenv("ENRICHMENT_MAP_TOKEN_BUDGET", "2000"))
REDUCE_TOP_K: int = int(os.getenv("ENRICHMENT_REDUCE_TOP_K", "15"))
REDUCE_TOKEN_BUDGET: int = int(os.getenv("ENRICHMENT_REDUCE_TOKEN_BUDGET", "12000"))

# --- Enrichment (cluster + meta-reduce) -----------------------------------
# Tables per cluster; drives auto cluster-count: ceil(n_tables / target)
CLUSTER_TARGET_SIZE: int = int(os.getenv("ENRICHMENT_CLUSTER_TARGET_SIZE", "15"))
# Token budget for the per-cluster REDUCE prompt (summaries only)
PER_CLUSTER_TOKEN_BUDGET: int = int(os.getenv("ENRICHMENT_PER_CLUSTER_TOKEN_BUDGET", "6000"))
# Token budget for the META-REDUCE prompt (cluster analyses)
META_REDUCE_TOKEN_BUDGET: int = int(os.getenv("ENRICHMENT_META_REDUCE_TOKEN_BUDGET", "8000"))

# --- Logging -------------------------------------------------------------
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT: str = os.getenv(
    "LOG_FORMAT",
    "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
