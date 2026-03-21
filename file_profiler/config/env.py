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

# --- DuckDB (accelerated counting & sampling for CSV/Parquet/JSON >50K rows)
def _auto_duckdb_memory() -> str:
    """Auto-scale DuckDB memory to min(system_ram / 4, 4GB), floor 512MB."""
    try:
        import psutil
        total_mb = psutil.virtual_memory().total // (1024 * 1024)
        limit_mb = max(512, min(total_mb // 4, 4096))
        return f"{limit_mb}MB"
    except (ImportError, Exception):
        return "512MB"

DUCKDB_MEMORY_LIMIT: str = os.getenv("DUCKDB_MEMORY_LIMIT", _auto_duckdb_memory())
_cpu_count = os.cpu_count() or 4
DUCKDB_THREADS: int = int(os.getenv("DUCKDB_THREADS", str(_cpu_count)))

# --- Enrichment (map-reduce) ----------------------------------------------
VECTOR_STORE_DIR = Path(os.getenv("PROFILER_VECTOR_STORE_DIR", str(OUTPUT_DIR / "chroma_store")))
MAP_MAX_WORKERS: int = int(os.getenv("ENRICHMENT_MAP_WORKERS", "12"))
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

# --- Column affinity --------------------------------------------------------
COLUMN_AFFINITY_THRESHOLD: float = float(os.getenv("COLUMN_AFFINITY_THRESHOLD", "0.65"))

# --- Batch processing -------------------------------------------------------
BATCH_SIZE: int = int(os.getenv("ENRICHMENT_BATCH_SIZE", "20"))

# --- LLM timeouts -----------------------------------------------------------
LLM_TIMEOUT: int = int(os.getenv("LLM_TIMEOUT", "60"))
# MAP phase (per-table summaries): shorter timeout for faster failure detection
LLM_MAP_TIMEOUT: int = int(os.getenv("LLM_MAP_TIMEOUT", "30"))
# REDUCE / META-REDUCE phases: longer timeout for cross-table analysis
LLM_REDUCE_TIMEOUT: int = int(os.getenv("LLM_REDUCE_TIMEOUT", "120"))

# --- Reduce model (stronger model for REDUCE / META-REDUCE phases) ----------
REDUCE_LLM_PROVIDER: str = os.getenv("REDUCE_LLM_PROVIDER", "")
REDUCE_LLM_MODEL: str = os.getenv("REDUCE_LLM_MODEL", "")

# --- PostgreSQL (chat persistence + session history) ----------------------
POSTGRES_HOST: str = os.getenv("POSTGRES_HOST", "")
POSTGRES_PORT: int = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_USER: str = os.getenv("POSTGRES_USER", "profiler")
POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "")
POSTGRES_DB: str = os.getenv("POSTGRES_DB", "profiler")
POSTGRES_POOL_MIN: int = int(os.getenv("POSTGRES_POOL_MIN", "2"))
POSTGRES_POOL_MAX: int = int(os.getenv("POSTGRES_POOL_MAX", "10"))

def get_postgres_dsn() -> str:
    """Build a PostgreSQL connection string from env vars. Empty if unconfigured.

    URL-encodes the password so special characters (like @, #, %) don't
    break the connection string parsing.
    """
    if not POSTGRES_HOST:
        return ""
    from urllib.parse import quote_plus
    safe_password = quote_plus(POSTGRES_PASSWORD)
    return (
        f"postgresql://{POSTGRES_USER}:{safe_password}"
        f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
    )

# --- Remote connectors (fallback credentials from env) -------------------
# AWS S3
AWS_ACCESS_KEY_ID: str = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY: str = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_DEFAULT_REGION: str = os.getenv("AWS_DEFAULT_REGION", "us-east-1")
AWS_PROFILE: str = os.getenv("AWS_PROFILE", "")

# Azure ADLS Gen2
AZURE_STORAGE_CONNECTION_STRING: str = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "")
AZURE_TENANT_ID: str = os.getenv("AZURE_TENANT_ID", "")
AZURE_CLIENT_ID: str = os.getenv("AZURE_CLIENT_ID", "")
AZURE_CLIENT_SECRET: str = os.getenv("AZURE_CLIENT_SECRET", "")

# Google Cloud Storage
GOOGLE_APPLICATION_CREDENTIALS: str = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")

# Snowflake
SNOWFLAKE_ACCOUNT: str = os.getenv("SNOWFLAKE_ACCOUNT", "")
SNOWFLAKE_USER: str = os.getenv("SNOWFLAKE_USER", "")
SNOWFLAKE_PASSWORD: str = os.getenv("SNOWFLAKE_PASSWORD", "")
SNOWFLAKE_WAREHOUSE: str = os.getenv("SNOWFLAKE_WAREHOUSE", "")

# Remote connector timeout (seconds)
CONNECTOR_TIMEOUT: int = int(os.getenv("CONNECTOR_TIMEOUT", "30"))

# --- Logging -------------------------------------------------------------
LOG_LEVEL: str = os.getenv("LOG_LEVEL", "INFO")
LOG_FORMAT: str = os.getenv(
    "LOG_FORMAT",
    "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)


# ---------------------------------------------------------------------------
# Startup validation
# ---------------------------------------------------------------------------

def _validate_config() -> None:
    """Validate configuration values at import time.

    Raises ValueError with a clear message for any invalid setting.
    Called automatically when the module is first imported.
    """
    import multiprocessing
    errors: list[str] = []

    cpu_count = multiprocessing.cpu_count()

    # Positive integer checks with upper bounds
    _int_bounds = {
        "MAX_PARALLEL_WORKERS": (MAX_PARALLEL_WORKERS, 1, cpu_count * 4),
        "MAX_UPLOAD_SIZE_MB": (MAX_UPLOAD_SIZE_MB, 1, 10_000),
        "UPLOAD_TTL_HOURS": (UPLOAD_TTL_HOURS, 1, 720),
        "DEFAULT_PORT": (DEFAULT_PORT, 1, 65535),
        "DUCKDB_THREADS": (DUCKDB_THREADS, 1, cpu_count * 4),
        "MAP_MAX_WORKERS": (MAP_MAX_WORKERS, 1, 64),
        "MAP_TOKEN_BUDGET": (MAP_TOKEN_BUDGET, 100, 100_000),
        "REDUCE_TOP_K": (REDUCE_TOP_K, 1, 1000),
        "REDUCE_TOKEN_BUDGET": (REDUCE_TOKEN_BUDGET, 100, 500_000),
        "CLUSTER_TARGET_SIZE": (CLUSTER_TARGET_SIZE, 2, 500),
        "PER_CLUSTER_TOKEN_BUDGET": (PER_CLUSTER_TOKEN_BUDGET, 100, 500_000),
        "META_REDUCE_TOKEN_BUDGET": (META_REDUCE_TOKEN_BUDGET, 100, 500_000),
        "BATCH_SIZE": (BATCH_SIZE, 1, 100),
    }

    for name, (value, lo, hi) in _int_bounds.items():
        if not (lo <= value <= hi):
            errors.append(f"{name}={value} is out of range [{lo}, {hi}]")

    # Float range checks
    if not (0.0 < COLUMN_AFFINITY_THRESHOLD <= 1.0):
        errors.append(
            f"COLUMN_AFFINITY_THRESHOLD={COLUMN_AFFINITY_THRESHOLD} "
            f"must be in (0.0, 1.0]"
        )

    # Transport must be valid
    valid_transports = {"stdio", "sse", "streamable-http"}
    if DEFAULT_TRANSPORT not in valid_transports:
        errors.append(
            f"MCP_TRANSPORT='{DEFAULT_TRANSPORT}' is not valid. "
            f"Must be one of: {', '.join(sorted(valid_transports))}"
        )

    if errors:
        import logging
        _log = logging.getLogger(__name__)
        for err in errors:
            _log.error("Config validation failed: %s", err)
        raise ValueError(
            "Invalid configuration:\n  " + "\n  ".join(errors)
        )


_validate_config()
