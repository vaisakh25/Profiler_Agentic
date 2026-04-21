"""
Unified configuration loader for the MCP server layer.

Load order (highest precedence wins):
  1. Real environment variables (Docker, CI, shell)
  2. .env file (secrets / API keys only)
  3. config.yaml (all non-secret settings)
  4. Hardcoded defaults in this module

All downstream code continues to import from this module:
    from file_profiler.config.env import OUTPUT_DIR, LOG_LEVEL
"""

from __future__ import annotations

import os
from pathlib import Path

# ---------------------------------------------------------------------------
# Step 1: Load .env (secrets only — override=False so real env wins)
# ---------------------------------------------------------------------------
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parents[2] / ".env", override=False)
except ImportError:
    pass  # python-dotenv not installed; rely on shell env

# ---------------------------------------------------------------------------
# Step 2: Load config.yaml
# ---------------------------------------------------------------------------
_yaml_cfg: dict = {}
_CONFIG_YAML_PATH = Path(__file__).resolve().parents[2] / "config.yaml"

try:
    import yaml
    if _CONFIG_YAML_PATH.exists():
        with open(_CONFIG_YAML_PATH, encoding="utf-8") as f:
            _yaml_cfg = yaml.safe_load(f) or {}
except ImportError:
    pass  # PyYAML not installed — fall back to env vars / defaults
except Exception:
    pass  # Malformed YAML — fall back silently


def _y(*keys, default=None):
    """Read a nested key from the loaded YAML config.

    Usage:
        _y("server", "port")            → _yaml_cfg["server"]["port"]
        _y("connectors", "aws", "region") → _yaml_cfg["connectors"]["aws"]["region"]
    """
    node = _yaml_cfg
    for k in keys:
        if isinstance(node, dict):
            node = node.get(k)
        else:
            return default
        if node is None:
            return default
    return node


def _get(env_key: str, *yaml_path: str, default: str = "") -> str:
    """Resolve a config value: env var → YAML → default.

    yaml_path is a sequence of keys for nested YAML lookup:
        _get("MCP_PORT", "server", "port", default="8080")
    """
    val = os.getenv(env_key)
    if val is not None:
        return val
    yaml_val = _y(*yaml_path) if yaml_path else None
    if yaml_val is not None:
        return str(yaml_val)
    return default


def _get_bool(env_key: str, *yaml_path: str, default: bool = False) -> bool:
    """Resolve a boolean value: env var → YAML → default."""
    val = os.getenv(env_key)
    if val is not None:
        return val.lower() in ("true", "1", "yes")
    yaml_val = _y(*yaml_path) if yaml_path else None
    if yaml_val is not None:
        if isinstance(yaml_val, bool):
            return yaml_val
        return str(yaml_val).lower() in ("true", "1", "yes")
    return default


def _get_int(env_key: str, *yaml_path: str, default: int = 0) -> int:
    """Resolve an integer value: env var → YAML → default."""
    val = os.getenv(env_key)
    if val is not None:
        return int(val)
    yaml_val = _y(*yaml_path) if yaml_path else None
    if yaml_val is not None:
        return int(yaml_val)
    return default


def _get_float(env_key: str, *yaml_path: str, default: float = 0.0) -> float:
    """Resolve a float value: env var → YAML → default."""
    val = os.getenv(env_key)
    if val is not None:
        return float(val)
    yaml_val = _y(*yaml_path) if yaml_path else None
    if yaml_val is not None:
        return float(yaml_val)
    return default


# ═══════════════════════════════════════════════════════════════════════════
# Exported configuration variables
# ═══════════════════════════════════════════════════════════════════════════

# --- File system ---------------------------------------------------------
DATA_DIR = Path(_get("PROFILER_DATA_DIR", "file_system", "data_dir", default="/data"))
UPLOAD_DIR = Path(_get("PROFILER_UPLOAD_DIR", "file_system", "upload_dir", default=str(DATA_DIR / "uploads")))
OUTPUT_DIR = Path(_get("PROFILER_OUTPUT_DIR", "file_system", "output_dir", default=str(DATA_DIR / "output")))

# --- Upload limits -------------------------------------------------------
MAX_UPLOAD_SIZE_MB: int = _get_int("MAX_UPLOAD_SIZE_MB", "uploads", "max_size_mb", default=500)
UPLOAD_TTL_HOURS: int = _get_int("UPLOAD_TTL_HOURS", "uploads", "ttl_hours", default=1)

# --- Server --------------------------------------------------------------
DEFAULT_TRANSPORT: str = _get("MCP_TRANSPORT", "server", "transport", default="stdio")
DEFAULT_HOST: str = _get("MCP_HOST", "server", "host", default="0.0.0.0")
DEFAULT_PORT: int = _get_int("MCP_PORT", "server", "port", default=8080)
CONNECTOR_MCP_PORT: int = _get_int("CONNECTOR_MCP_PORT", "server", "connector_port", default=8081)

# --- Parallelism ---------------------------------------------------------
MAX_PARALLEL_WORKERS: int = _get_int("MAX_PARALLEL_WORKERS", "parallelism", "max_workers", default=4)

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


def _resolve_duckdb_memory() -> str:
    raw = _get("DUCKDB_MEMORY_LIMIT", "duckdb", "memory_limit", default="auto")
    return _auto_duckdb_memory() if raw == "auto" else raw


_cpu_count = os.cpu_count() or 4


def _resolve_duckdb_threads() -> int:
    raw = _get("DUCKDB_THREADS", "duckdb", "threads", default="auto")
    return _cpu_count if raw == "auto" else int(raw)


DUCKDB_MEMORY_LIMIT: str = _resolve_duckdb_memory()
DUCKDB_THREADS: int = _resolve_duckdb_threads()

# --- LLM provider / model ------------------------------------------------
# These are also read directly by llm_factory.py via os.getenv().
# Setting them here ensures YAML values are visible as env vars.
_llm_provider = _get("LLM_PROVIDER", "llm", "provider", default="google")
_llm_model = _get("LLM_MODEL", "llm", "model", default="")
if _llm_provider and "LLM_PROVIDER" not in os.environ:
    os.environ["LLM_PROVIDER"] = _llm_provider
if _llm_model and "LLM_MODEL" not in os.environ:
    os.environ["LLM_MODEL"] = _llm_model

# --- Reduce model (stronger model for REDUCE / META-REDUCE phases) -------
REDUCE_LLM_PROVIDER: str = _get("REDUCE_LLM_PROVIDER", "llm", "reduce_provider", default="")
REDUCE_LLM_MODEL: str = _get("REDUCE_LLM_MODEL", "llm", "reduce_model", default="")
if REDUCE_LLM_PROVIDER and "REDUCE_LLM_PROVIDER" not in os.environ:
    os.environ["REDUCE_LLM_PROVIDER"] = REDUCE_LLM_PROVIDER
if REDUCE_LLM_MODEL and "REDUCE_LLM_MODEL" not in os.environ:
    os.environ["REDUCE_LLM_MODEL"] = REDUCE_LLM_MODEL

# --- LLM timeouts ---------------------------------------------------------
LLM_TIMEOUT: int = _get_int("LLM_TIMEOUT", "llm", "timeout", default=60)
LLM_MAP_TIMEOUT: int = _get_int("LLM_MAP_TIMEOUT", "llm", "map_timeout", default=30)
LLM_REDUCE_TIMEOUT: int = _get_int("LLM_REDUCE_TIMEOUT", "llm", "reduce_timeout", default=120)

# --- LLM adaptive backoff (429 / rate-limit handling) --------------------
LLM_429_BACKOFF_MULTIPLIER: float = _get_float("LLM_429_BACKOFF_MULTIPLIER", "llm", "backoff_multiplier", default=2.0)
LLM_429_ADAPTIVE_WINDOW: int = _get_int("LLM_429_ADAPTIVE_WINDOW", "llm", "adaptive_window", default=60)

# --- Provider RPM limits (requests per minute, 0 = unlimited) ------------
_rpm_cfg = _y("llm", "rpm_limits") or {}
PROVIDER_RPM: dict[str, int] = {
    "google": int(os.getenv("GOOGLE_RPM_LIMIT", str(_rpm_cfg.get("google", 15)))),
    "groq": int(os.getenv("GROQ_RPM_LIMIT", str(_rpm_cfg.get("groq", 30)))),
    "openai": int(os.getenv("OPENAI_RPM_LIMIT", str(_rpm_cfg.get("openai", 500)))),
    "anthropic": int(os.getenv("ANTHROPIC_RPM_LIMIT", str(_rpm_cfg.get("anthropic", 50)))),
}

# --- Enrichment (map-reduce) ---------------------------------------------
VECTOR_STORE_DIR = Path(
    _get("PROFILER_VECTOR_STORE_DIR", "vector_store", "dir", default=str(OUTPUT_DIR / "chroma_store"))
)
MAP_MAX_WORKERS: int = _get_int("ENRICHMENT_MAP_WORKERS", "enrichment", "map_workers", default=12)
MAP_TOKEN_BUDGET: int = _get_int("ENRICHMENT_MAP_TOKEN_BUDGET", "enrichment", "map_token_budget", default=2000)
MAP_TOKEN_BUDGET_MAX: int = _get_int("ENRICHMENT_MAP_TOKEN_BUDGET_MAX", "enrichment", "map_token_budget_max", default=16000)
REDUCE_TOP_K: int = _get_int("ENRICHMENT_REDUCE_TOP_K", "enrichment", "reduce_top_k", default=15)
REDUCE_TOKEN_BUDGET: int = _get_int("ENRICHMENT_REDUCE_TOKEN_BUDGET", "enrichment", "reduce_token_budget", default=12000)

# --- Enrichment (cluster + meta-reduce) ----------------------------------
CLUSTER_TARGET_SIZE: int = _get_int("ENRICHMENT_CLUSTER_TARGET_SIZE", "enrichment", "cluster_target_size", default=15)
PER_CLUSTER_TOKEN_BUDGET: int = _get_int("ENRICHMENT_PER_CLUSTER_TOKEN_BUDGET", "enrichment", "per_cluster_token_budget", default=6000)
META_REDUCE_TOKEN_BUDGET: int = _get_int("ENRICHMENT_META_REDUCE_TOKEN_BUDGET", "enrichment", "meta_reduce_token_budget", default=8000)

# --- Column affinity ------------------------------------------------------
COLUMN_AFFINITY_THRESHOLD: float = _get_float("COLUMN_AFFINITY_THRESHOLD", "enrichment", "column_affinity_threshold", default=0.65)

# --- Batch processing -----------------------------------------------------
BATCH_SIZE: int = _get_int("ENRICHMENT_BATCH_SIZE", "enrichment", "batch_size", default=20)

# --- PostgreSQL (chat persistence + session history) ---------------------
POSTGRES_HOST: str = _get("POSTGRES_HOST", "postgres", "host", default="")
POSTGRES_PORT: int = _get_int("POSTGRES_PORT", "postgres", "port", default=5432)
POSTGRES_USER: str = _get("POSTGRES_USER", "postgres", "user", default="profiler")
POSTGRES_PASSWORD: str = os.getenv("POSTGRES_PASSWORD", "")  # secret — .env only
POSTGRES_DB: str = _get("POSTGRES_DB", "postgres", "database", default="profiler")
POSTGRES_POOL_MIN: int = _get_int("POSTGRES_POOL_MIN", "postgres", "pool_min", default=2)
POSTGRES_POOL_MAX: int = _get_int("POSTGRES_POOL_MAX", "postgres", "pool_max", default=10)


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
# Secrets stay in .env; non-secret config from YAML
AWS_ACCESS_KEY_ID: str = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET_ACCESS_KEY: str = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_DEFAULT_REGION: str = _get("AWS_DEFAULT_REGION", "connectors", "aws", "region", default="us-east-1")
AWS_PROFILE: str = _get("AWS_PROFILE", "connectors", "aws", "profile", default="")

# Azure ADLS Gen2 — all secrets
AZURE_STORAGE_CONNECTION_STRING: str = os.getenv("AZURE_STORAGE_CONNECTION_STRING", "")
AZURE_TENANT_ID: str = os.getenv("AZURE_TENANT_ID", "")
AZURE_CLIENT_ID: str = os.getenv("AZURE_CLIENT_ID", "")
AZURE_CLIENT_SECRET: str = os.getenv("AZURE_CLIENT_SECRET", "")

# Google Cloud Storage
GOOGLE_APPLICATION_CREDENTIALS: str = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "")

# Snowflake
SNOWFLAKE_ACCOUNT: str = _get("SNOWFLAKE_ACCOUNT", "connectors", "snowflake", "account", default="")
SNOWFLAKE_USER: str = _get("SNOWFLAKE_USER", "connectors", "snowflake", "user", default="")
SNOWFLAKE_PASSWORD: str = os.getenv("SNOWFLAKE_PASSWORD", "")  # secret — .env only
SNOWFLAKE_WAREHOUSE: str = _get("SNOWFLAKE_WAREHOUSE", "connectors", "snowflake", "warehouse", default="")

# Remote connector timeout (seconds)
CONNECTOR_TIMEOUT: int = _get_int("CONNECTOR_TIMEOUT", "connectors", "timeout", default=30)

# --- Vector store backend -------------------------------------------------
VECTOR_BACKEND: str = _get("VECTOR_BACKEND", "vector_store", "backend", default="auto")

# --- Embedding (Jina API) — secret, .env only ----------------------------
JINA_API_KEY: str = os.getenv("JINA_API_KEY", "")

# --- Multi-user mode ------------------------------------------------------
def _get_multi_user() -> bool:
    val = os.getenv("MULTI_USER_MODE")
    if val is not None:
        return val.lower() in ("true", "1", "yes")
    yaml_val = _yaml_cfg.get("multi_user")
    if yaml_val is not None:
        if isinstance(yaml_val, bool):
            return yaml_val
        return str(yaml_val).lower() in ("true", "1", "yes")
    return False

MULTI_USER_MODE: bool = _get_multi_user()

# --- Authentication — secret, .env only -----------------------------------
PROFILER_API_KEY: str = os.getenv("PROFILER_API_KEY", "")

# --- Logging --------------------------------------------------------------
LOG_LEVEL: str = _get("LOG_LEVEL", "logging", "level", default="INFO")
LOG_FORMAT: str = _get(
    "LOG_FORMAT", "logging", "format",
    default="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
LOG_JSON: bool = _get_bool("LOG_JSON", "logging", "json", default=False)


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
        "CONNECTOR_MCP_PORT": (CONNECTOR_MCP_PORT, 1, 65535),
        "DUCKDB_THREADS": (DUCKDB_THREADS, 1, cpu_count * 4),
        "MAP_MAX_WORKERS": (MAP_MAX_WORKERS, 1, 64),
        "MAP_TOKEN_BUDGET": (MAP_TOKEN_BUDGET, 100, 100_000),
        "MAP_TOKEN_BUDGET_MAX": (MAP_TOKEN_BUDGET_MAX, 1000, 200_000),
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
