FROM python:3.11-slim AS base

# Prevent Python from buffering stdout/stderr
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Copy build manifests (layer caching — code changes won't re-download)
COPY pyproject.toml requirements.txt ./

# ============================================================================
# STAGE A: LangChain + LangGraph ecosystem
# Grouped by ecosystem — all share langchain-core as a dependency
# ============================================================================
RUN pip install --no-cache-dir --prefer-binary \
    langchain-core==1.2.17 \
    langchain-anthropic==1.3.4 \
    langchain-openai==1.1.7 \
    langchain-google-genai==4.2.1 \
    langchain-groq==1.1.2 \
    langchain-chroma==1.1.0 \
    langchain-mcp-adapters==0.2.1 \
    langgraph==1.0.7 \
    langgraph-checkpoint-postgres==3.0.5

# ============================================================================
# STAGE B: Runtime dependencies (data processing, API, DB, utilities)
# Lightest stage — no heavy ML deps, mostly pre-built wheels
# ============================================================================
RUN pip install --no-cache-dir --prefer-binary \
    pandas==2.3.3 \
    numpy==2.4.2 \
    fastapi==0.128.8 \
    "uvicorn[standard]==0.40.0" \
    duckdb==1.4.4 \
    chromadb \
    "mcp[cli]==1.26.0" \
    "psycopg[binary,pool]==3.2.3" \
    pyarrow==21.0.0 \
    openpyxl==3.1.5 \
    matplotlib==3.10.8 \
    seaborn==0.13.2 \
    python-dotenv==1.2.1 \
    psutil==6.1.0 \
    xlrd==2.0.1 \
    chardet==5.2.0 \
    typing-extensions

# ============================================================================
# DEV: Test dependencies — separate layer, can be excluded in prod builds
# ============================================================================
RUN pip install --no-cache-dir --prefer-binary \
    pytest==8.4.2 \
    pytest-cov==7.0.0 \
    pytest-asyncio==1.3.0

# Copy full application so one image can run MCP, connector, CLI, and web modes
COPY . .

# Create data directories
RUN mkdir -p /data/uploads /data/output

# Non-root user for security
RUN useradd -m -u 1000 appuser && \
    chown -R appuser:appuser /app /data
USER appuser

EXPOSE 8080
EXPOSE 8081
EXPOSE 8501

ENTRYPOINT ["python", "docker_entrypoint.py"]
