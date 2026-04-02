FROM python:3.11-slim AS base

# Prevent Python from buffering stdout/stderr
ENV PYTHONUNBUFFERED=1
ENV PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Install dependencies first (layer caching — code changes won't re-download)
COPY pyproject.toml requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY file_profiler/ file_profiler/

# Create data directories
RUN mkdir -p /data/uploads /data/output

# Non-root user for security
RUN useradd --create-home appuser && \
    chown -R appuser:appuser /app /data
USER appuser

EXPOSE 8080

ENTRYPOINT ["python", "-m", "file_profiler"]
CMD ["--transport", "sse", "--host", "0.0.0.0", "--port", "8080"]
