@echo off
REM Hybrid Deployment - MCP Servers in Docker, Web UI Local
REM =========================================================

echo.
echo ========================================
echo   Agentic Profiler - Hybrid Startup
echo ========================================
echo.

echo [1/3] Starting MCP servers in Docker...
docker compose up -d
if %errorlevel% neq 0 (
    echo ERROR: Failed to start Docker containers
    pause
    exit /b 1
)

echo.
echo [2/3] Waiting for MCP servers to initialize...
timeout /t 5 /nobreak >nul

echo.
echo [3/3] Starting Web UI locally...
echo.
echo Press Ctrl+C to stop the Web UI when done.
echo.

REM Start Web UI in foreground (keeps terminal open)
f:\agentic_profiler\Profiler_Agentic\.venv\Scripts\python.exe -m file_profiler.agent --web --web-port 8501 --mcp-url http://localhost:8080/sse --connector-mcp-url http://localhost:8081/sse

echo.
echo Web UI stopped.
echo.
echo To stop Docker MCP servers, run: docker compose down
pause
