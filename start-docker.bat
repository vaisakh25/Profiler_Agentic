@echo off
REM Docker startup script for Agentic Data Profiler (Windows)

echo.
echo Starting Agentic Data Profiler in Docker...
echo.

REM Check if docker-compose is available
where docker-compose >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo ERROR: docker-compose not found. Please install Docker Desktop.
    exit /b 1
)

REM Parse command line arguments
set PROFILE=
if "%1"=="--web" set PROFILE=--profile web
if "%1"=="-w" set PROFILE=--profile web

if not "%PROFILE%"=="" (
    echo Starting with Web UI enabled...
) else (
    echo Starting MCP servers only ^(use --web to include Web UI^)...
)

REM Check for .env file
if not exist .env (
    echo WARNING: No .env file found.
    if exist .env.example (
        echo Creating .env from .env.example...
        copy .env.example .env
        echo Created .env file. Please edit it with your API keys.
    ) else (
        echo ERROR: No .env.example found. Please create .env manually.
    )
)

REM Build and start services
echo.
echo Building Docker images...
docker-compose %PROFILE% build

echo.
echo Starting services...
docker-compose %PROFILE% up -d

echo.
echo Waiting for services to be healthy...
timeout /t 5 /nobreak >nul

REM Health check
echo.
echo Checking service health...

:CHECK_PROFILER
for /L %%i in (1,1,12) do (
    curl -s http://localhost:8080/health >nul 2>nul
    if not ERRORLEVEL 1 (
        echo [OK] File Profiler MCP Server is healthy ^(port 8080^)
        goto CHECK_CONNECTOR
    )
    echo Waiting for File Profiler MCP ^(attempt %%i/12^)...
    timeout /t 3 /nobreak >nul
)
echo [FAIL] File Profiler MCP Server failed to start
echo        Check logs with: docker-compose logs profiler-mcp
set PROFILER_STATUS=[FAIL]
goto CHECK_CONNECTOR

:CHECK_CONNECTOR
for /L %%i in (1,1,12) do (
    curl -s http://localhost:8081/health >nul 2>nul
    if not ERRORLEVEL 1 (
        echo [OK] Data Connector MCP Server is healthy ^(port 8081^)
        goto SUMMARY
    )
    echo Waiting for Data Connector MCP ^(attempt %%i/12^)...
    timeout /t 3 /nobreak >nul
)
echo [FAIL] Data Connector MCP Server failed to start
echo        Check logs with: docker-compose logs connector-mcp

:SUMMARY
echo.
echo ========================================
echo Service Status:
echo ========================================
echo   File Profiler MCP: http://localhost:8080/health
echo   Connector MCP:     http://localhost:8081/health

if not "%PROFILE%"=="" (
    curl -s http://localhost:8501 >nul 2>nul
    if not ERRORLEVEL 1 (
        echo   Web UI:            http://localhost:8501
    )
)

echo ========================================
echo.
echo Next Steps:
echo   * View logs:    docker-compose logs -f
echo   * Run CLI:      python -m file_profiler.agent --chat
echo   * Stop all:     docker-compose down
echo.
if "%PROFILE%"=="" echo   Tip: Use start-docker.bat --web to enable Web UI
echo.
