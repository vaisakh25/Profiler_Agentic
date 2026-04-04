@echo off
REM Simple single-container startup script for Windows

echo ================================================
echo Data Profiler - Simple Mode (Single Container)
echo ================================================
echo.

REM Check if Docker is running
docker info >nul 2>&1
if errorlevel 1 (
    echo ERROR: Docker is not running!
    echo Please start Docker Desktop and try again.
    pause
    exit /b 1
)

echo Starting Data Profiler in simple mode...
echo (All services in one container)
echo.

docker-compose --profile simple up -d

if errorlevel 1 (
    echo.
    echo ERROR: Failed to start container!
    pause
    exit /b 1
)

echo.
echo ================================================
echo     SUCCESS! Services are starting...
echo ================================================
echo.
echo Waiting for services to be ready (15 seconds)...
timeout /t 15 /nobreak >nul

echo.
echo Services:
echo   - File Profiler MCP:  http://localhost:8080
echo   - Connector MCP:      http://localhost:8081
echo   - Web UI:             http://localhost:8501
echo.
echo Opening Web UI in your browser...
start http://localhost:8501

echo.
echo To stop:  docker-compose down
echo To view logs:  docker-compose logs -f
echo.
pause
