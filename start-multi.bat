@echo off
REM Multi-container startup script for Windows

echo ================================================
echo Data Profiler - Multi Mode (Production)
echo ================================================
echo.
echo Architecture: 4 Containers + Nginx Reverse Proxy
echo - Nginx (Gateway): Port 9050
echo - Web UI: Internal
echo - MCP File Server: Internal
echo - MCP Connector: Internal
echo.

REM Check if Docker is running
docker info >nul 2>&1
if errorlevel 1 (
    echo ERROR: Docker is not running!
    echo Please start Docker Desktop and try again.
    pause
    exit /b 1
)

echo Starting Data Profiler in multi mode...
echo.

docker-compose --profile multi up -d --build

if errorlevel 1 (
    echo.
    echo ERROR: Failed to start containers!
    pause
    exit /b 1
)

echo.
echo ================================================
echo     SUCCESS! Services are starting...
echo ================================================
echo.
echo Waiting for all containers to be healthy (20 seconds)...
timeout /t 20 /nobreak >nul

echo.
echo Services:
echo   - Nginx Gateway:      http://localhost:9050
echo   - Web UI:             http://localhost:9050
echo   - MCP File Server:    Internal (via Nginx)
echo   - MCP Connector:      Internal (via Nginx)
echo.
echo Opening Web UI in your browser...
start http://localhost:9050

echo.
echo To stop:  docker-compose --profile multi down
echo To view logs:  docker-compose --profile multi logs -f
echo.
pause
