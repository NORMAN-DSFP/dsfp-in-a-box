@echo off
echo Building and starting DSFP Screening Service...
echo ==============================================

REM Navigate to the project root
cd /d "%~dp0"

REM Build the screening service image
echo Building screening service Docker image...
docker build -t dsfp-screening-service ./models/screening-service/

if %ERRORLEVEL% EQU 0 (
    echo ✅ Screening service image built successfully
) else (
    echo ❌ Failed to build screening service image
    exit /b 1
)

REM Start the screening service using docker-compose
echo Starting screening service...
docker-compose up -d models-screening-service

if %ERRORLEVEL% EQU 0 (
    echo ✅ Screening service started successfully
    echo.
    echo Service is now available at:
    echo - Health check: http://localhost:8003/health
    echo - Screening endpoint: http://localhost:8003/screen
    echo - Dashboard integration: http://localhost:9000/sample-screening.html
    echo.
    echo To check service logs:
    echo docker logs dsfp-screening-service
) else (
    echo ❌ Failed to start screening service
    exit /b 1
)

pause
