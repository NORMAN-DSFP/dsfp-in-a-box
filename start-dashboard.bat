@echo off
echo.
echo ==============================
echo   DSFP in a Box
echo ==============================
echo.

echo Starting all services...
docker-compose up -d

echo.
echo Waiting for services to start...
timeout /t 15 /nobreak >nul

echo.
echo ==============================
echo   Services Status
echo ==============================
docker-compose ps

echo.
echo ==============================
echo   Access Points
echo ==============================
echo.
echo 📊 Status Dashboard:     http://localhost:9000
echo 🔍 Elasticsearch:       http://localhost:9200
echo 🧪 DSFP Server:         http://localhost:3333
echo 🔬 Semiquantification:  http://localhost:8001
echo 📈 Spectral Similarity: http://localhost:8002
echo.
echo ==============================
echo   Dashboard Features
echo ==============================
echo.
echo ✅ Simple container status view
echo ✅ Start/Stop/Restart controls
echo ✅ System health monitoring
echo ✅ Auto-refresh every 30 seconds
echo ✅ Ready for custom extensions
echo.
echo Press any key to open the dashboard...
pause >nul
start http://localhost:9000
