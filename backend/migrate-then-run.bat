@echo off
REM 1) Start Postgres (Docker)  2) Run app (PostgreSQL only — SQLite removed)
REM Requires Docker Desktop running.

cd /d "%~dp0"

echo [1/2] Starting PostgreSQL...
docker compose up -d 2>nul || docker-compose up -d 2>nul
if errorlevel 1 (
  echo Docker not found. Install Docker Desktop and start it, or run PostgreSQL locally and set DATABASE_URL in backend\.env
  pause
  exit /b 1
)

echo Waiting 8 seconds for Postgres...
timeout /t 8 /nobreak >nul

echo [2/2] Starting app on http://localhost:8080 ...
cd backend
set DATABASE_URL=postgresql://solar:solar@localhost:5432/solar
python -m uvicorn main:app --port 8080 --host 127.0.0.1
