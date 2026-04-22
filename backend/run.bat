@echo off
REM One-command run: PostgreSQL (Docker) + Solar Analytics backend (fast DB).
REM Requires: Docker Desktop running, Python with backend/requirements.txt installed.

cd /d "%~dp0"

echo Starting PostgreSQL in Docker...
docker-compose up -d 2>nul
if errorlevel 1 (
  echo Docker not available. Install PostgreSQL locally and set DATABASE_URL in backend\.env then: cd backend ^&^& uvicorn main:app --reload --port 8080
  pause
  exit /b 1
)

echo Waiting for PostgreSQL to be ready...
timeout /t 5 /nobreak >nul

set DATABASE_URL=postgresql://solar:solar@localhost:5432/solar
echo Using PostgreSQL. Starting app on http://localhost:8080
cd backend
uvicorn main:app --reload --port 8080
