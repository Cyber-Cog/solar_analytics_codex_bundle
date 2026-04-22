@echo off
REM Start Solar Analytics FastAPI server
set PYTHONPATH=%~dp0
set DATABASE_URL=postgresql://solar:solar@localhost:5432/solar
uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload
