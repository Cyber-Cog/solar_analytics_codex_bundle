@echo off
cd /d "%~dp0"
if not defined DB_STATEMENT_TIMEOUT_MS set "DB_STATEMENT_TIMEOUT_MS=300000"
echo DB_STATEMENT_TIMEOUT_MS=%DB_STATEMENT_TIMEOUT_MS%
echo Running snapshot precompute (all active plants, or pass plant_id as arg^)...
python scripts\run_precompute_once.py %*
echo Exit code %ERRORLEVEL%
pause
