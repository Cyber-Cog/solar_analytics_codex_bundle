@echo off
setlocal
title Solar Analytics Platform
color 0A
echo.
echo  ======================================
echo    Solar Analytics Platform - Starting
echo  ======================================
echo.

REM START.bat lives inside backend\ so %~dp0 *is* the backend folder.
cd /d "%~dp0"

REM Fail loudly if Python itself is missing rather than limping on.
where python >nul 2>nul
if errorlevel 1 (
    echo [ERROR] Python not found on PATH. Install Python 3.11+ and retry.
    pause
    exit /b 1
)

if not defined DB_STATEMENT_TIMEOUT_MS set "DB_STATEMENT_TIMEOUT_MS=300000"
REM Schema already exists on RDS/local — skip slow create_all on every boot (~15s saved).
if not defined SOLAR_SKIP_CREATE_ALL set "SOLAR_SKIP_CREATE_ALL=1"
REM main.py already runs safe migrations in a background thread; do not block uvicorn here.
if not defined SOLAR_MIGRATIONS_ON_BOOT set "SOLAR_MIGRATIONS_ON_BOOT=background"

REM We are running on port 8000 now.
set "PORT=8000"
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$conn = Get-NetTCPConnection -State Listen -LocalPort 8000 -ErrorAction SilentlyContinue | Select-Object -First 1; " ^
  "if (-not $conn) { exit 0 }; " ^
  "$ownerPid = $conn.OwningProcess; " ^
  "$p = Get-Process -Id $ownerPid -ErrorAction SilentlyContinue; " ^
  "if ($p -and $p.ProcessName -ieq 'python') { " ^
  "  Write-Host ('[INFO] Port 8000 is busy (PID ' + $ownerPid + ', python.exe). Stopping old instance...'); " ^
  "  Stop-Process -Id $ownerPid -Force -ErrorAction SilentlyContinue; Start-Sleep -Seconds 1; exit 0 " ^
  "} " ^
  "Write-Host ('[ERROR] Port 8000 is in use by process: ' + ($(if($p){$p.ProcessName}else{'PID '+$ownerPid}))); " ^
  "exit 1"
if errorlevel 1 (
  echo [ERROR] Could not free port 8000. Close the blocking app and rerun START.bat.
  pause
  exit /b 1
)

echo  Starting server on http://127.0.0.1:%PORT% ...
echo  First boot can take 20-40s while Python connects to the database (AWS RDS).
echo  Do NOT close this window — closing it stops the server.
echo  If the browser shows "refused", wait here until you see "Application startup complete".
echo  ----------------------------------------
echo.

REM Open browser only after HTTP responds (port alone is not enough during slow import).
start /b cmd /c "powershell -NoProfile -ExecutionPolicy Bypass -Command \"$port=%PORT%; $url='http://127.0.0.1:'+$port+'/'; $deadline=(Get-Date).AddSeconds(120); while((Get-Date) -lt $deadline) { try { $r=Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 3; if ($r.StatusCode -eq 200) { Start-Process $url; exit 0 } } catch {}; Start-Sleep -Milliseconds 500 }; Write-Host '[WARN] Server did not respond at' $url 'within 120s. Open that URL manually after startup completes.'\""

echo  DB_STATEMENT_TIMEOUT_MS=%DB_STATEMENT_TIMEOUT_MS%
echo  SOLAR_SKIP_CREATE_ALL=%SOLAR_SKIP_CREATE_ALL%
echo  ----------------------------------------
echo.

REM "python -m uvicorn" works even when the Scripts dir isn't on PATH.
python -m uvicorn main:app --port %PORT% --host 127.0.0.1

echo.
echo  Server stopped. Press any key to close.
pause >nul
exit /b 0
