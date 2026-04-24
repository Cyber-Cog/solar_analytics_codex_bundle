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

REM Open browser only after port 8000 listens (avoids ERR_CONNECTION_REFUSED).
start /b cmd /c "powershell -NoProfile -ExecutionPolicy Bypass -Command \"$port=%PORT%; $deadline=(Get-Date).AddSeconds(90); while((Get-Date) -lt $deadline) { if (Get-NetTCPConnection -State Listen -LocalPort $port -ErrorAction SilentlyContinue) { Start-Process ('http://127.0.0.1:' + $port + '/'); exit 0 }; Start-Sleep -Milliseconds 400 }; Write-Host '[WARN] Port' $port 'not listening within 90s.'\""

echo  Server running at: http://127.0.0.1:%PORT%
echo  DB_STATEMENT_TIMEOUT_MS=%DB_STATEMENT_TIMEOUT_MS%
echo  Keep this window open. Close it to stop.
echo  ----------------------------------------
echo.

echo [INFO] Applying pending SQL migrations...
python -m migrations.runner auto
echo.

REM "python -m uvicorn" works even when the Scripts dir isn't on PATH.
python -m uvicorn main:app --port %PORT% --host 127.0.0.1

echo.
echo  Server stopped. Press any key to close.
pause >nul
exit /b 0
