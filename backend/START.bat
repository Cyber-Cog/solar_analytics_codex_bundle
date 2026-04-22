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

REM Always run on 8080 (frontend links depend on this).
set "PORT=8080"
powershell -NoProfile -ExecutionPolicy Bypass -Command ^
  "$conn = Get-NetTCPConnection -State Listen -LocalPort 8080 -ErrorAction SilentlyContinue | Select-Object -First 1; " ^
  "if (-not $conn) { exit 0 }; " ^
  "$ownerPid = $conn.OwningProcess; " ^
  "$p = Get-Process -Id $ownerPid -ErrorAction SilentlyContinue; " ^
  "if ($p -and $p.ProcessName -ieq 'python') { " ^
  "  Write-Host ('[INFO] Port 8080 is busy (PID ' + $ownerPid + ', python.exe). Stopping old instance...'); " ^
  "  Stop-Process -Id $ownerPid -Force -ErrorAction SilentlyContinue; Start-Sleep -Seconds 1; exit 0 " ^
  "} " ^
  "Write-Host ('[ERROR] Port 8080 is in use by process: ' + ($(if($p){$p.ProcessName}else{'PID '+$ownerPid}))); " ^
  "exit 1"
if errorlevel 1 (
    echo [ERROR] Could not free port 8080. Close the blocking app and rerun START.bat.
    pause
    exit /b 1
)

REM Open browser after 4 seconds (backgrounded so it doesn't block uvicorn).
start /b cmd /c "timeout /t 4 /nobreak >nul && start http://localhost:%PORT%"

echo  Server running at: http://localhost:%PORT%
echo  Keep this window open. Close it to stop.
echo  ----------------------------------------
echo.

REM "python -m uvicorn" works even when the Scripts dir isn't on PATH.
python -m uvicorn main:app --port %PORT% --host 127.0.0.1

echo.
echo  Server stopped. Press any key to close.
pause >nul
exit /b 0
