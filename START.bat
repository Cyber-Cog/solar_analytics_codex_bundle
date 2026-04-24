@echo off
setlocal EnableDelayedExpansion

cd /d "%~dp0"

echo =========================================
echo   Solar Analytics 1-Click Local Startup  
echo =========================================
echo.

if not exist "backend\.env" (
    echo [INFO] Creating initial backend\.env file from example...
    copy "backend\.env.example" "backend\.env" >nul
)

REM Check if the database URL requires configuration
findstr /C:"DATABASE_URL=postgresql://solar:solar@localhost:5432/solar" "backend\.env" >nul
if !errorlevel! equ 0 (
    echo.
    echo It looks like you haven't set up your AWS Database URL yet.
    set /p AWS_URL="Please enter your AWS PostgreSQL Database URL (postgresql://...): "
    if not "!AWS_URL!"=="" (
        powershell -Command "(Get-Content backend\.env) -replace '^DATABASE_URL=.*', '# Old DATABASE_URL replaced' | Set-Content backend\.env"
        
        echo.>> "backend\.env"
        echo DATABASE_URL=!AWS_URL!>> "backend\.env"
        echo [SUCCESS] Saved your database URL to backend\.env!
    )
)

echo.
echo [INFO] Passing control to backend server...
echo.
cd backend
call START.bat
