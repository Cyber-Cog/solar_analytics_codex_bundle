@echo off
REM Launches the GitHub push script with Windows PowerShell (works when pwsh is not installed).
set "SCRIPT_DIR=%~dp0"
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%SCRIPT_DIR%push-to-github-test-solaranalytics.ps1" %*
exit /b %ERRORLEVEL%
