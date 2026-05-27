# Quick local server diagnostic — run from backend folder:
#   powershell -ExecutionPolicy Bypass -File check_server.ps1
$port = if ($env:PORT) { [int]$env:PORT } else { 8000 }
$url = "http://127.0.0.1:$port/"
Write-Host "Solar Analytics - local check (port $port)" -ForegroundColor Cyan
Write-Host ""

$listeners = Get-NetTCPConnection -State Listen -LocalPort $port -ErrorAction SilentlyContinue
if (-not $listeners) {
    Write-Host 'FAIL: Nothing is listening on port' $port -ForegroundColor Red
    Write-Host '  Start the app: double-click START.bat or run:'
    Write-Host '    cd backend'
    Write-Host "    python -m uvicorn main:app --host 127.0.0.1 --port $port"
    Write-Host ''
    Write-Host '  Keep the terminal OPEN. Closing it stops the server.'
    exit 1
}

$ownerPid = ($listeners | Select-Object -First 1).OwningProcess
$proc = Get-Process -Id $ownerPid -ErrorAction SilentlyContinue
Write-Host 'OK: Port' $port 'in use by' $proc.ProcessName '(PID' $ownerPid ')' -ForegroundColor Green

try {
    $r = Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 8
    Write-Host 'OK: HTTP' $r.StatusCode 'at' $url -ForegroundColor Green
} catch {
    Write-Host 'FAIL: Port open but page not ready:' $_.Exception.Message -ForegroundColor Red
    Write-Host '  Wait for Application startup complete in the server window (RDS can take 20-40s).'
    exit 1
}

try {
    $h = Invoke-WebRequest -Uri "http://127.0.0.1:$port/health" -UseBasicParsing -TimeoutSec 5
    Write-Host 'OK: /health ->' $h.Content -ForegroundColor Green
} catch {
    Write-Host 'WARN: /health not reachable yet' -ForegroundColor Yellow
}

Write-Host ''
Write-Host 'Open in browser:' $url -ForegroundColor Cyan
exit 0
