<#
.SYNOPSIS
    Scheduled Postgres backup for the Solar Analytics DB.

.DESCRIPTION
    Creates a compressed pg_dump (custom format) into $BackupDir, then rotates:
      - keep the most recent 14 daily files
      - keep one weekly file per ISO week for the last 8 weeks
    Anything older is deleted.

    Intentionally writes OUTSIDE the OneDrive-synced repo folder. Default backup
    location is D:\SolarBackups. Override with -BackupDir.

.EXAMPLE
    # Manual run
    pwsh .\scripts\backup_postgres.ps1 -PgPassword "yourpass"

.EXAMPLE
    # Scheduled (see Register-SolarBackupTask.ps1 in this folder)

.NOTES
    Requires pg_dump.exe from a Postgres client installation.
    Expected at C:\Program Files\PostgreSQL\18\bin\pg_dump.exe; override with -PgBin.
#>
[CmdletBinding()]
param(
    [string]$DbHost       = "localhost",
    [int]   $DbPort     = 5432,
    [string]$DbName     = "solar",
    [string]$DbUser     = "solar",
    [string]$PgPassword = $env:PGPASSWORD,
    [string]$BackupDir  = "D:\SolarBackups",
    [string]$PgBin      = "C:\Program Files\PostgreSQL\18\bin",
    [int]   $KeepDaily  = 14,
    [int]   $KeepWeekly = 8
)

$ErrorActionPreference = "Stop"

function Write-Log {
    param([string]$msg)
    $ts = Get-Date -Format "yyyy-MM-dd HH:mm:ss"
    Write-Host "[$ts] $msg"
}

# Resolve pg_dump executable
$PgDump = Join-Path $PgBin "pg_dump.exe"
if (-not (Test-Path $PgDump)) {
    throw "pg_dump not found at $PgDump. Install PostgreSQL client tools or pass -PgBin."
}

if (-not $PgPassword) {
    throw "PGPASSWORD env var not set and -PgPassword not provided."
}

# Ensure backup directory exists
if (-not (Test-Path $BackupDir)) {
    Write-Log "Creating backup directory $BackupDir"
    New-Item -ItemType Directory -Path $BackupDir -Force | Out-Null
}

# Build dump filename: solar_YYYYMMDD_HHmm.dump
$stamp = Get-Date -Format "yyyyMMdd_HHmm"
$dumpFile = Join-Path $BackupDir "solar_${stamp}.dump"

Write-Log "Starting pg_dump of $DbName to $dumpFile"

$env:PGPASSWORD = $PgPassword
try {
    & $PgDump `
        --format=custom `
        --no-owner `
        --no-privileges `
        --host $DbHost `
        --port $DbPort `
        --username $DbUser `
        --dbname $DbName `
        --file $dumpFile

    if ($LASTEXITCODE -ne 0) {
        throw "pg_dump failed with exit code $LASTEXITCODE"
    }
}
finally {
    Remove-Item Env:PGPASSWORD -ErrorAction SilentlyContinue
}

$sizeMb = [math]::Round((Get-Item $dumpFile).Length / 1MB, 2)
Write-Log "Dump complete: $dumpFile ($sizeMb MB)"

# ----- Rotation -----
# Collect all existing dumps, newest first
$dumps = Get-ChildItem -Path $BackupDir -Filter "solar_*.dump" |
         Sort-Object LastWriteTime -Descending

# Keep the newest $KeepDaily dumps unconditionally
$keep = [System.Collections.Generic.HashSet[string]]::new()
$dumps | Select-Object -First $KeepDaily | ForEach-Object { [void]$keep.Add($_.FullName) }

# For weekly retention, keep the first dump we see per (year, ISO-week) for the last $KeepWeekly weeks.
$culture = [System.Globalization.CultureInfo]::InvariantCulture
$cal     = $culture.Calendar
$seenWeeks = @{}
foreach ($d in $dumps) {
    $wk  = $cal.GetWeekOfYear($d.LastWriteTime, [System.Globalization.CalendarWeekRule]::FirstFourDayWeek, [System.DayOfWeek]::Monday)
    $key = "$($d.LastWriteTime.Year)-W$wk"
    if (-not $seenWeeks.ContainsKey($key)) {
        $seenWeeks[$key] = $d.FullName
        if ($seenWeeks.Count -le $KeepWeekly) {
            [void]$keep.Add($d.FullName)
        }
    }
}

# Delete anything not in $keep
$deleted = 0
foreach ($d in $dumps) {
    if (-not $keep.Contains($d.FullName)) {
        Write-Log "Rotating out old dump: $($d.Name)"
        Remove-Item -Path $d.FullName -Force
        $deleted++
    }
}

$remaining = (Get-ChildItem -Path $BackupDir -Filter "solar_*.dump").Count
Write-Log "Rotation done. Deleted $deleted old dump(s). $remaining dump(s) retained in $BackupDir."
