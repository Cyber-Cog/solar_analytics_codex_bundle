<#
.SYNOPSIS
    Apply a manual (risky) SQL migration from backend/migrations/manual/.

.DESCRIPTION
    Refuses to run unless a recent Postgres backup exists in $BackupDir. Takes
    one filename (e.g. 010_timestamps_to_native.sql) and delegates to the
    Python runner. Use this for schema changes that rewrite data or alter
    column types.

.EXAMPLE
    pwsh .\scripts\run_manual_migration.ps1 -File 010_timestamps_to_native.sql
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)][string]$File,
    [string]$BackupDir = "D:\SolarBackups",
    [int]$MaxBackupAgeHours = 24,
    [switch]$SkipBackupCheck
)

$ErrorActionPreference = "Stop"

if (-not $SkipBackupCheck) {
    if (-not (Test-Path $BackupDir)) {
        throw "Backup directory $BackupDir does not exist. Run scripts/backup_postgres.ps1 first, or pass -SkipBackupCheck."
    }
    $recent = Get-ChildItem -Path $BackupDir -Filter "solar_*.dump" -ErrorAction SilentlyContinue |
              Where-Object { $_.LastWriteTime -ge (Get-Date).AddHours(-$MaxBackupAgeHours) } |
              Sort-Object LastWriteTime -Descending | Select-Object -First 1
    if (-not $recent) {
        throw "No Postgres backup newer than $MaxBackupAgeHours h in $BackupDir. Run scripts/backup_postgres.ps1 first, or pass -SkipBackupCheck."
    }
    Write-Host "[ok] Recent backup found: $($recent.Name) ($($recent.LastWriteTime))"
}

$backendDir = (Resolve-Path (Join-Path $PSScriptRoot "..\backend")).Path
Push-Location $backendDir
try {
    python -m migrations.runner manual --file $File
}
finally {
    Pop-Location
}
