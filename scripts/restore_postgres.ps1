<#
.SYNOPSIS
    Restore a Solar Analytics Postgres backup created by backup_postgres.ps1.

.DESCRIPTION
    Wraps pg_restore with the safe default flags:
      --clean --if-exists --no-owner --no-privileges

    By default restores into the live "solar" database. Pass -DbName solar_restore_test
    to do a dry-run into a side database first.

.EXAMPLE
    pwsh .\scripts\restore_postgres.ps1 -DumpFile D:\SolarBackups\solar_20260420_2300.dump -PgPassword "yourpass"

.EXAMPLE
    # Restore into a scratch database to validate a dump
    pwsh .\scripts\restore_postgres.ps1 -DumpFile D:\SolarBackups\solar_20260420_2300.dump -DbName solar_restore_test -CreateDb
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)][string]$DumpFile,
    [string]$DbHost     = "localhost",
    [int]   $DbPort     = 5432,
    [string]$DbName     = "solar",
    [string]$DbUser     = "solar",
    [string]$PgPassword = $env:PGPASSWORD,
    [string]$PgBin      = "C:\Program Files\PostgreSQL\18\bin",
    [switch]$CreateDb
)

$ErrorActionPreference = "Stop"

$PgRestore = Join-Path $PgBin "pg_restore.exe"
$Psql      = Join-Path $PgBin "psql.exe"
if (-not (Test-Path $PgRestore)) { throw "pg_restore.exe not found at $PgRestore" }
if (-not (Test-Path $DumpFile))  { throw "Dump file not found: $DumpFile" }
if (-not $PgPassword)            { throw "PGPASSWORD env var not set and -PgPassword not provided." }

$env:PGPASSWORD = $PgPassword
try {
    if ($CreateDb) {
        Write-Host "[info] Creating database $DbName (if not exists)"
        & $Psql --host $DbHost --port $DbPort --username $DbUser --dbname postgres `
            --command "CREATE DATABASE `"$DbName`";" 2>$null
    }

    Write-Host "[info] Restoring $DumpFile into $DbName on $DbHost:$DbPort"
    & $PgRestore `
        --clean `
        --if-exists `
        --no-owner `
        --no-privileges `
        --host $DbHost `
        --port $DbPort `
        --username $DbUser `
        --dbname $DbName `
        $DumpFile

    if ($LASTEXITCODE -ne 0 -and $LASTEXITCODE -ne 1) {
        # pg_restore returns 1 for "restored with warnings", which is usually fine.
        throw "pg_restore failed with exit code $LASTEXITCODE"
    }
    Write-Host "[ok] Restore complete."
}
finally {
    Remove-Item Env:PGPASSWORD -ErrorAction SilentlyContinue
}
