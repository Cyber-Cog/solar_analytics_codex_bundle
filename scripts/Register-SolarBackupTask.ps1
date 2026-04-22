<#
.SYNOPSIS
    Register a Windows Task Scheduler job that runs backup_postgres.ps1 nightly.

.DESCRIPTION
    Creates a task named "SolarAnalyticsNightlyBackup" that runs every day at 23:00
    as the current user. The task runs backup_postgres.ps1 with the supplied
    credentials. Backups land in D:\SolarBackups by default (NOT inside OneDrive).

    Re-run this script to update the task.

.EXAMPLE
    # One-time setup (run as administrator)
    pwsh .\scripts\Register-SolarBackupTask.ps1 -PgPassword "yourpass"
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory=$true)][string]$PgPassword,
    [string]$DbHost      = "localhost",
    [int]   $DbPort      = 5432,
    [string]$DbName      = "solar",
    [string]$DbUser      = "solar",
    [string]$BackupDir   = "D:\SolarBackups",
    [string]$PgBin       = "C:\Program Files\PostgreSQL\18\bin",
    [string]$RunTime     = "23:00",
    [string]$TaskName    = "SolarAnalyticsNightlyBackup"
)

$ErrorActionPreference = "Stop"

$scriptPath = (Resolve-Path (Join-Path $PSScriptRoot "backup_postgres.ps1")).Path

# Build PowerShell arguments as a single quoted string
$args = @(
    "-NoProfile", "-ExecutionPolicy", "Bypass",
    "-File", "`"$scriptPath`"",
    "-DbHost", $DbHost,
    "-DbPort", $DbPort,
    "-DbName", $DbName,
    "-DbUser", $DbUser,
    "-BackupDir", "`"$BackupDir`"",
    "-PgBin", "`"$PgBin`"",
    "-PgPassword", "`"$PgPassword`""
) -join " "

$action    = New-ScheduledTaskAction -Execute "powershell.exe" -Argument $args
$trigger   = New-ScheduledTaskTrigger -Daily -At $RunTime
$settings  = New-ScheduledTaskSettingsSet -AllowStartIfOnBatteries -DontStopIfGoingOnBatteries -StartWhenAvailable
$principal = New-ScheduledTaskPrincipal -UserId "$env:USERDOMAIN\$env:USERNAME" -LogonType S4U -RunLevel Limited

# Remove existing task if present
if (Get-ScheduledTask -TaskName $TaskName -ErrorAction SilentlyContinue) {
    Write-Host "[info] Unregistering existing task $TaskName"
    Unregister-ScheduledTask -TaskName $TaskName -Confirm:$false
}

Register-ScheduledTask -TaskName $TaskName `
    -Action $action -Trigger $trigger -Settings $settings -Principal $principal `
    -Description "Solar Analytics nightly Postgres backup (writes to $BackupDir, rotated 14 daily + 8 weekly)."

Write-Host "[ok] Registered scheduled task $TaskName running daily at $RunTime."
Write-Host "     Backups will land in $BackupDir"
Write-Host "     To test now, run:  Start-ScheduledTask -TaskName $TaskName"
