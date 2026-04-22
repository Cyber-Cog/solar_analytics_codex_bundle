# Postgres backup and restore

The app uses a single Postgres database (`solar`) as its only source of truth.
Historical `local_db.dump` files that used to live at the repo root have been
removed. Use the rotating scheduled backups documented below.

## Where backups live

- Default location: `D:\SolarBackups\` (outside OneDrive on purpose — we do not
  want every 400 MB dump re-uploaded to the cloud).
- One file per run, named `solar_YYYYMMDD_HHmm.dump` in Postgres custom format.

## Scheduled nightly backup

Files:

- `scripts/backup_postgres.ps1` — runs `pg_dump`, writes the dump, rotates old
  ones. Keeps the most recent 14 daily files plus 1 file per ISO week for the
  last 8 weeks.
- `scripts/Register-SolarBackupTask.ps1` — creates a Windows Task Scheduler
  job (`SolarAnalyticsNightlyBackup`) that runs the above every day at 23:00.

One-time setup (admin PowerShell):

```powershell
cd "D:\WorkingFolder\OneDrive - vikramsolar.com\Desktop\VS Code files\solar_analytics_codex_bundle"
pwsh .\scripts\Register-SolarBackupTask.ps1 -PgPassword "<your solar user pwd>"
```

Run it immediately once to verify:

```powershell
Start-ScheduledTask -TaskName SolarAnalyticsNightlyBackup
Get-ScheduledTaskInfo -TaskName SolarAnalyticsNightlyBackup
```

Or invoke the script directly any time:

```powershell
$env:PGPASSWORD = "<your solar user pwd>"
pwsh .\scripts\backup_postgres.ps1
```

## Restore

```powershell
pwsh .\scripts\restore_postgres.ps1 `
  -DumpFile D:\SolarBackups\solar_20260420_2300.dump `
  -PgPassword "<your solar user pwd>"
```

This uses `pg_restore --clean --if-exists --no-owner --no-privileges` — it will
drop and recreate every object from the dump into the live `solar` database.

### Validate a backup without touching live data

Restore into a scratch database first:

```powershell
pwsh .\scripts\restore_postgres.ps1 `
  -DumpFile D:\SolarBackups\solar_20260420_2300.dump `
  -DbName solar_restore_test `
  -CreateDb `
  -PgPassword "<your solar user pwd>"
```

Then spot-check row counts against the live DB.

## Off-site copy (recommended once hosting)

When the app is deployed, switch from local `pg_dump` to one of:

1. AWS RDS automated snapshots (if `DATABASE_URL` points at RDS — see
   `scripts/RDS_MIGRATION_STEPS.md`). Turn on daily snapshots with 7-day retention
   in the RDS console; no repo scripts needed.
2. Keep the nightly `backup_postgres.ps1` on the host and `robocopy` / `rclone`
   the latest `.dump` file to S3 / Google Drive / another machine once a week.

Never rely on a single on-disk backup — at least two copies, ideally one off-host.
