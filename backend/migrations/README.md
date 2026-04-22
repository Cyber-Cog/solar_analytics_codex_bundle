# Migrations

Plain SQL files applied in order by a tiny in-house runner
(`backend/migrations/runner.py`). Every file that has been applied is recorded
in a `schema_migrations` Postgres table so it never runs twice.

## Layout

```
backend/migrations/
  runner.py              Python runner + CLI
  sql/                   AUTO-applied at server startup (safe, additive)
    001_raw_data_dedupe_and_unique.sql
    002_brin_on_timestamp.sql
    003_plant_equipment_upsert_support.sql
  manual/                Run ONLY via the CLI. These rewrite data or types.
    010_timestamps_to_native.sql        # TEXT -> TIMESTAMP
    020_partition_raw_data_generic.sql  # Monthly partitions
    021_roll_next_month_partition.sql   # Monthly rollover (idempotent)
```

## CLI

From the `backend/` folder (so imports resolve), after `DATABASE_URL` is set
in `backend/.env`:

```powershell
# Show what has been applied and what is pending:
python -m migrations.runner status

# Apply all pending AUTO migrations (usually already done by server startup):
python -m migrations.runner auto

# Apply one MANUAL migration (after taking a fresh backup!):
python -m migrations.runner manual --file 010_timestamps_to_native.sql
```

`scripts/run_manual_migration.ps1` wraps the last command, refuses to run
unless a recent backup exists in `D:\SolarBackups\`, and is the recommended
entry point on Windows.

## Writing a new migration

1. Pick the next zero-padded number. Safe/additive -> `sql/`. Risky (rewrites,
   type changes) -> `manual/`.
2. Name it `NNN_short_description.sql`.
3. Wrap the body in `BEGIN; ... COMMIT;`. The runner also wraps the whole file
   in a transaction, so the inner BEGIN/COMMIT is mainly documentation; use
   it anyway so the file is also runnable via `psql -f`.
4. Make it idempotent (`IF NOT EXISTS`, `ON CONFLICT DO NOTHING`, ...). Even
   though the runner tracks applied versions, idempotency protects you when
   running the same file against multiple environments.

## Recovery

If an auto migration fails, the app still starts. The failure is logged, the
`schema_migrations` row is not inserted, and the next startup retries. Fix the
SQL, save, restart — no cleanup needed.

If a manual migration fails mid-way, the outer transaction is rolled back and
the `schema_migrations` row is not inserted. The DB is in the state it was in
before you started. Investigate the logs, adjust, re-run.
