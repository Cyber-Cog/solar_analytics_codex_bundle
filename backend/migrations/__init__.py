"""Lightweight SQL migration runner.

Files matching ``sql/NNN_*.sql`` under this package are applied in order at
startup; each one runs in its own transaction and its filename is recorded in a
``schema_migrations`` table so it never re-runs.

Files under ``manual/`` are NOT auto-applied. They contain potentially risky
changes (column-type migrations, partitioning) and must be triggered explicitly
via ``scripts/run_manual_migration.ps1``.
"""
