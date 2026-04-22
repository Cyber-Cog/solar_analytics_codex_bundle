# Database and speed

## PostgreSQL is required

The backend **requires** `DATABASE_URL` in `backend/.env` pointing to PostgreSQL. There is no SQLite fallback.

### Windows (Docker)

1. Install [Docker Desktop](https://www.docker.com/products/docker-desktop/) and start it.
2. In the project folder (`solar_analytics_codex_bundle`), run:
   ```bat
   run.bat
   ```
3. Ensure `backend/.env` contains `DATABASE_URL=postgresql://solar:solar@localhost:5432/solar` (or match your Docker compose).

`run.bat` starts PostgreSQL in Docker and runs the backend with `DATABASE_URL` set. Tables are created automatically on first run.

### Linux / Mac

```bash
chmod +x run.sh
./run.sh
```

---

## Legacy SQLite (`solar.db`)

If you have an old **solar.db** file, copy it into PostgreSQL with **pgloader**, CSV export/import, or another migration tool. The in-repo `migrate_sqlite_to_pg` scripts were removed.

See `docs/POSTGRES_MIGRATION.md` for pgloader and manual options.

---

## What makes the app faster

- **PostgreSQL** for large `raw_data_generic` / `fault_diagnostics` tables.
- **Dashboard bundle**: one API call loads dashboard data (cached for a few minutes).
- **Indexes** on `raw_data_generic`, `fault_diagnostics`, and related tables (created in `db_perf.py`).
- **Fault / analytics** tuning: see router code and `db_perf.ensure_performance_objects`.

---

## Running without Docker

Install PostgreSQL locally, create database and user, set `DATABASE_URL` in `backend/.env`, then:

```bat
cd backend
python -m uvicorn main:app --reload --port 8080
```

If `DATABASE_URL` is missing, the app will **not** start (clear error from `database.py`).
