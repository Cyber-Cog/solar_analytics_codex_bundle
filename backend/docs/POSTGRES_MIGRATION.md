# PostgreSQL setup & legacy SQLite data

> **Note:** The app **requires PostgreSQL** (`DATABASE_URL` in `backend/.env`). Built-in SQLite (`solar.db`) and the old `migrate_sqlite_to_pg` / `fast_migrate_to_pg` scripts have been **removed**. If you still have a `solar.db` file, use **pgloader**, **CSV export/import**, or a one-off tool of your choice to load it into Postgres.

## Quick start (Windows – PostgreSQL already installed)

If PostgreSQL is installed and the `solar` database exists:

1. **Backend folder**: ensure `backend/.env` contains:
   ```ini
   DATABASE_URL=postgresql://solar:solar@localhost:5432/solar
    ```
2. **Start the server** (tables are created on first run):
   ```powershell
   cd backend
   python -m uvicorn main:app --port 8081
   ```

---

## Option A: PostgreSQL inside the project (Docker)

No need to install PostgreSQL on your machine. From the **project root** (`solar_analytics_codex_bundle`):

```bash
# 1. Start PostgreSQL in the background
docker-compose up -d

# 2. Create a .env file in the backend folder (or set in shell)
#    DATABASE_URL=postgresql://solar:solar@localhost:5432/solar

# 3. Install Python deps and run the app (tables are created automatically)
cd backend
pip install -r requirements.txt
set DATABASE_URL=postgresql://solar:solar@localhost:5432/solar
uvicorn main:app --reload --port 8000
```

To load data from an old **solar.db** file, use **pgloader** or export CSV from SQLite and `COPY` into PostgreSQL (see section 4 below).

---

## Option B: Use an existing PostgreSQL server

## 1. Install PostgreSQL and create database

```bash
# Example: create user and database
sudo -u postgres createuser -P solaruser
sudo -u postgres createdb -O solaruser solar
```

## 2. Set environment and install driver

```bash
export DATABASE_URL="postgresql://solaruser:YOUR_PASSWORD@localhost:5432/solar"
pip install psycopg2-binary
```

## 3. Create tables in PostgreSQL

Start the app once so SQLAlchemy creates all tables (users, plants, raw_data_generic, plant_architecture, equipment_specs, fault_diagnostics, support_tickets, etc.):

```bash
cd backend
uvicorn main:app --reload
```

Then stop it. Tables will be empty.

## 4. Migrate existing SQLite data

Option A – use a small Python script (run from project root):

- Export SQLite data to CSV per table, then use PostgreSQL `COPY` or insert from CSV.
- Or use `sqlite3` and `psycopg2` to read from SQLite and write to PostgreSQL in a loop.

Option B – use `pgloader` (if available):

```bash
pgloader solar.db postgresql://solaruser:pass@localhost/solar
```

(You may need to map SQLite types to PostgreSQL and fix table names if they differ.)

Option C – manual export/import:

1. From SQLite: export each table to CSV (e.g. with DB browser or `sqlite3`).
2. In PostgreSQL: create tables (already done by step 3), then use `COPY table FROM 'file.csv' WITH CSV HEADER` or insert via a script.

## 5. Run with PostgreSQL

```bash
export DATABASE_URL="postgresql://solaruser:YOUR_PASSWORD@localhost:5432/solar"
uvicorn main:app --reload --port 8080
```

Note: With PostgreSQL, the 15‑minute cache table (`raw_data_15m`) is not created or used; all queries run against `raw_data_generic`. For faster dashboards you can add a PG‑compatible materialized view or cache later (see `docs/ARCHITECTURE_DATA.md`).

### Optional: Reset sequences after copy

If you used a script that inserted rows with explicit `id` values, reset PostgreSQL sequences so new inserts get correct IDs:

```sql
SELECT setval(pg_get_serial_sequence('raw_data_generic', 'id'), COALESCE((SELECT MAX(id) FROM raw_data_generic), 1));
SELECT setval(pg_get_serial_sequence('fault_diagnostics', 'id'), COALESCE((SELECT MAX(id) FROM fault_diagnostics), 1));
-- repeat for other tables with SERIAL id
```

## 6. Recomputing Disconnected String (`fault_diagnostics`) from CLI

Scripts read **`DATABASE_URL` from `backend/.env`** (via `script_env.load_backend_env()`). Run them from the **`backend`** folder so imports resolve.

**Full plant recompute** (deletes existing `fault_diagnostics` rows for that plant, then repopulates day-by-day):

```bash
cd solar_analytics_codex_bundle/backend
python scripts/recompute_ds_faults.py --plant NTPCNOKHRA
```

**Small date window** (uses `run_ds_detection` only for the given range — does not delete other days):

```bash
cd solar_analytics_codex_bundle/backend
python _recompute_ds_range.py --plant NTPCNOKHRA --from "2026-03-01 00:00:00" --to "2026-03-02 23:59:59"
```

If you see `DATABASE_URL is not set`, create **`backend/.env`** with:

`DATABASE_URL=postgresql://USER:PASSWORD@HOST:5432/DATABASE`

**Earlier failures were often caused by:**

- Running the script without `cd` into `backend` (imports / `.env` not found).
- Manual URL parsing in `recompute_ds_faults.py` breaking on passwords with `:` or `@` — fixed by using **`engine.raw_connection()`** from `database.py`.
- Irradiance stored as **`equipment_level=wms`** — recompute SQL now treats **`plant` and `wms`** like the rest of the app.
