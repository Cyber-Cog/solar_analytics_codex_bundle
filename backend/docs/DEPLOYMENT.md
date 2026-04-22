# Solar Analytics — Deployment

This doc covers how to bring the stack up locally, how to run it in production
on a single VM, and how to point it at an external managed Postgres (e.g. AWS
RDS).

- Related: [BACKUP_AND_RESTORE.md](./BACKUP_AND_RESTORE.md),
  [DB_AND_SPEED.md](./DB_AND_SPEED.md).

---

## 1. Topology

Three containers behind one reverse proxy:

```
              ┌─────────────┐
 browser ──►  │  nginx :80  │──► static UI (frontend/)
              │             │──► /api,/auth ─► backend:8000
              └─────────────┘
                             ┌────────────┐     ┌───────────┐
                   backend ──►  postgres  │     │   redis   │  (cache)
                             └────────────┘     └───────────┘
```

- `backend` is FastAPI running under **gunicorn + UvicornWorker** in prod
  (`gunicorn main:app --worker-class uvicorn.workers.UvicornWorker`). Local dev
  can override the command to `uvicorn main:app --reload` if you prefer hot
  reload (see *Local development* below).
- `nginx` serves the static UI and reverse-proxies `/api`, `/auth`, `/docs`,
  `/health`, `/openapi.json`.
- `postgres` is shipped in the dev stack; in prod you can either keep it or
  swap it for a managed DB (RDS / Cloud SQL / Neon).
- `redis` is optional — set `REDIS_URL` to enable cross-worker caching. The app
  falls back to an in-process dict cache when it's absent.

---

## 2. Prerequisites

- Docker Engine 24+ and Docker Compose v2 (`docker compose version`).
- A `.env` file at the repo root. Copy the template and edit:

  ```bash
  cp .env.example .env
  ```

  At minimum, change `POSTGRES_PASSWORD` and `SECRET_KEY` before exposing the
  service publicly.

---

## 3. Local development

```bash
make run            # build + start dev stack
make migrate        # apply safe SQL migrations
make logs           # tail logs
```

UI: <http://localhost:8080> (override via `HTTP_PORT` in `.env`).

Hot reload for the backend: bind-mount the source and override the command
once (the image has `uvicorn` because it's a dep of gunicorn's worker class):

```bash
docker compose run --rm --service-ports \
  -v $PWD/backend:/app/backend \
  backend uvicorn main:app --host 0.0.0.0 --port 8000 --reload
```

---

## 4. Production — single VM, DB on-box

```bash
# On the VM
git clone <repo> solar && cd solar
cp .env.example .env && vim .env       # set strong SECRET_KEY / POSTGRES_PASSWORD
make prod                               # -f docker-compose.yml -f docker-compose.prod.yml
```

Extras the prod overlay enables:

- `GUNICORN_WORKERS=5` by default (tune per CPU count: `(2*CPU)+1`).
- DB port is NOT published to the host.
- `SOLAR_MIGRATIONS_STRICT=1` makes the backend refuse to boot if a safe
  migration fails (instead of logging and continuing).
- `nginx` mounts `./deploy/nginx.prod.conf:/etc/nginx/conf.d/default.conf:ro`.
  Copy `deploy/nginx.prod.conf` to that path and wire in real TLS certs before
  exposing 443.

Scheduled nightly pg_dump (Linux host):

```bash
sudo ln -s $PWD/scripts/backup_postgres.sh /usr/local/bin/solar-backup
sudo crontab -e
# Add:
0 23 * * * PGPASSWORD=... /usr/local/bin/solar-backup >> /var/log/solar-backup.log 2>&1
```

On Windows, use `scripts/Register-SolarBackupTask.ps1` instead (see
[BACKUP_AND_RESTORE.md](./BACKUP_AND_RESTORE.md)).

---

## 5. Production — managed Postgres (AWS RDS, Cloud SQL, Neon, …)

1. Provision the DB, grab the connection string.
2. Put it in `.env`:

   ```env
   DATABASE_URL=postgresql://solaruser:***@my-db.xxx.rds.amazonaws.com:5432/solar
   DATABASE_URL_READ=postgresql://solaruser:***@my-db.xxx.rds.amazonaws.com:5432/solar
   SECRET_KEY=...
   ```

3. Run the RDS overlay — it drops the `db` container and forwards your
   DATABASE_URL into the backend:

   ```bash
   make rds
   ```

   (Equivalent to `docker compose -f docker-compose.yml
   -f docker-compose.prod.yml -f docker-compose.rds.yml up -d --build`.)

4. Enable **automated snapshots** on the managed side (RDS does this by
   default; keep retention ≥ 14 days). For belt-and-braces off-site backups,
   also run `scripts/backup_postgres.sh` with `S3_BUCKET=your-bucket` so you
   still have PITR-friendly `pg_dump` files outside the RDS account.

---

## 6. First-time DB bring-up

After the stack is up:

```bash
make migrate-status       # see what's pending
make migrate              # apply safe SQL migrations
```

For risky / data-altering migrations (type changes, partitioning) — ALWAYS
take a backup first:

```bash
make backup
make migrate-manual FILE=010_timestamps_to_native.sql
make migrate-manual FILE=020_partition_raw_data_generic.sql
```

---

## 7. Ops cheat-sheet

| Task                       | Command                                   |
| -------------------------- | ----------------------------------------- |
| Restart backend only       | `docker compose restart backend`          |
| Follow backend logs        | `docker compose logs -f backend`          |
| Open a psql session        | `make psql`                               |
| Open a shell in backend    | `make shell`                              |
| Stop stack, keep data      | `make down`                               |
| Nuke stack + volumes       | `make nuke`  ← destructive                |
| Take a backup right now    | `make backup`                             |
| Restore from a backup      | `make restore FILE=./backups/solar_*.dump`|

---

## 8. Upgrading

```bash
git pull
make prod      # rebuilds images, applies safe migrations at startup
```

If a release ships a manual migration, the PR description will call it out and
the command will look like:

```bash
make backup
make migrate-manual FILE=NNN_description.sql
```

---

## 9. Troubleshooting

- **`docker compose up` hangs on db healthcheck** — check the password in
  `.env` matches what's already persisted in the `db_data` volume. If you
  changed it, you either need `make nuke` (destructive) or to connect and
  update the role password.
- **Frontend 502s** — the backend container is unhealthy. `make logs backend`
  usually shows either DB auth errors or a migration runner error (with
  `SOLAR_MIGRATIONS_STRICT=1`).
- **RDS deploy refuses to start** — `docker-compose.rds.yml` requires
  `DATABASE_URL` to be set in `.env`; the overlay fails fast if it's not.
- **Slow first load after a fresh deploy** — expected: the backend builds
  equipment materialisations and warms caches on first request. Subsequent
  loads should be snappy.
