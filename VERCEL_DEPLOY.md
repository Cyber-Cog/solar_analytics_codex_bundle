# Vercel Deploy Guide

This repo is now set up so the frontend and backend can run in **one Vercel project**
while still using your **existing PostgreSQL database**.

## What changed

- `app.py` is the Vercel Python entrypoint for the FastAPI backend.
- `build.py` copies `frontend/` into `public/` during the Vercel build.
- `vercel.json` sets the build command and Python function timeout.
- The backend now runs in a serverless-safe mode on Vercel:
  - skips background boot threads
  - skips startup migration threads
  - uses a small SQLAlchemy `QueuePool` for DB connections
  - auto-creates `users` + `plants` tables if missing (login/signup)
  - rewrites `postgres://` URLs and adds `sslmode=require` for remote hosts
- Spec-sheet uploads use **Vercel Blob** only if you enable it.

## Before you deploy

Push this code to GitHub first.

## Vercel setup

1. Go to Vercel.
2. Click `Add New...` -> `Project`.
3. Import your GitHub repository.
4. Keep the project as a **Python** project if Vercel asks.
5. In **Project → Settings → Environment Variables**, add for **Production** (and Preview if needed):

```env
# Required — same Postgres you use locally / on RDS (must be reachable from the public internet)
DATABASE_URL=postgresql://USER:PASSWORD@HOST:5432/DATABASE
DATABASE_URL_READ=postgresql://USER:PASSWORD@HOST:5432/DATABASE

# Required — any long random string (JWT signing)
SECRET_KEY=replace-with-a-long-random-secret
```

Notes:

- `postgres://...` URLs from Neon/Supabase/Heroku are rewritten automatically.
- Remote hosts get `sslmode=require` on Vercel unless you set `sslmode` yourself.
- RDS: security group must allow inbound **5432** from `0.0.0.0/0` or Vercel’s IPs.
- After changing env vars, **Redeploy** (env is baked at deploy time).

6. Optional, only if you want spec-sheet uploads on Vercel:

```env
ENABLE_BLOB_UPLOADS=1
BLOB_READ_WRITE_TOKEN=your-vercel-blob-read-write-token
BLOB_ACCESS=public
```

7. Click `Deploy`.

## After deploy

Test these URLs:

- `/`
- `/health` → `{"status":"ok","version":"2.0.0"}`
- `/health/db` → `{"status":"ok","database":"connected","users_table":true,"user_count":N}`
- `/docs`

## Login / API errors on Vercel

**Symptom:** Sign In shows `API Error` or `HTTP 500: Internal Server Error on /auth/login`.

**What the 21-byte plain-text 500 means:** Vercel returned the literal string `Internal Server Error` (not JSON). That happens when the Python function crashes on the **first database query** during login. `/health` can still be `ok` because it does not touch Postgres.

**Diagnose after redeploying this repo:**

1. Open `https://YOUR_APP.vercel.app/health/db`
   - `database: connected`, `users_table: true` → DB OK; wrong password shows `Incorrect email or password`.
   - `503` with `detail` → fix `DATABASE_URL`, SSL, or RDS security group.
   - `users_table: false` → redeploy (serverless boot now creates `users`); or run migrations on the DB.

2. Browser DevTools → Network → `POST /auth/login` → Response body should now be JSON with a `detail` field (after this fix is deployed).

**Wrong password** always returns JSON `401` with `"Incorrect email or password"` — not a generic API error.
