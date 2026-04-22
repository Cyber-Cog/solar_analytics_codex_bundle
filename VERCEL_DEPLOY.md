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
  - uses SQLAlchemy `NullPool` for DB connections
- Spec-sheet uploads use **Vercel Blob** only if you enable it.

## Before you deploy

Push this code to GitHub first.

## Vercel setup

1. Go to Vercel.
2. Click `Add New...` -> `Project`.
3. Import your GitHub repository.
4. Keep the project as a **Python** project if Vercel asks.
5. In `Environment Variables`, add:

```env
DATABASE_URL=your-existing-postgres-url
DATABASE_URL_READ=your-existing-postgres-url
SECRET_KEY=your-secret-key
```

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
- `/health`
- `/docs`

## Important note

This deployment does **not** create or move your database.
It only points the Vercel backend to the database URL you already have.
