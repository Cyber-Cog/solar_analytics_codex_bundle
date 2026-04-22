# syntax=docker/dockerfile:1.6
#
# Solar Analytics backend image.
#
# Layout inside the container:
#   /app/backend        FastAPI app (mirrors repo backend/)
#   /app/frontend       static UI served by FastAPI at /
#   /app/backend/uploads  named volume for user-uploaded spec sheets
#
# Production run (via docker compose) uses gunicorn + UvicornWorker. For local
# dev you can override the command to `uvicorn main:app --reload`.

FROM python:3.11-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# System deps:
#   - build-essential + libpq-dev: in case psycopg2 has to compile from source
#   - curl: health checks
#   - tini: proper PID 1 to forward signals to gunicorn workers
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      build-essential \
      libpq-dev \
      curl \
      tini \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install deps first for better layer caching.
COPY backend/requirements.txt /app/backend/requirements.txt
RUN pip install -r /app/backend/requirements.txt

# Copy source trees. The compose file also bind-mounts these in dev so local
# edits show up without rebuilding; the COPY here is what ships to production.
COPY backend /app/backend
COPY frontend /app/frontend

# Uploads live on a persistent volume mounted by compose. Make sure the
# directory exists in the image so FastAPI can write to it on first boot.
RUN mkdir -p /app/backend/uploads/spec_sheets

WORKDIR /app/backend

EXPOSE 8000

# Default command: gunicorn managing Uvicorn workers for prod.
# Runtime tuning lives in gunicorn.conf.py and can still be overridden with env.
ENV GUNICORN_WORKERS=3 \
    GUNICORN_TIMEOUT=180 \
    PORT=8000

ENTRYPOINT ["/usr/bin/tini", "--"]

CMD ["gunicorn", "-c", "gunicorn.conf.py", "main:app"]
