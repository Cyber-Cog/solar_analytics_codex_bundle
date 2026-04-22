#!/usr/bin/env bash
# One-command run: PostgreSQL (Docker) + Solar Analytics backend (fast DB).
# Requires: Docker, Python with backend/requirements.txt installed.

set -e
cd "$(dirname "$0")"

echo "Starting PostgreSQL in Docker..."
docker-compose up -d 2>/dev/null || true
sleep 5

export DATABASE_URL="postgresql://solar:solar@localhost:5432/solar"
echo "Using PostgreSQL. Starting app on http://localhost:8080"
cd backend && exec uvicorn main:app --reload --port 8080
