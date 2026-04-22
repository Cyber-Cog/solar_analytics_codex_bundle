#!/usr/bin/env bash
# Nightly pg_dump for Solar Analytics deployments on Linux (bare-metal / Docker).
# Mirrors scripts/backup_postgres.ps1 for Windows: custom-format dump, daily +
# weekly rotation, optional S3 sync.
#
# Run from cron, e.g.:
#   0 23 * * * /opt/solar/scripts/backup_postgres.sh >> /var/log/solar-backup.log 2>&1
#
# Env vars (override as needed):
#   PGHOST (default: 127.0.0.1)
#   PGPORT (default: 5432)
#   PGUSER (default: solar)
#   PGPASSWORD (REQUIRED unless ~/.pgpass is configured)
#   PGDATABASE (default: solar)
#   BACKUP_DIR (default: ./backups relative to repo root)
#   S3_BUCKET (optional; if set, uploads the dump to s3://$S3_BUCKET/solar/)
#   KEEP_DAILY (default: 14)
#   KEEP_WEEKLY (default: 8)

set -euo pipefail

PGHOST="${PGHOST:-127.0.0.1}"
PGPORT="${PGPORT:-5432}"
PGUSER="${PGUSER:-solar}"
PGDATABASE="${PGDATABASE:-solar}"
BACKUP_DIR="${BACKUP_DIR:-$(cd "$(dirname "$0")/.." && pwd)/backups}"
KEEP_DAILY="${KEEP_DAILY:-14}"
KEEP_WEEKLY="${KEEP_WEEKLY:-8}"

mkdir -p "$BACKUP_DIR/daily" "$BACKUP_DIR/weekly"

stamp="$(date +%Y%m%d_%H%M%S)"
daily_file="$BACKUP_DIR/daily/solar_${stamp}.dump"

echo "[backup] dumping $PGDATABASE from $PGHOST:$PGPORT -> $daily_file"
pg_dump \
  -h "$PGHOST" -p "$PGPORT" -U "$PGUSER" \
  -F c -Z 9 \
  -f "$daily_file" \
  "$PGDATABASE"

# Weekly: copy Sunday's dump into weekly/ (dow == 7).
if [[ "$(date +%u)" == "7" ]]; then
  cp "$daily_file" "$BACKUP_DIR/weekly/solar_${stamp}.dump"
fi

# Rotation.
echo "[backup] pruning daily (keep $KEEP_DAILY) / weekly (keep $KEEP_WEEKLY)"
ls -1t "$BACKUP_DIR"/daily/solar_*.dump 2>/dev/null  | tail -n +$((KEEP_DAILY + 1))  | xargs -r rm -f
ls -1t "$BACKUP_DIR"/weekly/solar_*.dump 2>/dev/null | tail -n +$((KEEP_WEEKLY + 1)) | xargs -r rm -f

# Optional: ship to S3.
if [[ -n "${S3_BUCKET:-}" ]]; then
  echo "[backup] uploading to s3://$S3_BUCKET/solar/"
  aws s3 cp "$daily_file" "s3://$S3_BUCKET/solar/daily/" --only-show-errors
  if [[ "$(date +%u)" == "7" ]]; then
    aws s3 cp "$daily_file" "s3://$S3_BUCKET/solar/weekly/" --only-show-errors
  fi
fi

echo "[backup] done."
