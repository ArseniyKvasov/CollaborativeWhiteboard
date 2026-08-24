#!/usr/bin/env bash
# Scheduled PostgreSQL backup -> S3 (Yandex Object Storage) with retention.
#
# Host cron (daily 03:30) - installed automatically by ./deploy/production.sh:
#   30 3 * * * /opt/collaborative-whiteboard/deploy/backup_db.sh >> .../backup.log 2>&1
#
# NO host dependencies beyond docker: dump runs in the db container, upload /
# verification / retention run in the whiteboard container (boto3).
#
# Required vars in .env.production (passed into containers via env_file):
#   PG_BACKUP_S3_PREFIX=s3://whiteboard-postgres/postgres/production
#   AWS_ENDPOINT=https://storage.yandexcloud.net
#   AWS_ACCESS_KEY_ID=YCA... / AWS_SECRET_ACCESS_KEY=YCP...
# Optional:
#   AWS_REGION (ru-central1), PG_USER / PG_DB (whiteboard),
#   BACKUP_KEEP_LOCAL (7), BACKUP_S3_RETENTION_DAYS (30, 0 = off)
#
# Flags:
#   --dry-run        dump only; no upload, no retention
#   --prune-s3 DAYS  one-off retention override
set -Eeuo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$APP_DIR"

ENV_FILE="${ENV_FILE:-.env.production}"
[[ -f "$ENV_FILE" ]] || { echo "[backup] $ENV_FILE not found" >&2; exit 1; }
set -a; . "$ENV_FILE"; set +a

: "${PG_BACKUP_S3_PREFIX:?PG_BACKUP_S3_PREFIX is not set (e.g. s3://whiteboard-postgres/postgres/production)}"
: "${AWS_ENDPOINT:?AWS_ENDPOINT is not set}"
: "${AWS_ACCESS_KEY_ID:?AWS_ACCESS_KEY_ID is not set}"
: "${AWS_SECRET_ACCESS_KEY:?AWS_SECRET_ACCESS_KEY is not set}"
PG_USER="${PG_USER:-whiteboard}"
PG_DB="${PG_DB:-whiteboard}"
BACKUP_KEEP_LOCAL="${BACKUP_KEEP_LOCAL:-7}"
BACKUP_S3_RETENTION_DAYS="${BACKUP_S3_RETENTION_DAYS:-30}"

DRY_RUN=0
PRUNE_OVERRIDE=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run) DRY_RUN=1 ;;
    --prune-s3) PRUNE_OVERRIDE="$2"; shift ;;
    *) echo "[backup] unknown arg: $1" >&2; exit 1 ;;
  esac
  shift
done

COMPOSE=(docker compose --env-file "$ENV_FILE" -f docker-compose.prod.yml)
s3tool() { "${COMPOSE[@]}" exec -T whiteboard python scripts/s3_backup_tool.py "$@"; }

TS="$(date +%Y%m%d-%H%M%S)"
LOCAL_DIR="$APP_DIR/backups/db"
mkdir -p "$LOCAL_DIR"
LOCAL_FILE="$LOCAL_DIR/db-$TS.sql.gz"

echo "[backup] $(date -u +%Y-%m-%dT%H:%M:%SZ) dumping postgres..."
"${COMPOSE[@]}" exec -T db pg_dump -U "$PG_USER" "$PG_DB" | gzip -9 > "$LOCAL_FILE"

SIZE=$(stat -c%s "$LOCAL_FILE" 2>/dev/null || stat -f%z "$LOCAL_FILE")
(( SIZE > 1000 )) || { echo "[backup] dump suspiciously small ($SIZE bytes) - aborting, NOT uploading" >&2; exit 1; }

if (( DRY_RUN )); then
  echo "[backup] dry-run: dump is at $LOCAL_FILE ($SIZE bytes) - skipping upload/retention"
else
  echo "[backup] uploading ($SIZE bytes) -> $PG_BACKUP_S3_PREFIX/db-$TS.sql.gz"
  s3tool put "db-$TS.sql.gz" < "$LOCAL_FILE"

  # Retention: explicit flag wins, else BACKUP_S3_RETENTION_DAYS (0 disables).
  if [[ -n "$PRUNE_OVERRIDE" ]]; then
    s3tool prune --days "$PRUNE_OVERRIDE"
  elif [[ "$BACKUP_S3_RETENTION_DAYS" != "0" ]]; then
    s3tool prune --days "$BACKUP_S3_RETENTION_DAYS"
  fi
fi

# Local rotation (always).
ls -1t "$LOCAL_DIR"/db-*.sql.gz 2>/dev/null | tail -n +$((BACKUP_KEEP_LOCAL+1)) | xargs -r rm -f --

echo "[backup] done"
