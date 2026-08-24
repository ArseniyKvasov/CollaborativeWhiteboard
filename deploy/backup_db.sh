#!/usr/bin/env bash
# Scheduled PostgreSQL backup -> S3 (Yandex Object Storage).
#
# Intended for host cron, e.g. daily at 03:30:
#   30 3 * * * /opt/collaborative-whiteboard/deploy/backup_db.sh >> /var/log/whiteboard-backup.log 2>&1
#
# Requires: awscli on the host (sudo apt install -y awscli) and these vars in
# .env.production:
#   PG_BACKUP_S3_PREFIX=s3://whiteboard-postgres/postgres/production
#   AWS_ENDPOINT=https://storage.yandexcloud.net
#   AWS_REGION=ru-central1
#   AWS_ACCESS_KEY_ID=YCA...
#   AWS_SECRET_ACCESS_KEY=YCP...
# Optional: PG_USER / PG_DB (default whiteboard/whiteboard), BACKUP_KEEP_LOCAL (default 7)
#
# Flags:
#   --prune-s3 DAYS   also delete S3 backups older than DAYS (otherwise configure
#                     a lifecycle rule in the Yandex console and skip this flag)
#   --dry-run         dump and upload, but never delete anything
set -Eeuo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$APP_DIR"

ENV_FILE="${ENV_FILE:-.env.production}"
[[ -f "$ENV_FILE" ]] || { echo "[backup] $ENV_FILE not found" >&2; exit 1; }
set -a; . "$ENV_FILE"; set +a

: "${PG_BACKUP_S3_PREFIX:?PG_BACKUP_S3_PREFIX is not set (e.g. s3://whiteboard-postgres/postgres/production)}"
: "${AWS_ENDPOINT:?AWS_ENDPOINT is not set (https://storage.yandexcloud.net)}"
: "${AWS_ACCESS_KEY_ID:?AWS_ACCESS_KEY_ID is not set}"
: "${AWS_SECRET_ACCESS_KEY:?AWS_SECRET_ACCESS_KEY is not set}"
AWS_REGION="${AWS_REGION:-ru-central1}"
PG_USER="${PG_USER:-whiteboard}"
PG_DB="${PG_DB:-whiteboard}"
BACKUP_KEEP_LOCAL="${BACKUP_KEEP_LOCAL:-7}"

PRUNE_S3_DAYS=""
DRY_RUN=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    --prune-s3) PRUNE_S3_DAYS="$2"; shift ;;
    --dry-run) DRY_RUN=1 ;;
    *) echo "[backup] unknown arg: $1" >&2; exit 1 ;;
  esac
  shift
done

command -v aws >/dev/null || { echo "[backup] awscli not found on host (apt install awscli)" >&2; exit 1; }
export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_REGION

TS="$(date +%Y%m%d-%H%M%S)"
STAMP="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
LOCAL_DIR="$APP_DIR/backups/db"
mkdir -p "$LOCAL_DIR"
LOCAL_FILE="$LOCAL_DIR/db-$TS.sql.gz"
S3_URI="$PG_BACKUP_S3_PREFIX/db-$TS.sql.gz"

echo "[backup] $STAMP dumping postgres..."
docker compose --env-file "$ENV_FILE" -f docker-compose.prod.yml exec -T db \
  pg_dump -U "$PG_USER" "$PG_DB" | gzip -9 > "$LOCAL_FILE"

SIZE=$(stat -c%s "$LOCAL_FILE" 2>/dev/null || stat -f%z "$LOCAL_FILE")
(( SIZE > 1000 )) || { echo "[backup] dump suspiciously small ($SIZE bytes) - aborting, NOT uploading" >&2; exit 1; }

echo "[backup] uploading $LOCAL_FILE ($SIZE bytes) -> $S3_URI"
if (( DRY_RUN )); then
  echo "[backup] dry-run: skipping upload"
else
  aws s3 cp "$LOCAL_FILE" "$S3_URI" --endpoint-url "$AWS_ENDPOINT" --only-show-errors
  REMOTE_SIZE=$(aws s3api head-object --endpoint-url "$AWS_ENDPOINT" \
    --bucket "${PG_BACKUP_S3_PREFIX#s3://}" --key "db-$TS.sql.gz" \
    --query 'ContentLength' --output text)
  [[ "$REMOTE_SIZE" == "$SIZE" ]] || { echo "[backup] size mismatch after upload ($REMOTE_SIZE != $SIZE)" >&2; exit 1; }
  echo "[backup] uploaded and verified"
fi

# Local rotation (always, even on dry-run: these are just yesterday's files)
ls -1t "$LOCAL_DIR"/db-*.sql.gz 2>/dev/null | tail -n +$((BACKUP_KEEP_LOCAL+1)) | xargs -r rm -f --

if [[ -n "$PRUNE_S3_DAYS" && ! $PRUNE_S3_DAYS =~ ^[0-9]+$ ]]; then
  echo "[backup] --prune-s3 expects a number of days" >&2; exit 1
fi
if [[ -n "$PRUNE_S3_DAYS" && "$DRY_RUN" == 0 && "${PRUNE_S3_DAYS:-}" != "" ]]; then
  BUCKET="${PG_BACKUP_S3_PREFIX#s3://}"; BUCKET="${BUCKET%%/*}"
  BASE_KEY="${PG_BACKUP_S3_PREFIX#s3://*/}"; BASE_KEY="${BASE_KEY%/}"
  CUTOFF=$(date -u -d "-$PRUNE_S3_DAYS days" +%s 2>/dev/null || date -v-${PRUNE_S3_DAYS}d -u +%s)
  aws s3api list-objects-v2 --endpoint-url "$AWS_ENDPOINT" --bucket "$BUCKET" --prefix "$BASE_KEY/" \
    --query 'Contents[].{Key:Key,LastModified:LastModified}' --output text |
  while IFS=$'\t' read -r key lm; do
    [[ -z "$key" || "$key" == "None" ]] && continue
    ts=$(date -u -d "$lm" +%s 2>/dev/null || date -u -j -f '%Y-%m-%dT%H:%M:%S.%NZ' "$lm" +%s 2>/dev/null) || continue
    if (( ts < CUTOFF )); then
      aws s3api delete-object --endpoint-url "$AWS_ENDPOINT" --bucket "$BUCKET" --key "$key" >/dev/null
      echo "[backup] pruned s3://$BUCKET/$key"
    fi
  done
fi

echo "[backup] done: $S3_URI"
