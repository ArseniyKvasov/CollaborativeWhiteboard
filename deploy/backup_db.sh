#!/usr/bin/env bash
# Scheduled PostgreSQL backup -> S3 (Yandex Object Storage) with retention.
#
# Host cron (daily 03:30):
#   30 3 * * * /opt/collaborative-whiteboard/deploy/backup_db.sh >> /var/log/whiteboard-backup.log 2>&1
#
# Requires awscli on the host (sudo apt install -y awscli) and these vars in
# .env.production:
#   PG_BACKUP_S3_PREFIX=s3://whiteboard-postgres/postgres/production
#   AWS_ENDPOINT=https://storage.yandexcloud.net
#   AWS_REGION=ru-central1
#   AWS_ACCESS_KEY_ID=YCA...
#   AWS_SECRET_ACCESS_KEY=YCP...
# Optional:
#   PG_USER / PG_DB            (default whiteboard/whiteboard)
#   BACKUP_KEEP_LOCAL          local copies to keep        (default 7)
#   BACKUP_S3_RETENTION_DAYS   auto-delete S3 dumps older  (default 30, 0 = off;
#                              alternative: lifecycle rule in Yandex console)
#
# Flags:
#   --dry-run              dump + upload, but never delete anything
#   --prune-s3 DAYS        one-off retention override (implies prune this run)
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

command -v aws >/dev/null || { echo "[backup] awscli not found on host (apt install awscli)" >&2; exit 1; }
export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_REGION

S3_BUCKET="${PG_BACKUP_S3_PREFIX#s3://}"; S3_BUCKET="${S3_BUCKET%%/*}"
S3_BASE_KEY="${PG_BACKUP_S3_PREFIX#s3://*/}"; S3_BASE_KEY="${S3_BASE_KEY%/}"

prune_s3() {  # prune_s3 <days>
  local days="$1"
  [[ "$days" =~ ^[0-9]+$ ]] || { echo "[backup] retention days must be numeric, got: $days" >&2; return 1; }
  (( days == 0 )) && return 0

  # GNU date (Ubuntu server) and BSD date (macOS) both handled.
  local cutoff
  if cutoff=$(date -u -d "-$days days" +%s 2>/dev/null); then :;
  elif cutoff=$(date -u -v-${days}d +%s 2>/dev/null); then :;
  else echo "[backup] cannot compute cutoff date" >&2; return 1; fi

  local pruned=0
  while IFS=$'\t' read -r key lm; do
    [[ -z "$key" || "$key" == "None" ]] && continue
    local ts
    if ! ts=$(date -u -d "$lm" +%s 2>/dev/null); then
      ts=$(date -u -j -f '%Y-%m-%dT%H:%M:%S' "${lm%%.*}" +%s 2>/dev/null) || continue
    fi
    if (( ts < cutoff )); then
      aws s3api delete-object --endpoint-url "$AWS_ENDPOINT" --bucket "$S3_BUCKET" --key "$key" >/dev/null
      echo "[backup] pruned s3://$S3_BUCKET/$key (older than ${days}d)"
      pruned=$((pruned+1))
    fi
  done < <(aws s3api list-objects-v2 --endpoint-url "$AWS_ENDPOINT" --bucket "$S3_BUCKET" \
             --prefix "$S3_BASE_KEY/" \
             --query 'Contents[].{Key:Key,LastModified:LastModified}' --output text)
  echo "[backup] s3 retention (${days}d): pruned $pruned object(s)"
}

TS="$(date +%Y%m%d-%H%M%S)"
LOCAL_DIR="$APP_DIR/backups/db"
mkdir -p "$LOCAL_DIR"
LOCAL_FILE="$LOCAL_DIR/db-$TS.sql.gz"
S3_URI="$PG_BACKUP_S3_PREFIX/db-$TS.sql.gz"

echo "[backup] $(date -u +%Y-%m-%dT%H:%M:%SZ) dumping postgres..."
docker compose --env-file "$ENV_FILE" -f docker-compose.prod.yml exec -T db \
  pg_dump -U "$PG_USER" "$PG_DB" | gzip -9 > "$LOCAL_FILE"

SIZE=$(stat -c%s "$LOCAL_FILE" 2>/dev/null || stat -f%z "$LOCAL_FILE")
(( SIZE > 1000 )) || { echo "[backup] dump suspiciously small ($SIZE bytes) - aborting, NOT uploading" >&2; exit 1; }

echo "[backup] uploading $LOCAL_FILE ($SIZE bytes) -> $S3_URI"
if (( DRY_RUN )); then
  echo "[backup] dry-run: skipping upload and retention"
else
  aws s3 cp "$LOCAL_FILE" "$S3_URI" --endpoint-url "$AWS_ENDPOINT" --only-show-errors
  REMOTE_SIZE=$(aws s3api head-object --endpoint-url "$AWS_ENDPOINT" \
    --bucket "$S3_BUCKET" --key "$S3_BASE_KEY/db-$TS.sql.gz" \
    --query 'ContentLength' --output text)
  [[ "$REMOTE_SIZE" == "$SIZE" ]] || { echo "[backup] size mismatch after upload ($REMOTE_SIZE != $SIZE)" >&2; exit 1; }
  echo "[backup] uploaded and verified"

  # Retention: explicit flag wins, otherwise BACKUP_S3_RETENTION_DAYS (0 disables).
  if [[ -n "$PRUNE_OVERRIDE" ]]; then
    prune_s3 "$PRUNE_OVERRIDE"
  elif [[ "$BACKUP_S3_RETENTION_DAYS" != "0" ]]; then
    prune_s3 "$BACKUP_S3_RETENTION_DAYS"
  fi
fi

# Local rotation (always).
ls -1t "$LOCAL_DIR"/db-*.sql.gz 2>/dev/null | tail -n +$((BACKUP_KEEP_LOCAL+1)) | xargs -r rm -f --

echo "[backup] done: $S3_URI"
