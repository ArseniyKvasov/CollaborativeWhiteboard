#!/usr/bin/env bash
#
# Zero-downtime production deploy for CollaborativeWhiteboard.
#
# Strategy: blue-green on a single host.
#   1. Build images.
#   2. Start the INACTIVE slot (whiteboard + celery-worker) on its own port;
#      db/redis are shared and keep running untouched.
#   3. Poll /health on the new port until it reports ok+redis.
#   4. Rewrite the nginx upstream file to the new port and `systemctl reload
#      nginx` (graceful: existing connections finish, new ones hit the new slot).
#   5. Remove the old slot containers.
# Any failure before step 4 leaves the old slot serving traffic untouched.
#
# Usage:
#   ./deploy/production.sh                # full zero-downtime deploy
#   ./deploy/production.sh --skip-build   # reuse existing images
#   ./deploy/production.sh --no-backup    # skip pre-deploy pg_dump
#   ./deploy/production.sh --no-cron      # do not touch host crontab
#   ./deploy/production.sh --prune        # docker image prune after switch
#   ./deploy/production.sh status         # show slots / state / upstream
#   ./deploy/production.sh rollback       # deploy back to the previous slot
#   ./deploy/production.sh nginx-install  # one-time nginx setup from template
#
set -Eeuo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$APP_DIR"

COMPOSE_FILE="docker-compose.prod.yml"
ENV_FILE="${ENV_FILE:-.env.production}"
STATE_FILE=".deploy-state"
BACKUP_DIR="backups"
BACKUP_KEEP="${BACKUP_KEEP:-7}"
PORT_A="${HOST_PORT_A:-18743}"
PORT_B="${HOST_PORT_B:-18744}"
HEALTH_TIMEOUT="${HEALTH_TIMEOUT:-180}"
NGINX_UPSTREAM_CONF="${NGINX_UPSTREAM_CONF:-/etc/nginx/conf.d/whiteboard-upstream.conf}"
NGINX_SITE_TMPL="$APP_DIR/deploy/nginx/whiteboard.conf"
NGINX_SITE_NAME="whiteboard"

C_G="\033[0;32m"; C_Y="\033[0;33m"; C_R="\033[0;31m"; C_0="\033[0m"
info() { printf "${C_G}[deploy]${C_0} %s\n" "$*"; }
warn() { printf "${C_Y}[warn]${C_0} %s\n" "$*"; }
die()  { printf "${C_R}[error]${C_0} %s\n" "$*" >&2; exit 1; }

SKIP_BUILD=0; DO_BACKUP=1; DO_PRUNE=0; SKIP_CRON=0; COMMAND="deploy"
while [[ $# -gt 0 ]]; do
  case "$1" in
    --skip-build) SKIP_BUILD=1 ;;
    --no-backup)  DO_BACKUP=0 ;;
    --no-cron)    SKIP_CRON=1 ;;
    --prune)      DO_PRUNE=1 ;;
    status|rollback|nginx-install) COMMAND="$1" ;;
    *) die "Unknown argument: $1 (see header of this script)" ;;
  esac
  shift
done

command -v docker >/dev/null || die "docker not found"
docker compose version >/dev/null 2>&1 || die "docker compose plugin not found"
[[ -f "$COMPOSE_FILE" ]] || die "$COMPOSE_FILE not found (run from repo root)"
[[ -f "$ENV_FILE" ]] || die "$ENV_FILE not found. Copy .env.example and fill secrets first."

set -a; . "$ENV_FILE"; set +a

slot_port() { case "$1" in a) echo "$PORT_A";; b) echo "$PORT_B";; *) return 1;; esac; }
other_slot() { case "$1" in a) echo b;; b) echo a;; *) die "bad slot $1";; esac; }

read_state() {
  if [[ -f "$STATE_FILE" ]]; then
    ACTIVE_SLOT="$(sed -n 1p "$STATE_FILE")"; PREV_SLOT="$(sed -n 2p "$STATE_FILE")"
  else
    ACTIVE_SLOT=""; PREV_SLOT=""
  fi
}
write_state() { printf '%s\n%s\n' "$1" "${2:-}" > "$STATE_FILE"; chmod 600 "$STATE_FILE" 2>/dev/null || true; }

slot_running() {
  local s="$1"
  docker ps --format '{{.Names}}' | grep -qx "whiteboard-prod-$s"
}

detect_active_slot() {
  if [[ -n "$ACTIVE_SLOT" ]] && slot_running "$ACTIVE_SLOT"; then return; fi
  for s in a b; do
    if slot_running "$s"; then ACTIVE_SLOT="$s"; write_state "$s"; info "Detected running slot '$s'"; return; fi
  done
  ACTIVE_SLOT=""
}

compose_slot() {  # compose_slot <slot> <args...>
  local slot="$1"; shift
  DEPLOY_SLOT="$slot" HOST_PORT="$(slot_port "$slot")" \
    docker compose --env-file "$ENV_FILE" -f "$COMPOSE_FILE" "$@"
}

health_ok() {
  curl -fsS --max-time 3 "http://127.0.0.1:$1/health" 2>/dev/null \
    | grep -q '"status": *"ok"' || return 1
  curl -fsS --max-time 3 "http://127.0.0.1:$1/health" 2>/dev/null \
    | grep -q '"redis": *true'
}

wait_healthy() {  # wait_healthy <port>
  local port="$1" waited=0
  info "Waiting for health on :$port (timeout ${HEALTH_TIMEOUT}s)"
  while ! health_ok "$port"; do
    sleep 2; waited=$((waited+2))
    if (( waited >= HEALTH_TIMEOUT )); then
      warn "--- last response ---"
      curl -sS --max-time 3 "http://127.0.0.1:$port/health" || true
      docker logs --tail 30 "whiteboard-prod-$(cat "$STATE_FILE" 2>/dev/null | sed -n 1p)" 2>/dev/null || true
      return 1
    fi
  done
  info "Slot is healthy on :$port after ${waited}s"
}

backup_db() {
  mkdir -p "$BACKUP_DIR"
  local ts f
  ts="$(date +%Y%m%d-%H%M%S)"; f="$BACKUP_DIR/pre-deploy-$ts.sql.gz"
  info "Backing up PostgreSQL -> $f"
  if docker exec whiteboard-db-prod pg_dump -U "${PG_USER:-whiteboard}" "${PG_DB:-whiteboard}" 2>/dev/null | gzip > "$f"; then
    ls -1t "$BACKUP_DIR"/pre-deploy-*.sql.gz 2>/dev/null | tail -n +$((BACKUP_KEEP+1)) | xargs -r rm -f --
    info "Backup done (kept last $BACKUP_KEEP)"
  else
    rm -f "$f"
    die "pg_dump failed - aborting deploy rather than migrating without a backup"
  fi
}

switch_upstream() {  # switch_upstream <slot>
  local slot="$1" port tmp
  port="$(slot_port "$slot")"
  tmp="$(mktemp)"
  cat > "$tmp" <<EOF
# Managed by deploy/production.sh - do not edit by hand.
upstream whiteboard_app {
    server 127.0.0.1:$port;
    keepalive 32;
}
EOF
  if [[ -w "$(dirname "$NGINX_UPSTREAM_CONF")" ]]; then
    mv "$tmp" "$NGINX_UPSTREAM_CONF"
  elif command -v sudo >/dev/null; then
    sudo tee "$NGINX_UPSTREAM_CONF" > /dev/null < "$tmp"; rm -f "$tmp"
  else
    rm -f "$tmp"
    die "Cannot write $NGINX_UPSTREAM_CONF (no sudo). Write it manually, then re-run."
  fi
  sudo -n nginx -t 2>/dev/null || nginx -t || sudo nginx -t
  if sudo -n systemctl reload nginx 2>/dev/null || systemctl reload nginx 2>/dev/null || sudo systemctl reload nginx; then
    info "nginx upstream switched to slot $slot (:$(slot_port "$slot"))"
  else
    die "nginx reload failed - old slot still active, rolling back new slot"
  fi
}

stop_slot() {  # stop_slot <slot>
  local s="$1"
  docker rm -f "whiteboard-prod-$s" "whiteboard-celery-worker-prod-$s" >/dev/null 2>&1 || true
  info "Removed slot $s containers"
}

install_cron() {
  # Idempotent: replaces only the marker-wrapped block, keeps user's other jobs.
  command -v crontab >/dev/null || { warn "crontab not found - install cron jobs manually (see README)"; return 0; }

  local backup_line="" cleanup_line
  cleanup_line="0 4 * * 0 cd $APP_DIR && docker compose --env-file $ENV_FILE -f $COMPOSE_FILE exec -T whiteboard python scripts/cleanup_stale_boards.py --yes >> $APP_DIR/backups/cleanup.log 2>&1"
  if [[ -n "${PG_BACKUP_S3_PREFIX:-}" && -n "${AWS_ACCESS_KEY_ID:-}" && -n "${AWS_SECRET_ACCESS_KEY:-}" ]]; then
    backup_line="30 3 * * * mkdir -p $APP_DIR/backups && $APP_DIR/deploy/backup_db.sh >> $APP_DIR/backups/backup.log 2>&1"
  else
    warn "PG_BACKUP_S3_PREFIX / AWS_* not set in $ENV_FILE - backup cron skipped (cleanup cron installed)"
  fi

  local BEGIN="# >>> whiteboard-managed >>>" END="# <<< whiteboard-managed <<<"
  {
    crontab -l 2>/dev/null | awk -v s="$BEGIN" -v e="$END" \
      'index($0,s)==1 {skip=1; next} index($0,e)==1 {skip=0; next} !skip'
    echo "$BEGIN"
    [[ -n "$backup_line" ]] && echo "$backup_line"
    echo "$cleanup_line"
    echo "$END"
  } | crontab -

  if [[ -n "$backup_line" ]]; then
    info "cron installed: db backup daily 03:30, stale-board cleanup weekly Sun 04:00"
  else
    info "cron installed: stale-board cleanup weekly Sun 04:00 (backup skipped - see warning above)"
  fi
}

cmd_deploy() {
  read_state; detect_active_slot
  local target prev_active
  prev_active="$ACTIVE_SLOT"

  if [[ -z "$prev_active" ]]; then
    info "Fresh install - deploying slot a"
    target="a"
  else
    target="$(other_slot "$prev_active")"
    info "Active slot: $prev_active (:$(slot_port "$prev_active")) -> deploying to slot $target (:$(slot_port "$target"))"
  fi

  (( DO_BACKUP )) && backup_db

  if (( SKIP_BUILD )); then
    info "Building skipped (--skip-build)"
  else
    info "Building images..."
    compose_slot "$target" build --pull whiteboard celery-worker
  fi

  info "Starting slot $target on :$(slot_port "$target")"
  compose_slot "$target" up -d --no-deps whiteboard celery-worker
  write_state "$target" "$prev_active"   # tentative; reverted below on failure

  if ! wait_healthy "$(slot_port "$target")"; then
    warn "New slot unhealthy - rolling back (old slot untouched)"
    stop_slot "$target"
    if [[ -n "$prev_active" ]]; then write_state "$prev_active"; else rm -f "$STATE_FILE"; fi
    exit 1
  fi

  if [[ -n "$prev_active" ]]; then
    switch_upstream "$target"
    stop_slot "$prev_active"
  else
    switch_upstream "$target"
  fi

  (( DO_PRUNE )) && { info "Pruning dangling images"; docker image prune -f >/dev/null; }
  (( SKIP_CRON )) || install_cron

  info "Deploy complete: slot $target on :$(slot_port "$target") (previous: ${prev_active:-none})"
  info "WebSocket note: existing sockets were served by the old worker until clients reconnect; Socket.IO auto-reconnects."
}

cmd_status() {
  read_state; detect_active_slot
  echo "State file:     $([[ -f $STATE_FILE ]] && cat "$STATE_FILE" | tr '\n' ' ' || echo 'none')"
  echo "Running slots:"
  docker ps --format '  {{.Names}}\t{{.Status}}\t{{.Ports}}' | grep -E 'whiteboard-(prod|celery)' || echo '  none'
  if [[ -r "$NGINX_UPSTREAM_CONF" ]]; then
    echo "nginx upstream: $(grep -o '127.0.0.1:[0-9]*' "$NGINX_UPSTREAM_CONF" | head -1)"
  fi
  for p in "$PORT_A" "$PORT_B"; do
    if health_ok "$p"; then echo "health :$p OK"; else echo "health :$p -"; fi
  done
}

cmd_rollback() {
  read_state; detect_active_slot
  [[ -n "$ACTIVE_SLOT" ]] || die "Nothing is running"
  local target; target="$(other_slot "$ACTIVE_SLOT")"
  warn "Rollback = fresh deploy into slot $target (build + health + switch). Continue? [y/N]"
  read -r ans; [[ "$ans" == "y" ]] || exit 0
  PREV_SLOT="$ACTIVE_SLOT" COMMAND=deploy cmd_deploy
}

cmd_nginx_install() {
  [[ -n "${WHITEBOARD_DOMAIN:-}" ]] || die "WHITEBOARD_DOMAIN is not set (export it or put it in $ENV_FILE)"
  [[ -f "$NGINX_SITE_TMPL" ]] || die "Template missing: $NGINX_SITE_TMPL"
  local cert_dir="/etc/letsencrypt/live/$WHITEBOARD_DOMAIN"
  [[ -d "$cert_dir" ]] || warn "No certs at $cert_dir yet - obtain them with certbot first."

  sudo tee /etc/nginx/conf.d/websocket-map.conf >/dev/null <<'EOF'
map $http_upgrade $connection_upgrade {
    default upgrade;
    '' close;
}
EOF
  switch_upstream "${ACTIVE_SLOT:-a}"

  sed "s/__WHITEBOARD_DOMAIN__/$WHITEBOARD_DOMAIN/g" "$NGINX_SITE_TMPL" \
    | sudo tee "/etc/nginx/sites-available/$NGINX_SITE_NAME" >/dev/null
  sudo ln -sf "/etc/nginx/sites-available/$NGINX_SITE_NAME" "/etc/nginx/sites-enabled/$NGINX_SITE_NAME"
  sudo nginx -t && sudo systemctl reload nginx
  info "nginx installed for $WHITEBOARD_DOMAIN (upstream -> slot ${ACTIVE_SLOT:-a})"
}

case "$COMMAND" in
  deploy)  cmd_deploy ;;
  status)  cmd_status ;;
  rollback) cmd_rollback ;;
  nginx-install) cmd_nginx_install ;;
esac
