# WhiteboardCollaborative

Коллаборативная бесконечная доска: FastAPI + Fabric.js + Socket.IO.
Встраивается в сторонний сервис через `iframe`, авторизация — JWT.
![Доска](docs/images/whiteboard-desktop.png)

**Возможности:** бесконечный холст с pan/zoom, realtime-курсоры и совместное
редактирование, фигуры/текст/стикеры/карандаш/ластик, загрузка картинок (сжатие
в Celery, хранение в S3), undo/redo, блокировка объектов, слои, модерация
(очистка доски, политика «можно ли ученикам рисовать»), zero-downtime деплой,
бэкапы PostgreSQL в S3 по расписанию, автоочистка неактивных досок.

## Разработка

```bash
docker compose up --build
```

Поднимаются `db`, `redis`, `whiteboard`, `celery-worker`.

**Доска:** http://localhost:8642/board/dev-board (порт = `DEV_HOST_PORT`;
`DEBUG=True` в `.env` — JWT не требуется).

Локально без Docker: `pip install -r requirements.txt`, в `.env` указать
`DATABASE_URL=app/boards.db`, `UPLOAD_DIR=app/uploads`, доступный Redis;
`uvicorn app.main:asgi_app --reload --host 0.0.0.0 --port 8000 --env-file .env
--reload-exclude "app/uploads/*" --reload-exclude "*.db" --reload-exclude "*.db-*"`.
Celery: `celery -A app.celery_app worker --loglevel=info` — без него картинки
зависают в статусе `processing`.

## Деплой (Ubuntu 22.04/24.04)

Нужны: Docker + compose plugin, nginx, certbot, `awscli` (для бэкапов),
`crontab`. Домены основного сервиса и доски должны указывать A-записями на сервер.

```bash
# 1. Секреты (один раз). Замените домены!
sudo mkdir -p /opt/whiteboard && sudo chown "$USER":"$USER" /opt/whiteboard
export FASTCLASS_BASE_URL="https://fastclass.example.com"
export WHITEBOARD_BASE_URL="https://board.example.com"
cat > /opt/whiteboard/whiteboard.env <<EOF
WHITEBOARD_BASE_URL=${WHITEBOARD_BASE_URL}
WHITEBOARD_DOMAIN=${WHITEBOARD_BASE_URL#https://}
FASTCLASS_BASE_URL=${FASTCLASS_BASE_URL}
FASTCLASS_DOMAIN=${FASTCLASS_BASE_URL#https://}
WHITEBOARD_JWT_SECRET=$(openssl rand -hex 32)
WHITEBOARD_SERVICE_API_KEY=$(openssl rand -hex 32)
EOF
chmod 600 /opt/whiteboard/whiteboard.env

# 2. Код и конфиг
cd /opt && git clone https://github.com/ArseniyKvasov/CollaborativeWhiteboard.git collaborative-whiteboard
cd collaborative-whiteboard
set -a; . /opt/whiteboard/whiteboard.env; set +a
cat > .env.production <<EOF
DEBUG=False
HOST_PORT=18743
UVICORN_WORKERS=1
DATABASE_URL=postgresql://whiteboard:whiteboard_pass@db:5432/whiteboard
REDIS_URL=redis://redis:6379/0
UPLOAD_DIR=/data/uploads
CORS_ORIGINS=${FASTCLASS_BASE_URL},${WHITEBOARD_BASE_URL}
JWT_SECRET=${WHITEBOARD_JWT_SECRET}
SERVICE_API_KEY=${WHITEBOARD_SERVICE_API_KEY}
# --- S3 (медиа). Пусто = локальный диск. Бакет держать приватным. ---
MEDIA_S3_BUCKET=
MEDIA_S3_LOCATION=media
MEDIA_S3_ENDPOINT_URL=https://storage.yandexcloud.net
MEDIA_S3_REGION_NAME=ru-central1
MEDIA_S3_ACCESS_KEY_ID=
MEDIA_S3_SECRET_ACCESS_KEY=
# --- Бэкапы PostgreSQL в S3 ---
PG_BACKUP_S3_PREFIX=s3://whiteboard-postgres/postgres/production
AWS_ENDPOINT=https://storage.yandexcloud.net
AWS_REGION=ru-central1
AWS_ACCESS_KEY_ID=
AWS_SECRET_ACCESS_KEY=
BACKUP_KEEP_LOCAL=7
BACKUP_S3_RETENTION_DAYS=30
# --- Очистка неактивных досок ---
STALE_BOARD_DAYS=90
EOF
chmod 600 .env.production

# 3. Сертификат
sudo systemctl stop nginx
sudo certbot certonly --standalone -d "$WHITEBOARD_DOMAIN"
sudo systemctl start nginx

# 4. Nginx + первый деплой
./deploy/production.sh nginx-install
./deploy/production.sh
```

`./deploy/production.sh` при каждом запуске: build → старт неактивного слота
(порты 18743/18744, `db`/`redis` общие) → ожидание `/health` → переключение
nginx upstream (graceful reload, даунтайма нет) → удаление старого слота →
**идемпотентная установка cron'ов** (бэкап ежедневно 03:30, очистка досок вс
04:00 — только если заданы `PG_BACKUP_S3_PREFIX` и `AWS_*`). Перед деплоем
снимается локальный `pg_dump`. Если новый слот не прошёл health-check — старый
продолжает работать.

```bash
./deploy/production.sh status      # слоты, порты, upstream
./deploy/production.sh rollback    # вернуть предыдущий слот
./deploy/production.sh --skip-build --no-backup   # быстрые флаги
```

## Медиа в S3

Новые загрузки пишутся **только в S3** (сбой → retry Celery), раздаются
приложением по прежним ссылкам `/uploads/{board}/{file}` — фронтенд и данные
доски не меняются, бакет приватный. Без `MEDIA_S3_*` работает локальный диск.

Перенос существующих файлов (идемпотентно, можно перезапускать):

```bash
docker compose --env-file .env.production -f docker-compose.prod.yml \
  exec whiteboard python scripts/migrate_uploads_to_s3.py --dry-run
docker compose --env-file .env.production -f docker-compose.prod.yml \
  exec whiteboard python scripts/migrate_uploads_to_s3.py
# после проверки досок — освободить диск:
docker compose --env-file .env.production -f docker-compose.prod.yml \
  exec whiteboard python scripts/migrate_uploads_to_s3.py --delete-local
```

## Бэкапы и очистка

- **Бэкап БД**: cron (ставится деплоем) → `deploy/backup_db.sh`: дамп → gzip →
  S3 с проверкой размера; ротация `BACKUP_S3_RETENTION_DAYS` (30 дней) и
  локально 7 копий. Логи: `backups/backup.log`.
- **Восстановление**:
  ```bash
  aws s3 cp s3://whiteboard-postgres/postgres/production/db-XXXX.sql.gz . \
    --endpoint-url https://storage.yandexcloud.net
  # остановить app, пересоздать volume db, затем:
  gunzip -c db-XXXX.sql.gz | docker compose --env-file .env.production \
    -f docker-compose.prod.yml exec -T db psql -U whiteboard -d whiteboard
  ```
- **Неактивные доски**: `scripts/cleanup_stale_boards.py` (cron вс 04:00)
  удаляет доски без изменений `STALE_BOARD_DAYS` дней + их медиа (S3 и диск) +
  кэш; dry-run по умолчанию. Логи: `backups/cleanup.log`. Раз в месяц стоит
  проверять восстановление бэкапа на тестовой машине.

## Переменные окружения

| Переменная | Назначение |
|---|---|
| `JWT_SECRET`, `SERVICE_API_KEY` | секреты; обязательны в проде (валидация на старте) |
| `DATABASE_URL`, `REDIS_URL` | Postgres / Redis (Redis обязателен) |
| `CORS_ORIGINS` | origin'ы через запятую (сервис + доска) |
| `DEBUG` | `True` — JWT отключён (только dev) |
| `HOST_PORT` / `DEV_HOST_PORT` | порты prod/dev |
| `UVICORN_WORKERS` | держать `1` (см. комментарий в compose про sticky) |
| `MEDIA_S3_*` | бакет/ключи медиа; пусто = локальный диск |
| `PG_BACKUP_S3_PREFIX`, `AWS_*` | бакет и ключи бэкапов |
| `BACKUP_S3_RETENTION_DAYS`, `BACKUP_KEEP_LOCAL` | ротация бэкапов (30 / 7) |
| `STALE_BOARD_DAYS` | возраст неактивных досок для удаления (90) |
| `RATE_LIMIT_*`, `CELERY_WORKER_CONCURRENCY` | лимиты и воркеры (см. `.env.example`) |

Rate limiting: бюджет по `user_id` из JWT (600/мин) + backstop по IP (5000/мин)
+ отдельные бакеты на upload/miro/ws-token; лимиты на все мутирующие
Socket.IO-события. Настройка — в `.env.example`.

## Интеграция

JWT HS256; обязательные claims `user_id`, `exp`; `username` для курсоров;
`role`: `viewer` / `editor` / `moderator`. Секрет только на сервере.

```python
jwt.encode({"user_id": str(user.pk), "username": user.get_full_name(),
            "role": "moderator" if user.has_full_access else "editor",
            "board_id": board_id,
            "exp": datetime.now(timezone.utc) + timedelta(hours=1)},
           WHITEBOARD_JWT_SECRET, algorithm="HS256")
```

```html
<iframe src="https://board.example.com/board/BOARD_ID?token=JWT_TOKEN"
        allow="clipboard-read; clipboard-write"></iframe>
```

Служебные операции (заголовок `X-API-Key: SERVICE_API_KEY`):

```bash
curl -X POST "$WHITEBOARD_BASE_URL/api/admin/board/lesson-123/drawing" \
  -H "X-API-Key: $WHITEBOARD_SERVICE_API_KEY" -H "Content-Type: application/json" \
  -d '{"allow_students_draw": true}'
curl -X DELETE "$WHITEBOARD_BASE_URL/api/admin/board/lesson-123" \
  -H "X-API-Key: $WHITEBOARD_SERVICE_API_KEY"
```
