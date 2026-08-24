# WhiteboardCollaborative

Коллаборативная бесконечная доска на FastAPI, Fabric.js и Socket.IO. Проект
рассчитан на встраивание через `iframe` во внешний сервис, доступ по JWT,
realtime-редактирование несколькими пользователями и продакшен-деплой за Nginx.

![Desktop Board](docs/images/whiteboard-desktop.png)

![Mobile Board](docs/images/whiteboard-mobile.png)

![Toolbar And Shapes](docs/images/whiteboard-toolbar.png)

## Возможности

- Бесконечный холст с pan/zoom
- Realtime-коллаборация через Socket.IO
- JWT для REST, iframe-доступа и websocket-подключения
- Undo/redo на каждого пользователя
- Presence-курсоры
- Библиотека фигур, текст, карандаш, ластик, загрузка изображений
- Модераторские функции: очистка доски, политика рисования
- Zero-downtime деплой через `./deploy/production.sh`

## Стек

- Backend: FastAPI
- Realtime: python-socketio (Redis — для масштабирования на несколько воркеров)
- Фоновые задачи: Celery (брокер и бэкенд — Redis) — сжатие изображений уходит
  из HTTP-запроса в воркер
- Frontend: Fabric.js + Bootstrap 5
- База: PostgreSQL (без `DATABASE_URL` — локальный SQLite, см. «Локальный запуск»)
- Auth: JWT HS256

## Разработка (dev)

### Docker Compose

```bash
docker compose up --build
```

Поднимаются сервисы `db`, `redis`, `whiteboard` и `celery-worker` (загрузка и
сжатие картинок в фоне). Доска будет доступна на `http://localhost:8000`.

Открыть доску в dev-режиме без JWT:

- **http://localhost:8000/board/dev-board**

Если `DEBUG=True`, JWT отключается для HTTP/Socket.IO и доска открывается
напрямую в браузере.

Проверка здоровья: `curl http://localhost:8000/health`

### Локальный запуск (uvicorn)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

`.env.example` написан под сеть Docker Compose (`DATABASE_URL` указывает на хост
`db`, `UPLOAD_DIR=/data/uploads`) — вне контейнера они не резолвятся. Для
голого `uvicorn` поправьте `.env`:

```bash
DEBUG=True
# SQLite-файл рядом с кодом - строку postgresql:// можно просто удалить
DATABASE_URL=app/boards.db
UPLOAD_DIR=app/uploads
# Redis обязателен (presence, история undo/redo, rate-limiting):
REDIS_URL=redis://localhost:6379/0
```

Запуск:

```bash
uvicorn app.main:asgi_app --reload --host 0.0.0.0 --port 8000 --env-file .env \
  --reload-exclude "app/uploads/*" --reload-exclude "*.db" --reload-exclude "*.db-*"
```

`--reload-exclude` обязателен: `boards.db` и `app/uploads/` лежат внутри `app/`
и меняются при каждой операции; без исключений `--reload` перезапускает процесс
и рвёт все открытые WebSocket-соединения.

Celery-воркер запускается отдельно (см. [Celery](#celery)) — без него загрузка
изображений навсегда останется в статусе «обрабатывается».

## Деплой на продакшен (Ubuntu 22.04/24.04)

Деплой делается скриптом `./deploy/production.sh`: blue-green на одном хосте,
два слота приложения на портах 18743/18744, общие `db`/`redis`, переключение
через graceful reload Nginx. Даунтайма нет: если новый слот не прошёл
health-check, старый продолжает обслуживать трафик.

### 1. Зависимости на сервере

```bash
sudo apt update
sudo apt install -y ca-certificates curl gnupg git openssl nginx snapd ufw

sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
  | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
  | sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt update && sudo apt install -y docker-ce docker-ce-cli containerd.io \
  docker-buildx-plugin docker-compose-plugin
sudo systemctl enable --now docker
sudo usermod -aG docker "$USER"   # после этого переподключитесь по SSH

sudo snap install core && sudo snap refresh core
sudo snap install --classic certbot
sudo ln -sf /snap/bin/certbot /usr/local/bin/certbot
```

### 2. Домены и секреты

Задайте домены основного сервиса и доски и сгенерируйте секреты один раз:

```bash
export FASTCLASS_BASE_URL="https://fastclass.example.com"
export WHITEBOARD_BASE_URL="https://board.example.com"

sudo mkdir -p /opt/whiteboard && sudo chown "$USER":"$USER" /opt/whiteboard
cat > /opt/whiteboard/whiteboard.env <<EOF
WHITEBOARD_BASE_URL=${WHITEBOARD_BASE_URL}
WHITEBOARD_DOMAIN=${WHITEBOARD_BASE_URL#https://}
FASTCLASS_BASE_URL=${FASTCLASS_BASE_URL}
FASTCLASS_DOMAIN=${FASTCLASS_BASE_URL#https://}
WHITEBOARD_JWT_SECRET=$(openssl rand -hex 32)
WHITEBOARD_SERVICE_API_KEY=$(openssl rand -hex 32)
EOF
chmod 600 /opt/whiteboard/whiteboard.env
```

Создайте DNS A-запись `<WHITEBOARD_DOMAIN> -> IP сервера` и проверьте:
`getent hosts "$WHITEBOARD_DOMAIN"`.

### 3. Код и production-конфиг

```bash
sudo mkdir -p /opt && sudo chown "$USER":"$USER" /opt
cd /opt
git clone https://github.com/ArseniyKvasov/CollaborativeWhiteboard.git collaborative-whiteboard
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
EOF
chmod 600 .env.production
```

`JWT_SECRET` должен совпадать с секретом, которым FastClass подписывает токены;
`SERVICE_API_KEY` — для служебных admin-операций. SQLite в проде не использовать.

### 4. Сертификат Let's Encrypt

```bash
set -a; . /opt/whiteboard/whiteboard.env; set +a
sudo systemctl stop nginx   # если запущен (standalone-проверка)
sudo certbot certonly --standalone -d "$WHITEBOARD_DOMAIN"
sudo systemctl start nginx
sudo certbot renew --dry-run
```

### 5. Nginx и первый деплой

```bash
./deploy/production.sh nginx-install   # один раз: сайт, websocket-map, upstream, reload
./deploy/production.sh                 # каждый релиз: build -> health -> switch -> cleanup
```

Полезные команды:

```bash
./deploy/production.sh status          # активный слот, порты, upstream
./deploy/production.sh rollback        # задеплоить обратно предыдущий слот
./deploy/production.sh --skip-build    # без пересборки образов
./deploy/production.sh --prune         # почистить dangling-образы
```

Как это работает:

1. Скрипт поднимает **неактивный** слот (`whiteboard-prod-a` или `-b`) на своём
   порту; `db` и `redis` продолжают работать.
2. Ждёт `/health` = `ok` + `redis:true` (таймаут `HEALTH_TIMEOUT`, по умолчанию 180 с).
3. Перезаписывает `/etc/nginx/conf.d/whiteboard-upstream.conf` на новый порт и
   делает `systemctl reload nginx`. Старые соединения дорабатывают у старого
   воркера, Socket.IO сам переподключается на новый слот.
4. Только после успешного переключения удаляются контейнеры старого слота.
   Перед деплоем снимается `pg_dump` в `backups/` (хранятся последние 7).

Порты файрвола:

```bash
sudo ufw allow OpenSSH && sudo ufw allow 80/tcp && sudo ufw allow 443/tcp
sudo ufw --force enable && sudo ufw status
```

### 6. Проверка после деплоя

```bash
curl https://"$WHITEBOARD_DOMAIN"/health
```

В браузере: два пользователя видят правки друг друга, курсоры с именами,
картинки завершают обработку, в DevTools нет ошибок WebSocket/Socket.IO.
При нескольких воркерах за прокси для websocket-маршрута нужны sticky sessions;
при `UVICORN_WORKERS=1` (по умолчанию) ничего дополнительно не требуется.

## Celery

Изображения (`POST /api/board/{board_id}/upload-image`) сжимаются фоново:
эндпоинт сразу возвращает `job_id`, фронтенд опрашивает
`GET /api/board/{board_id}/upload-image/{job_id}` и показывает плейсхолдер до
готовности.

В Docker Compose воркер стартует автоматически. Локально — отдельным
терминалом с теми же переменными окружения (`REDIS_URL`, `UPLOAD_DIR`):

```bash
celery -A app.celery_app worker --loglevel=info
```

Без воркера сервис работает, но любая загрузка изображения зависнет в статусе
`processing` — `job_id` никогда не будет обработан.

## Environment

- `JWT_SECRET` — общий секрет JWT (HS256), обязателен в проде
- `SERVICE_API_KEY` — ключ служебных admin-операций, обязателен в проде
- `DATABASE_URL` — PostgreSQL (`postgresql://user:pass@host:5432/db`);
  без него — локальный SQLite (`app/boards.db`)
- `REDIS_URL` — Redis (обязателен: presence, история undo/redo,
  rate-limiting, межпроцессный Socket.IO)
- `UPLOAD_DIR` — каталог загруженных изображений (по умолчанию `app/uploads`)
- `CORS_ORIGINS` — список origin через запятую
- `DEBUG` — dev-режим; при `True` JWT не требуется
- `RATE_LIMIT_HTTP_PER_USER_PER_MINUTE` — бюджет запросов к `/api/*` на пользователя (из JWT), по умолчанию `600`
- `RATE_LIMIT_HTTP_PER_IP_PER_MINUTE` — backstop на IP от флуда, по умолчанию `5000`;
  держите высоким — университет может NAT-ить ~100 пользователей за один IP
- `RATE_LIMIT_UPLOAD_PER_MINUTE` / `RATE_LIMIT_UPLOAD_POLL_PER_MINUTE` — загрузка картинок (`30`)
  и поллинг статуса (`240`) в отдельных бакетах
- `RATE_LIMIT_MIRO_IMPORT_PER_MINUTE` — импорт из Miro (`5`, тяжёлый внешний трафик)
- `RATE_LIMIT_WS_TOKEN_PER_MINUTE` — выпуск/refresh ws-токенов (`60`)
- `RATE_LIMIT_SOCKET_PER_10S` — базовый лимит событий Socket.IO на соединение (`60`);
  производные: cursor ×5, undo/redo/hist 30/10с, save 12/10с, clear 6/10с
- `CELERY_WORKER_CONCURRENCY` — число воркер-процессов Celery (`2`)
- `HOST_PORT` — внешний порт Docker Compose (рекомендуется `18743`)
- `UVICORN_WORKERS` — держите `1`, см. комментарий в `docker-compose.prod.yml`
  про sticky sessions

## Медиа в S3 (Yandex Object Storage)

Загруженные картинки после сжатия складываются в S3, а раздаются приложением
по прежним ссылкам `/uploads/{board_id}/{file}` — фронтенд и данные доски не
заметили переезда. Файлы в бакете можно держать приватными.

- Если `MEDIA_S3_*` не заданы — работает локальный диск (`UPLOAD_DIR`), как раньше
- Раздача: `GET /uploads/...` стримит из S3; локальная копия используется как
  fallback для файлов до миграции
- Новые файлы пишутся **только в S3**; сбой загрузки ретраится Celery, а не
  теряется

Включение и перенос старых файлов:

```bash
# 1) Создайте приватный bucket и статические ключи в Yandex Cloud,
#    затем пропишите в .env.production:
# MEDIA_S3_BUCKET=whiteboard-prod-media
# MEDIA_S3_LOCATION=media
# MEDIA_S3_ENDPOINT_URL=https://storage.yandexcloud.net
# MEDIA_S3_REGION_NAME=ru-central1
# MEDIA_S3_ACCESS_KEY_ID=YCA...
# MEDIA_S3_SECRET_ACCESS_KEY=YCP...

./deploy/production.sh   # пересборка с boto3 + zero-downtime переключение

# 2) Миграция старых загрузок (идемпотентна: уже перенесённое пропускается)
docker compose --env-file .env.production -f docker-compose.prod.yml \
  exec whiteboard python scripts/migrate_uploads_to_s3.py --dry-run
docker compose --env-file .env.production -f docker-compose.prod.yml \
  exec whiteboard python scripts/migrate_uploads_to_s3.py

# 3) После проверки досок можно освободить диск:
docker compose --env-file .env.production -f docker-compose.prod.yml \
  exec whiteboard python scripts/migrate_uploads_to_s3.py --delete-local
```

Пока шаг 3 не выполнен, откат тривиален: уберите `MEDIA_S3_*` из
`.env.production` и повторите `./deploy/production.sh` — раздача вернётся на
локальный диск.

## JWT

Ожидаются claims: `user_id` (обязательно), `exp` (обязательно), `username`
(для курсоров), `role` (`moderator` даёт право очистки доски).

JWT обязателен для REST (`Authorization: Bearer <token>`) и Socket.IO/iframe
(`?token=<jwt>`).

Пример генерации на стороне основного сервиса:

```python
from datetime import datetime, timedelta, timezone
import jwt

def make_whiteboard_token(user, board_id):
    payload = {
        "user_id": str(user.pk),
        "username": user.get_full_name() or user.username,
        "role": "moderator" if user.has_full_access else "editor",
        "board_id": board_id,
        "exp": datetime.now(timezone.utc) + timedelta(hours=1),
    }
    return jwt.encode(payload, WHITEBOARD_JWT_SECRET, algorithm="HS256")
```

Секрет нельзя передавать во frontend или хранить в HTML.

## Service API Key

Служебные операции без user JWT, заголовок `X-API-Key: <SERVICE_API_KEY>`:

- `POST /api/admin/board/{board_id}/drawing` — тело `{ "allow_students_draw": true|false }`
- `DELETE /api/admin/board/{board_id}` — удалить доску

Разрешить ученикам рисовать:

```bash
set -a; . /opt/whiteboard/whiteboard.env; set +a
curl -X POST "${WHITEBOARD_BASE_URL}/api/admin/board/lesson-123/drawing" \
  -H "X-API-Key: $WHITEBOARD_SERVICE_API_KEY" \
  -H "Content-Type: application/json" \
  -d '{"allow_students_draw": true}'
```

При `allow_students_draw=false` рисовать смогут только пользователи с ролью
`moderator`.

## iframe

```html
<iframe
    src="https://board.example.com/board/BOARD_ID?token=JWT_TOKEN"
    title="Виртуальная доска"
    allow="clipboard-read; clipboard-write"
></iframe>
```

`BOARD_ID` должен быть стабильным для урока/класса; JWT создаётся на сервере и
передаётся только авторизованному пользователю.
