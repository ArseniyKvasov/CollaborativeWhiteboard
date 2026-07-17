# WhiteboardCollaborative

Collaborative infinite whiteboard built with FastAPI, Fabric.js and Socket.IO. The project is designed for iframe embedding into an external platform, JWT-based access control, realtime multi-user editing, and production deployment behind Nginx.

![Desktop Board](docs/images/whiteboard-desktop.png)

![Mobile Board](docs/images/whiteboard-mobile.png)

![Toolbar And Shapes](docs/images/whiteboard-toolbar.png)

## Features

- Infinite canvas with pan and zoom
- Realtime collaboration via Socket.IO
- JWT auth for REST, iframe access and websocket connection
- Per-user undo/redo
- Presence cursors
- Shape library, text, pencil, eraser, image upload
- Moderator controls for clearing board and drawing policy
- Production-ready Docker Compose deployment

## Stack

- Backend: FastAPI
- Realtime: python-socketio (Redis-backed for multi-worker scaling)
- Background jobs: Celery (Redis broker/backend) - image upload compression runs here, off the request path
- Frontend: Fabric.js + Bootstrap 5
- Database: PostgreSQL (falls back to a local SQLite file if `DATABASE_URL` is unset - see Local Run below)
- Auth: JWT HS256

## Development

### Docker Compose

```bash
docker compose up --build
```

Это поднимает `db`, `redis`, `whiteboard` и `celery-worker` (обрабатывает загрузку/сжатие изображений в фоне - см. [Celery](#celery) ниже). Сервис будет доступен на `http://localhost:8000`.

Открыть доску в dev-режиме без JWT:

- `http://localhost:8000/board/dev-board`

Если `DEBUG=True`, JWT отключается для HTTP/Socket.IO и доска может открываться напрямую в браузере.

## Production

1. Проверьте `CORS_ORIGINS` в `.env.production` и укажите домен основного сервиса.
2. Запустите:

```bash
docker compose --env-file .env.production -f docker-compose.prod.yml up -d --build
```

3. Проверка health:

```bash
curl http://localhost:8000/health
```

Остановка:

```bash
docker compose --env-file .env.production -f docker-compose.prod.yml down
```

### Local Run (uvicorn)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

`.env.example` is written for the Docker Compose network (`DATABASE_URL` points at the `db` host, `UPLOAD_DIR=/data/uploads`) - neither resolves outside a container. For a plain `uvicorn` run, edit `.env`:

```bash
DEBUG=True
# Falls back to a local SQLite file - drop the postgresql:// URL entirely,
# or point it at a Postgres instance reachable from your machine.
DATABASE_URL=app/boards.db
UPLOAD_DIR=app/uploads
# Redis is a hard dependency (presence/history/rate-limiting) - point at a
# reachable instance, e.g. `docker compose up -d redis` for just that service.
REDIS_URL=redis://localhost:6379/0
```

Запустите приложение через `uvicorn`:

```bash
uvicorn app.main:asgi_app --reload --host 0.0.0.0 --port 8000 --env-file .env
```

Открыть доску без токена в debug-режиме:

- `http://localhost:8000/board/dev-board`

Запустите Celery-воркер в отдельном терминале (см. [Celery](#celery)) - без него загрузка изображений останется в статусе "обрабатывается" навсегда.

## Celery

Загруженные изображения (`POST /api/board/{board_id}/upload-image`) сжимаются не в самом запросе, а в фоне через Celery - эндпоинт сразу возвращает `job_id`, а фронтенд опрашивает `GET /api/board/{board_id}/upload-image/{job_id}` и показывает плейсхолдер-загрузку до готовности.

В Docker Compose воркер (`celery-worker`) запускается автоматически вместе с `docker compose up`/`docker compose -f docker-compose.prod.yml up`. Для локального запуска через `uvicorn` его нужно поднять отдельно, с теми же переменными окружения (`REDIS_URL`, `UPLOAD_DIR`), что и сам сервис:

```bash
source .venv/bin/activate
export $(grep -v '^#' .env | xargs)  # or just export REDIS_URL/UPLOAD_DIR manually
celery -A app.celery_app worker --loglevel=info
```

(Celery's CLI doesn't read `.env` files itself like uvicorn's `--env-file` does - the variables need to already be in the shell's environment before `celery worker` starts.)

Без запущенного воркера сам сервис продолжит работать, но загрузка любого изображения зависнет в статусе `processing` навсегда - `job_id` просто никогда не будет обработан.

## Environment

- `JWT_SECRET` — общий секрет для JWT (HS256)
- `DATABASE_URL` — строка подключения к PostgreSQL (`postgresql://user:pass@host:5432/db`); если не задана, используется локальный SQLite-файл (`app/boards.db`)
- `REDIS_URL` — подключение к Redis (обязателен: presence, история undo/redo, rate-limiting, межпроцессный Socket.IO)
- `UPLOAD_DIR` — каталог для загруженных изображений (по умолчанию `app/uploads`)
- `CORS_ORIGINS` — список origin через запятую
- `DEBUG` — dev-режим; при `True` JWT не обязателен

Для production используется `.env.production`:

- `DEBUG=False`
- `HOST_PORT=18743`
- `DATABASE_URL=/data/boards.db` (persist volume `whiteboard_data`)
- `JWT_SECRET` задан
- `SERVICE_API_KEY` — ключ сервисных admin-операций

`HOST_PORT` задает внешний порт хоста для Docker Compose. Внутри контейнера приложение по-прежнему слушает `8000`.

Recommended production host port:

- `HOST_PORT=18743`

## JWT

Ожидаются claims:

- `user_id` (обязательно)
- `exp` (обязательно)
- `username` (для отображения курсоров)
- `role` (например `moderator` для права очистки доски)

JWT обязателен для:

- REST (заголовок `Authorization: Bearer <token>`)
- Socket.IO/iframe (`?token=<jwt>`)

## Service API Key

Для системных операций (без user JWT) используются endpoint’ы с заголовком:

`X-API-Key: <SERVICE_API_KEY>`

- `POST /api/admin/board/{board_id}/drawing`
  body: `{ "allow_students_draw": true|false }`
- `DELETE /api/admin/board/{board_id}`

Если `allow_students_draw=false`, рисовать через websocket смогут только пользователи с JWT role=`moderator`.

## iframe

```html
<iframe src="https://board-service/board/board123?token=JWT_TOKEN"></iframe>
```
