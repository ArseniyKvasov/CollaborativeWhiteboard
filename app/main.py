import asyncio
import html
import json
import logging
import os
import re
import sqlite3
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import parse_qs

import httpx
import jwt
import redis.asyncio as aioredis
import socketio
from fastapi import Depends, FastAPI, File, Header, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field
from starlette.requests import Request

from app.celery_app import celery_app
from app.image_processing import process_and_store_image
from app.tasks import process_image_task

BASE_DIR = Path(__file__).resolve().parent

JWT_SECRET = os.getenv("JWT_SECRET", "change-me")
SERVICE_API_KEY = os.getenv("SERVICE_API_KEY", "")
DATABASE_URL = os.getenv("DATABASE_URL", str(BASE_DIR / "boards.db"))
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
CORS_ORIGINS = [o.strip() for o in os.getenv("CORS_ORIGINS", "*").split(",") if o.strip()]
DEBUG_RAW = os.getenv("DEBUG", "false")
DEBUG = str(DEBUG_RAW).strip().lower() in {"1", "true", "yes", "on"}
DEBUG_USER_ID = os.getenv("DEBUG_USER_ID", "debug-user")
JWT_LEEWAY_SECONDS = int(os.getenv("JWT_LEEWAY_SECONDS", "45"))
WS_ACCESS_TTL_SECONDS = int(os.getenv("WS_ACCESS_TTL_SECONDS", "300"))
WS_REFRESH_TTL_SECONDS = int(os.getenv("WS_REFRESH_TTL_SECONDS", "28800"))
RATE_LIMIT_HTTP_PER_MINUTE = int(os.getenv("RATE_LIMIT_HTTP_PER_MINUTE", "120"))
RATE_LIMIT_SOCKET_PER_10S = int(os.getenv("RATE_LIMIT_SOCKET_PER_10S", "60"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("whiteboard")

# Shared across every worker process so Socket.IO broadcast, presence and
# undo/redo history all stay consistent once the app runs with more than one
# uvicorn worker/instance (previously all three lived in per-process memory,
# which only happened to work because the app ran as a single process).
redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)

app = FastAPI(title="Whiteboard Service", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def request_logging_and_rate_limit(request: Request, call_next):
    start = time.monotonic()
    client_ip = request.client.host if request.client else "unknown"

    if request.url.path.startswith("/api/"):
        window = int(time.time() // 60)
        key = f"wb:ratelimit:http:{client_ip}:{window}"
        try:
            count = await redis_client.incr(key)
            if count == 1:
                await redis_client.expire(key, 60)
        except Exception:
            count = 0  # Redis unavailable: fail open rather than blocking all traffic.
        if count > RATE_LIMIT_HTTP_PER_MINUTE:
            logger.warning("rate_limited method=%s path=%s ip=%s", request.method, request.url.path, client_ip)
            return JSONResponse(status_code=429, content={"detail": "Too many requests"})

    response = await call_next(request)
    duration_ms = int((time.monotonic() - start) * 1000)
    logger.info(
        "request method=%s path=%s status=%s duration_ms=%d ip=%s",
        request.method,
        request.url.path,
        response.status_code,
        duration_ms,
        client_ip,
    )
    return response


app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

UPLOAD_DIR = Path(os.getenv("UPLOAD_DIR", str(BASE_DIR / "uploads")))
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
app.mount("/uploads", StaticFiles(directory=str(UPLOAD_DIR)), name="uploads")

# Raw uploads are staged here just long enough for the Celery worker to pick
# them up (see /upload-image below) - kept outside the public /uploads mount
# since these are pre-compression originals, not something to ever serve.
PENDING_UPLOAD_DIR = UPLOAD_DIR / "_pending"
PENDING_UPLOAD_DIR.mkdir(parents=True, exist_ok=True)


ROLE_RANK = {"viewer": 1, "editor": 2, "owner": 3}
PENDING_OWNER_ID = "__pending_moderator__"
MAX_BOARD_BYTES = 30 * 1024 * 1024
DEFAULT_SURFACE_ID = "main"
SOCKET_MAX_BUFFER_BYTES = 20 * 1024 * 1024
OPS_COMPACT_COUNT_THRESHOLD = 700
# Raw upload cap before server-side processing. The client already downsizes
# images to ~2MB/2400px before sending, this is just generous headroom for
# clients that skip that step (or a future Miro import feeding raw images in).
MAX_UPLOAD_IMAGE_BYTES = 10 * 1024 * 1024
OPS_COMPACT_BYTES_THRESHOLD = 4 * 1024 * 1024

MIRO_API_BASE = os.getenv("MIRO_API_BASE", "https://api.miro.com/v2")
MIRO_IMPORT_MAX_ITEMS = int(os.getenv("MIRO_IMPORT_MAX_ITEMS", "300"))
MIRO_IMPORT_PAGE_SIZE = 50
MIRO_IMPORT_HTTP_TIMEOUT = 20.0
STICKER_DEFAULT_FILL = "#fef08a"


@dataclass
class UserContext:
    user_id: str
    username: str
    role: str
    payload: dict[str, Any]


class CreateBoardRequest(BaseModel):
    board_id: Optional[str] = Field(default=None, min_length=1, max_length=128)


class SaveBoardRequest(BaseModel):
    canvas_json: dict[str, Any]


class MemberRequest(BaseModel):
    user_id: str = Field(min_length=1, max_length=128)
    role: str = Field(pattern="^(viewer|editor)$")


class DrawingPolicyRequest(BaseModel):
    allow_students_draw: bool = True


class WsTokenRefreshRequest(BaseModel):
    board_id: str = Field(min_length=1, max_length=128)
    refresh_token: str = Field(min_length=1)


class MiroImportRequest(BaseModel):
    miro_board_id: str = Field(min_length=1, max_length=128)
    # A Miro personal access token (Settings -> Your apps -> a token with
    # boards:read scope), pasted in by the user. Simpler than a full OAuth app
    # registration/redirect flow for a first version; not stored server-side.
    miro_token: str = Field(min_length=1)


try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
except ImportError:
    psycopg2 = None
    RealDictCursor = None

IS_POSTGRES = DATABASE_URL.startswith(("postgresql://", "postgres://"))

if IS_POSTGRES and psycopg2 is None:
    print("[DB] Warning: DATABASE_URL points to PostgreSQL, but psycopg2-binary is not installed!")


class DbConnectionWrapper:
    def __init__(self, conn, is_postgres: bool):
        self._conn = conn
        self._is_postgres = is_postgres

    def execute(self, sql: str, params: tuple = ()):
        if self._is_postgres:
            sql = sql.replace("?", "%s")
            if "INSERT OR IGNORE" in sql:
                sql = sql.replace("INSERT OR IGNORE INTO", "INSERT INTO")
                if "board_members" in sql:
                    sql = sql + " ON CONFLICT (board_id, user_id) DO NOTHING"
                elif "boards" in sql:
                    sql = sql + " ON CONFLICT (board_id) DO NOTHING"
            if "AUTOINCREMENT" in sql:
                sql = sql.replace("AUTOINCREMENT", "")
                sql = sql.replace("INTEGER PRIMARY KEY", "SERIAL PRIMARY KEY")
                sql = sql.replace("integer primary key", "serial primary key")
            if sql.strip().upper() == "BEGIN IMMEDIATE":
                class DummyCursor:
                    def fetchone(self): return None
                    def fetchall(self): return []
                return DummyCursor()

            is_insert_ops = "INSERT INTO board_ops" in sql
            if is_insert_ops:
                sql = sql + " RETURNING id"

            cur = self._conn.cursor(cursor_factory=RealDictCursor)
            cur.execute(sql, params)
            if is_insert_ops:
                row = cur.fetchone()
                cur.lastrowid = row["id"] if row else None
            return cur
        else:
            return self._conn.execute(sql, params)

    def commit(self):
        self._conn.commit()

    def rollback(self):
        self._conn.rollback()

    def close(self):
        self._conn.close()

    def __getattr__(self, name):
        return getattr(self._conn, name)


def get_db():
    if IS_POSTGRES:
        url = DATABASE_URL
        if url.startswith("postgres://"):
            url = url.replace("postgres://", "postgresql://", 1)
        conn = psycopg2.connect(url)
        return DbConnectionWrapper(conn, True)
    else:
        conn = sqlite3.connect(DATABASE_URL, timeout=10.0)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys = ON")
        conn.execute("PRAGMA journal_mode = WAL")
        conn.execute("PRAGMA synchronous = NORMAL")
        return DbConnectionWrapper(conn, False)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def init_db() -> None:
    conn = get_db()
    try:
        if IS_POSTGRES:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS boards (
                    board_id TEXT PRIMARY KEY,
                    canvas_json TEXT NOT NULL,
                    owner_id TEXT NOT NULL,
                    allow_students_draw INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    last_compacted_seq_id INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS board_members (
                    board_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    role TEXT NOT NULL CHECK(role IN ('owner', 'editor', 'viewer')),
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (board_id, user_id),
                    FOREIGN KEY (board_id) REFERENCES boards(board_id) ON DELETE CASCADE
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS board_ops (
                    id SERIAL PRIMARY KEY,
                    board_id TEXT NOT NULL,
                    op_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (board_id) REFERENCES boards(board_id) ON DELETE CASCADE
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_board_ops_board_id_id ON board_ops(board_id, id)")
            rows = conn.execute("SELECT column_name AS name FROM information_schema.columns WHERE table_name = 'boards'").fetchall()
            cols = {r["name"] for r in rows}
            if "allow_students_draw" not in cols:
                conn.execute("ALTER TABLE boards ADD COLUMN allow_students_draw INTEGER NOT NULL DEFAULT 1")
            if "last_compacted_seq_id" not in cols:
                conn.execute("ALTER TABLE boards ADD COLUMN last_compacted_seq_id INTEGER NOT NULL DEFAULT 0")
        else:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS boards (
                    board_id TEXT PRIMARY KEY,
                    canvas_json TEXT NOT NULL,
                    owner_id TEXT NOT NULL,
                    allow_students_draw INTEGER NOT NULL DEFAULT 1,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    last_compacted_seq_id INTEGER NOT NULL DEFAULT 0
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS board_members (
                    board_id TEXT NOT NULL,
                    user_id TEXT NOT NULL,
                    role TEXT NOT NULL CHECK(role IN ('owner', 'editor', 'viewer')),
                    created_at TEXT NOT NULL,
                    PRIMARY KEY (board_id, user_id),
                    FOREIGN KEY (board_id) REFERENCES boards(board_id) ON DELETE CASCADE
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS board_ops (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    board_id TEXT NOT NULL,
                    op_json TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY (board_id) REFERENCES boards(board_id) ON DELETE CASCADE
                )
                """
            )
            conn.execute("CREATE INDEX IF NOT EXISTS idx_board_ops_board_id_id ON board_ops(board_id, id)")
            cols = {row["name"] for row in conn.execute("PRAGMA table_info(boards)").fetchall()}
            if "allow_students_draw" not in cols:
                conn.execute("ALTER TABLE boards ADD COLUMN allow_students_draw INTEGER NOT NULL DEFAULT 1")
            if "last_compacted_seq_id" not in cols:
                conn.execute("ALTER TABLE boards ADD COLUMN last_compacted_seq_id INTEGER NOT NULL DEFAULT 0")
        conn.commit()
    finally:
        conn.close()


# Asynchronous write-queue manager to serialize DB writes and run them in thread executor.
class BoardTaskManager:
    def __init__(self):
        self._queues: dict[str, asyncio.Queue] = {}
        self._workers: dict[str, asyncio.Task] = {}

    def _get_queue(self, board_id: str) -> asyncio.Queue:
        if board_id not in self._queues:
            self._queues[board_id] = asyncio.Queue()
            self._workers[board_id] = asyncio.create_task(self._worker(board_id))
        return self._queues[board_id]

    async def _worker(self, board_id: str):
        queue = self._queues[board_id]
        while True:
            try:
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=600.0)
                except asyncio.TimeoutError:
                    if board_id in self._queues and self._queues[board_id].empty():
                        self._queues.pop(board_id, None)
                        self._workers.pop(board_id, None)
                        break
                    continue

                fn, args, kwargs, future = item
                try:
                    res = await asyncio.to_thread(fn, *args, **kwargs)
                    if not future.cancelled():
                        future.set_result(res)
                except Exception as exc:
                    if not future.cancelled():
                        future.set_exception(exc)
                finally:
                    queue.task_done()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.exception("BoardTaskManager worker error for %s: %s", board_id, e)
                await asyncio.sleep(0.1)

    async def run(self, board_id: str, fn, *args, **kwargs):
        queue = self._get_queue(board_id)
        future = asyncio.get_running_loop().create_future()
        await queue.put((fn, args, kwargs, future))
        return await future

    async def drain(self, timeout: float = 10.0) -> None:
        """Wait for all in-flight writes to finish, so a deploy/restart doesn't
        drop the last few edits a client just sent."""
        queues = list(self._queues.values())
        if not queues:
            return
        try:
            await asyncio.wait_for(asyncio.gather(*(q.join() for q in queues)), timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning("BoardTaskManager.drain timed out with writes still pending")


board_task_manager = BoardTaskManager()
compaction_tasks: dict[str, asyncio.Task] = {}


def trigger_compaction_debounced(board_id: str):
    if board_id in compaction_tasks:
        compaction_tasks[board_id].cancel()

    async def _debounced():
        try:
            await asyncio.sleep(5.0)
            # With multiple workers/instances, more than one process can debounce
            # a compaction for the same board at once. A short-lived Redis lock
            # keeps only one of them actually doing the work.
            lock_key = f"wb:compact-lock:{board_id}"
            try:
                acquired = await redis_client.set(lock_key, "1", nx=True, ex=30)
            except Exception:
                acquired = True  # Redis unavailable: fall back to old single-process behavior.
            if not acquired:
                return
            await board_task_manager.run(board_id, _compact_board_db, board_id)
        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.exception("Error compacting board %s: %s", board_id, e)
        finally:
            compaction_tasks.pop(board_id, None)

    task = asyncio.create_task(_debounced())
    compaction_tasks[board_id] = task


def _compact_board_db(board_id: str):
    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        max_op_row = conn.execute("SELECT MAX(id) AS max_id FROM board_ops WHERE board_id = ?", (board_id,)).fetchone()
        max_id = max_op_row["max_id"] if max_op_row and max_op_row["max_id"] is not None else None
        if max_id is None:
            conn.commit()
            return

        compacted_canvas = _read_board_canvas(conn, board_id)
        _ensure_board_size_limit(compacted_canvas)
        conn.execute(
            "UPDATE boards SET canvas_json = ?, last_compacted_seq_id = ? WHERE board_id = ?",
            (json.dumps(compacted_canvas, ensure_ascii=False), max_id, board_id),
        )
        conn.execute("DELETE FROM board_ops WHERE board_id = ? AND id <= ?", (board_id, max_id))
        conn.commit()
        logger.info("Board %s compacted up to sequence %s", board_id, max_id)
    except Exception as exc:
        conn.rollback()
        logger.exception("Failed to compact board %s: %s", board_id, exc)
        raise exc
    finally:
        conn.close()


def _db_read_canvas_only(board_id: str) -> dict[str, Any]:
    conn = get_db()
    try:
        return _read_board_canvas(conn, board_id)
    finally:
        conn.close()


def _db_read_canvas_and_max_id(board_id: str) -> tuple[dict[str, Any], int]:
    conn = get_db()
    try:
        row = conn.execute("SELECT last_compacted_seq_id FROM boards WHERE board_id = ?", (board_id,)).fetchone()
        last_compacted = row["last_compacted_seq_id"] if row else 0
        max_op_row = conn.execute("SELECT MAX(id) AS max_id FROM board_ops WHERE board_id = ?", (board_id,)).fetchone()
        max_op_id = max_op_row["max_id"] if max_op_row and max_op_row["max_id"] is not None else last_compacted
        canvas = _read_board_canvas(conn, board_id)
        return canvas, max_op_id
    finally:
        conn.close()



def _load_connect_data(board_id: str, user: UserContext) -> tuple[str, bool, dict[str, Any], int, int]:
    conn = get_db()
    try:
        ensure_user_board_access(conn, board_id, user)
        ensure_board_exists(conn, board_id)
        board_role = require_role(conn, board_id, user.user_id, "viewer")
        row = conn.execute("SELECT canvas_json, last_compacted_seq_id FROM boards WHERE board_id = ?", (board_id,)).fetchone()
        last_compacted_seq_id = row["last_compacted_seq_id"] if row else 0
        max_op_row = conn.execute("SELECT MAX(id) AS max_id FROM board_ops WHERE board_id = ?", (board_id,)).fetchone()
        max_op_id = max_op_row["max_id"] if max_op_row and max_op_row["max_id"] is not None else last_compacted_seq_id
        canvas_json = _read_board_canvas(conn, board_id)
        allow_students_draw = board_allows_students_draw(conn, board_id)
        return board_role, allow_students_draw, canvas_json, last_compacted_seq_id, max_op_id
    finally:
        conn.close()


def _get_missed_ops(board_id: str, last_seen_seq: int) -> list[dict[str, Any]]:
    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT id, op_json FROM board_ops WHERE board_id = ? AND id > ? ORDER BY id ASC",
            (board_id, last_seen_seq)
        ).fetchall()
        ops = []
        for r in rows:
            try:
                op = json.loads(r["op_json"])
                if isinstance(op, dict):
                    op["seq"] = r["id"]
                    ops.append(op)
            except Exception:
                pass
        return ops
    finally:
        conn.close()


def _append_board_ops_with_ids(conn: sqlite3.Connection, board_id: str, ops: list[dict[str, Any]]) -> tuple[str, list[int]]:
    if not ops:
        updated_at = now_iso()
        conn.execute("UPDATE boards SET updated_at = ? WHERE board_id = ?", (updated_at, board_id))
        return updated_at, []

    ts = now_iso()
    op_ids = []
    for op in ops:
        cursor = conn.execute(
            "INSERT INTO board_ops (board_id, op_json, created_at) VALUES (?, ?, ?)",
            (board_id, json.dumps(op, ensure_ascii=False), ts),
        )
        op_ids.append(cursor.lastrowid)

    updated_at = now_iso()
    conn.execute("UPDATE boards SET updated_at = ? WHERE board_id = ?", (updated_at, board_id))
    return updated_at, op_ids


def _db_update(board_id: str, new_canvas: dict[str, Any], user: UserContext) -> tuple[list[dict[str, Any]], str, list[int], bool, dict[str, Any]]:
    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        old_canvas = _read_board_canvas(conn, board_id)
        action = _build_action(old_canvas, new_canvas)
        applied_ops = []
        if action:
            _extract_added_objects(action, new_canvas)
            added_ids = set(action.get("added_ids", []))
            for obj in action.get("added_objects", []):
                if isinstance(obj, dict):
                    applied_ops.append({"type": "add", "object": obj})
            for obj in action.get("modified_after", []):
                if isinstance(obj, dict) and obj.get("obj_id") not in added_ids:
                    applied_ops.append({"type": "update", "object": obj})
            for obj in action.get("removed_objects", []):
                obj_id = obj.get("obj_id") if isinstance(obj, dict) else None
                if isinstance(obj_id, str) and obj_id:
                    applied_ops.append({"type": "remove", "obj_id": obj_id})

        _ensure_board_size_limit(new_canvas)
        updated_at, op_ids = _append_board_ops_with_ids(conn, board_id, applied_ops)
        
        compact_stats = conn.execute(
            "SELECT COUNT(*) AS cnt, COALESCE(SUM(LENGTH(op_json)), 0) AS bytes FROM board_ops WHERE board_id = ?",
            (board_id,),
        ).fetchone()
        op_count = int(compact_stats["cnt"]) if compact_stats else 0
        op_bytes = int(compact_stats["bytes"]) if compact_stats else 0
        should_compact = (op_count >= OPS_COMPACT_COUNT_THRESHOLD or op_bytes >= OPS_COMPACT_BYTES_THRESHOLD)

        conn.commit()
        return applied_ops, updated_at, op_ids, should_compact, action
    except Exception as exc:
        conn.rollback()
        raise exc
    finally:
        conn.close()


def _db_apply_ops(board_id: str, ops: list[dict[str, Any]], user: UserContext) -> tuple[list[dict[str, Any]], list[dict[str, Any]], str, list[int], bool]:
    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        current = _read_board_canvas(conn, board_id)
        next_canvas, applied_ops, inverse_ops = _apply_ops_build_inverse(current, ops, user)
        if not applied_ops:
            conn.commit()
            return [], [], now_iso(), [], False

        _ensure_board_size_limit(next_canvas)
        updated_at, op_ids = _append_board_ops_with_ids(conn, board_id, applied_ops)
        
        compact_stats = conn.execute(
            "SELECT COUNT(*) AS cnt, COALESCE(SUM(LENGTH(op_json)), 0) AS bytes FROM board_ops WHERE board_id = ?",
            (board_id,),
        ).fetchone()
        op_count = int(compact_stats["cnt"]) if compact_stats else 0
        op_bytes = int(compact_stats["bytes"]) if compact_stats else 0
        should_compact = (op_count >= OPS_COMPACT_COUNT_THRESHOLD or op_bytes >= OPS_COMPACT_BYTES_THRESHOLD)

        conn.commit()
        return applied_ops, inverse_ops, updated_at, op_ids, should_compact
    except Exception as exc:
        conn.rollback()
        raise exc
    finally:
        conn.close()


def _db_clear(board_id: str, cleared_canvas: dict[str, Any]) -> str:
    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        updated_at = now_iso()
        conn.execute("DELETE FROM board_ops WHERE board_id = ?", (board_id,))
        conn.execute(
            "UPDATE boards SET canvas_json = ?, updated_at = ?, last_compacted_seq_id = 0 WHERE board_id = ?",
            (json.dumps(cleared_canvas, ensure_ascii=False), updated_at, board_id)
        )
        conn.commit()
        return updated_at
    except Exception as exc:
        conn.rollback()
        raise exc
    finally:
        conn.close()


def _db_undo(board_id: str, action: Any, user: UserContext) -> tuple[list[dict[str, Any]], dict[str, Any], str, list[int], bool]:
    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        current = _read_board_canvas(conn, board_id)
        if isinstance(action, dict) and isinstance(action.get("undo_ops"), list):
            applied_ops = action["undo_ops"]
            next_canvas = _apply_prepared_ops(
                current,
                applied_ops,
                user,
            )
        else:
            next_canvas = _apply_undo_action(current, action)
            next_canvas = _normalize_canvas(next_canvas, user)
            applied_ops = []

        _ensure_board_size_limit(next_canvas)
        if applied_ops:
            updated_at, op_ids = _append_board_ops_with_ids(conn, board_id, applied_ops)
        else:
            updated_at = _replace_board_canvas_baseline(conn, board_id, next_canvas)
            op_ids = []

        compact_stats = conn.execute(
            "SELECT COUNT(*) AS cnt, COALESCE(SUM(LENGTH(op_json)), 0) AS bytes FROM board_ops WHERE board_id = ?",
            (board_id,),
        ).fetchone()
        op_count = int(compact_stats["cnt"]) if compact_stats else 0
        op_bytes = int(compact_stats["bytes"]) if compact_stats else 0
        should_compact = (op_count >= OPS_COMPACT_COUNT_THRESHOLD or op_bytes >= OPS_COMPACT_BYTES_THRESHOLD)

        conn.commit()
        return applied_ops, next_canvas, updated_at, op_ids, should_compact
    except Exception as exc:
        conn.rollback()
        raise exc
    finally:
        conn.close()


def _db_redo(board_id: str, action: Any, user: UserContext) -> tuple[list[dict[str, Any]], dict[str, Any], str, list[int], bool]:
    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        current = _read_board_canvas(conn, board_id)
        if isinstance(action, dict) and isinstance(action.get("redo_ops"), list):
            applied_ops = action["redo_ops"]
            next_canvas = _apply_prepared_ops(
                current,
                applied_ops,
                user,
            )
        else:
            next_canvas = _apply_redo_action_with_added(current, action)
            next_canvas = _normalize_canvas(next_canvas, user)
            applied_ops = []

        _ensure_board_size_limit(next_canvas)
        if applied_ops:
            updated_at, op_ids = _append_board_ops_with_ids(conn, board_id, applied_ops)
        else:
            updated_at = _replace_board_canvas_baseline(conn, board_id, next_canvas)
            op_ids = []

        compact_stats = conn.execute(
            "SELECT COUNT(*) AS cnt, COALESCE(SUM(LENGTH(op_json)), 0) AS bytes FROM board_ops WHERE board_id = ?",
            (board_id,),
        ).fetchone()
        op_count = int(compact_stats["cnt"]) if compact_stats else 0
        op_bytes = int(compact_stats["bytes"]) if compact_stats else 0
        should_compact = (op_count >= OPS_COMPACT_COUNT_THRESHOLD or op_bytes >= OPS_COMPACT_BYTES_THRESHOLD)

        conn.commit()
        return applied_ops, next_canvas, updated_at, op_ids, should_compact
    except Exception as exc:
        conn.rollback()
        raise exc
    finally:
        conn.close()


@app.on_event("startup")
def on_startup() -> None:
    init_db()


@app.on_event("shutdown")
async def on_shutdown() -> None:
    # Let in-flight board writes finish before the process exits, so a deploy
    # or restart doesn't silently drop a client's last edit.
    await board_task_manager.drain()
    await redis_client.aclose()


def parse_bearer(authorization: Optional[str]) -> Optional[str]:
    if not authorization:
        return None
    parts = authorization.split()
    if len(parts) == 2 and parts[0].lower() == "bearer":
        return parts[1]
    return None


def decode_jwt(token: str) -> dict[str, Any]:
    try:
        payload = jwt.decode(
            token,
            JWT_SECRET,
            algorithms=["HS256"],
            options={"require": ["exp", "user_id"]},
            leeway=JWT_LEEWAY_SECONDS,
        )
    except jwt.ExpiredSignatureError as exc:
        raise HTTPException(status_code=401, detail="Token expired") from exc
    except jwt.InvalidTokenError as exc:
        raise HTTPException(status_code=401, detail="Invalid token") from exc

    user_id = payload.get("user_id")
    if not user_id:
        raise HTTPException(status_code=401, detail="Token missing user_id")
    return payload


def issue_jwt(payload: dict[str, Any], ttl_seconds: int) -> tuple[str, int]:
    exp = int(time.time()) + max(30, int(ttl_seconds))
    token_payload = dict(payload)
    token_payload["exp"] = exp
    encoded = jwt.encode(token_payload, JWT_SECRET, algorithm="HS256")
    return encoded, exp


def issue_ws_tokens(user: UserContext, board_id: str) -> dict[str, Any]:
    shared = {
        "user_id": user.user_id,
        "username": user.username,
        "role": user.role,
        "board_id": board_id,
    }
    ws_token, ws_token_exp = issue_jwt({**shared, "type": "ws_access"}, WS_ACCESS_TTL_SECONDS)
    ws_refresh_token, ws_refresh_token_exp = issue_jwt({**shared, "type": "ws_refresh"}, WS_REFRESH_TTL_SECONDS)
    return {
        "ws_token": ws_token,
        "ws_token_exp": ws_token_exp,
        "ws_refresh_token": ws_refresh_token,
        "ws_refresh_token_exp": ws_refresh_token_exp,
    }


def authenticate_request(
    authorization: Optional[str] = Header(default=None),
    token: Optional[str] = Query(default=None),
) -> UserContext:
    bearer = parse_bearer(authorization)
    resolved_token = bearer or token
    if not resolved_token and DEBUG:
        return UserContext(
            user_id=DEBUG_USER_ID,
            username="Debug",
            role="moderator",
            payload={"user_id": DEBUG_USER_ID, "debug": True, "username": "Debug", "role": "moderator"},
        )
    if not resolved_token:
        raise HTTPException(status_code=401, detail="Missing token")
    payload = decode_jwt(resolved_token)
    return UserContext(
        user_id=str(payload["user_id"]),
        username=str(payload.get("username") or payload.get("name") or payload["user_id"]),
        role=str(payload.get("role") or "editor"),
        payload=payload,
    )


def authenticate_service_key(
    x_api_key: Optional[str] = Header(default=None, alias="X-API-Key"),
    authorization: Optional[str] = Header(default=None),
) -> None:
    provided = x_api_key or parse_bearer(authorization)
    if not SERVICE_API_KEY:
        raise HTTPException(status_code=500, detail="SERVICE_API_KEY is not configured")
    if provided != SERVICE_API_KEY:
        raise HTTPException(status_code=401, detail="Invalid API key")


def board_row(conn: sqlite3.Connection, board_id: str) -> Optional[sqlite3.Row]:
    return conn.execute("SELECT * FROM boards WHERE board_id = ?", (board_id,)).fetchone()


def get_user_role(conn: sqlite3.Connection, board_id: str, user_id: str) -> Optional[str]:
    b = board_row(conn, board_id)
    if not b:
        return None
    if b["owner_id"] == user_id:
        return "owner"
    row = conn.execute(
        "SELECT role FROM board_members WHERE board_id = ? AND user_id = ?",
        (board_id, user_id),
    ).fetchone()
    return row["role"] if row else None


def require_role(conn: sqlite3.Connection, board_id: str, user_id: str, min_role: str) -> str:
    if DEBUG and user_id == DEBUG_USER_ID:
        return "owner"
    role = get_user_role(conn, board_id, user_id)
    if role is None:
        raise HTTPException(status_code=403, detail="No access to board")
    if ROLE_RANK[role] < ROLE_RANK[min_role]:
        raise HTTPException(status_code=403, detail="Insufficient permissions")
    return role


def ensure_board_exists(conn: sqlite3.Connection, board_id: str) -> sqlite3.Row:
    row = board_row(conn, board_id)
    if not row:
        raise HTTPException(status_code=404, detail="Board not found")
    return row


def board_allows_students_draw(conn: sqlite3.Connection, board_id: str) -> bool:
    row = ensure_board_exists(conn, board_id)
    return bool(row["allow_students_draw"])


def default_canvas_state() -> dict[str, Any]:
    return {"version": "6.0.0", "objects": [], "background": "#ffffff"}


def _new_id() -> str:
    return uuid.uuid4().hex


def _json_size_bytes(value: Any) -> int:
    return len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


def _ensure_board_size_limit(value: Any) -> None:
    size = _json_size_bytes(value)
    if size > MAX_BOARD_BYTES:
        raise ValueError(f"Board size exceeds {MAX_BOARD_BYTES // (1024 * 1024)} MB ({size} bytes)")


def _normalize_runtime_canvas(canvas_json: Any, user: Optional[UserContext] = None) -> dict[str, Any]:
    if not isinstance(canvas_json, dict):
        return default_canvas_state()
    normalized = dict(canvas_json)
    objects = normalized.get("objects")
    if not isinstance(objects, list):
        objects = []
    out_objects: list[dict[str, Any]] = []
    for raw in objects:
        if not isinstance(raw, dict):
            continue
        obj = dict(raw)
        if not obj.get("obj_id"):
            obj["obj_id"] = _new_id()
        if not obj.get("author_id"):
            obj["author_id"] = user.user_id if user else "unknown"
        if not obj.get("author_name"):
            obj["author_name"] = user.username if user else "user"
        out_objects.append(obj)
    normalized["objects"] = out_objects
    if "version" not in normalized:
        normalized["version"] = "6.0.0"
    if "background" not in normalized:
        normalized["background"] = "#ffffff"
    return normalized


def _normalize_lite_canvas_state(state: Any) -> dict[str, Any]:
    if not isinstance(state, dict):
        return {"v": 2, "type": "lite_canvas_relative", "objects": []}
    objects = state.get("objects")
    if not isinstance(objects, list):
        objects = []
    normalized_objects: list[dict[str, Any]] = []
    for item in objects:
        if not isinstance(item, dict):
            continue
        rel_raw = item.get("rel")
        if not isinstance(rel_raw, dict):
            rel_raw = item.get("object") if isinstance(item.get("object"), dict) else None
        if not isinstance(rel_raw, dict):
            rel_raw = dict(item)
            for k in ("annotationId", "obj_id", "author_id", "author_name", "rel", "object"):
                rel_raw.pop(k, None)
        rel = dict(rel_raw)

        obj_id = str(item.get("obj_id") or rel.get("obj_id") or item.get("annotationId") or _new_id())
        author_id = str(item.get("author_id") or rel.get("author_id") or "unknown")
        author_name = str(item.get("author_name") or rel.get("author_name") or "user")
        annotation_id = str(item.get("annotationId") or obj_id)

        rel["obj_id"] = obj_id
        rel["author_id"] = author_id
        rel["author_name"] = author_name
        normalized_objects.append(
            {
                "annotationId": annotation_id,
                "obj_id": obj_id,
                "author_id": author_id,
                "author_name": author_name,
                "rel": rel,
            }
        )
    normalized = {"v": 2, "type": "lite_canvas_relative", "objects": normalized_objects}
    if "background" in state:
        normalized["background"] = state["background"]
    return normalized


def _runtime_to_lite_canvas_state(canvas_json: dict[str, Any], user: Optional[UserContext] = None) -> dict[str, Any]:
    runtime = _normalize_runtime_canvas(canvas_json, user)
    objects = runtime.get("objects", [])
    lite_objects = []
    for raw in objects:
        if not isinstance(raw, dict):
            continue
        obj = dict(raw)
        obj_id = str(obj.get("obj_id") or _new_id())
        author_id = str(obj.get("author_id") or (user.user_id if user else "unknown"))
        author_name = str(obj.get("author_name") or (user.username if user else "user"))
        obj["obj_id"] = obj_id
        obj["author_id"] = author_id
        obj["author_name"] = author_name
        lite_objects.append(
            {
                "annotationId": obj_id,
                "obj_id": obj_id,
                "author_id": author_id,
                "author_name": author_name,
                "rel": obj,
            }
        )
    out = {"v": 2, "type": "lite_canvas_relative", "objects": lite_objects}
    if "background" in runtime:
        out["background"] = runtime["background"]
    return out


def _lite_canvas_state_to_runtime(state: Any) -> dict[str, Any]:
    lite = _normalize_lite_canvas_state(state)
    runtime_objects: list[dict[str, Any]] = []
    for item in lite.get("objects", []):
        if not isinstance(item, dict):
            continue
        rel = item.get("rel")
        if not isinstance(rel, dict):
            continue
        obj = dict(rel)
        obj_id = str(item.get("obj_id") or item.get("annotationId") or obj.get("obj_id") or _new_id())
        obj["obj_id"] = obj_id
        if not obj.get("author_id"):
            obj["author_id"] = item.get("author_id") or "unknown"
        if not obj.get("author_name"):
            obj["author_name"] = item.get("author_name") or "user"
        runtime_objects.append(obj)
    runtime = {"version": "6.0.0", "objects": runtime_objects}
    runtime["background"] = lite.get("background", "#ffffff")
    return runtime


def _normalize_storage_state(value: Any) -> dict[str, Any]:
    if isinstance(value, dict) and value.get("type") == "multi_surface" and isinstance(value.get("surfaces"), dict):
        surfaces: dict[str, dict[str, Any]] = {}
        for surface_id, surface_state in value["surfaces"].items():
            if not isinstance(surface_id, str) or not surface_id:
                continue
            surfaces[surface_id] = _normalize_lite_canvas_state(surface_state)
        if not surfaces:
            surfaces[DEFAULT_SURFACE_ID] = _normalize_lite_canvas_state({})
        return {"v": 1, "type": "multi_surface", "surfaces": surfaces}

    if isinstance(value, dict) and value.get("type") == "lite_canvas_relative":
        return {
            "v": 1,
            "type": "multi_surface",
            "surfaces": {DEFAULT_SURFACE_ID: _normalize_lite_canvas_state(value)},
        }

    if isinstance(value, dict) and isinstance(value.get("objects"), list):
        return {
            "v": 1,
            "type": "multi_surface",
            "surfaces": {DEFAULT_SURFACE_ID: _runtime_to_lite_canvas_state(value)},
        }

    return {
        "v": 1,
        "type": "multi_surface",
        "surfaces": {DEFAULT_SURFACE_ID: _runtime_to_lite_canvas_state(default_canvas_state())},
    }


def _storage_to_runtime_canvas(storage: Any, surface_id: str = DEFAULT_SURFACE_ID) -> dict[str, Any]:
    normalized = _normalize_storage_state(storage)
    surfaces = normalized.get("surfaces", {})
    surface = surfaces.get(surface_id)
    if not isinstance(surface, dict):
        surface = next((v for v in surfaces.values() if isinstance(v, dict)), {"v": 2, "type": "lite_canvas_relative", "objects": []})
    return _lite_canvas_state_to_runtime(surface)


def create_board_if_missing(conn: sqlite3.Connection, board_id: str, owner_id: str) -> sqlite3.Row:
    row = board_row(conn, board_id)
    if row:
        return row

    ts = now_iso()
    conn.execute(
        "INSERT INTO boards (board_id, canvas_json, owner_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
        (board_id, json.dumps(default_canvas_state(), ensure_ascii=False), owner_id, ts, ts),
    )
    conn.execute(
        "INSERT OR IGNORE INTO board_members (board_id, user_id, role, created_at) VALUES (?, ?, 'owner', ?)",
        (board_id, owner_id, ts),
    )
    conn.commit()
    return ensure_board_exists(conn, board_id)


def _target_board_role(user: UserContext) -> str:
    return "owner" if user.role == "moderator" else "editor"


def ensure_user_board_access(conn: sqlite3.Connection, board_id: str, user: UserContext) -> str:
    row = board_row(conn, board_id)
    ts = now_iso()
    target_role = _target_board_role(user)

    if not row:
        owner_id = user.user_id if target_role == "owner" else PENDING_OWNER_ID
        conn.execute(
            "INSERT INTO boards (board_id, canvas_json, owner_id, created_at, updated_at) VALUES (?, ?, ?, ?, ?)",
            (board_id, json.dumps(default_canvas_state(), ensure_ascii=False), owner_id, ts, ts),
        )
        conn.execute(
            """
            INSERT INTO board_members (board_id, user_id, role, created_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(board_id, user_id)
            DO UPDATE SET role = excluded.role
            """,
            (board_id, user.user_id, target_role, ts),
        )
        conn.commit()
        return target_role

    if target_role == "owner":
        previous_owner_id = str(row["owner_id"] or "")
        if previous_owner_id and previous_owner_id not in {user.user_id, PENDING_OWNER_ID}:
            conn.execute(
                """
                INSERT INTO board_members (board_id, user_id, role, created_at)
                VALUES (?, ?, 'editor', ?)
                ON CONFLICT(board_id, user_id)
                DO UPDATE SET role = 'editor'
                """,
                (board_id, previous_owner_id, ts),
            )
        conn.execute(
            "UPDATE board_members SET role = 'editor' WHERE board_id = ? AND user_id != ? AND role = 'owner'",
            (board_id, user.user_id),
        )
        conn.execute(
            "UPDATE boards SET owner_id = ?, updated_at = ? WHERE board_id = ?",
            (user.user_id, ts, board_id),
        )
        conn.execute(
            """
            INSERT INTO board_members (board_id, user_id, role, created_at)
            VALUES (?, ?, 'owner', ?)
            ON CONFLICT(board_id, user_id)
            DO UPDATE SET role = 'owner'
            """,
            (board_id, user.user_id, ts),
        )
        conn.commit()
        return "owner"

    # Only provision a default 'editor' row the first time this user touches the
    # board. If a membership row already exists, leave it untouched: an owner
    # may have explicitly set it to 'viewer' via /api/board/{id}/members, and
    # that choice must not be silently overwritten on the user's next page
    # load/reconnect.
    conn.execute(
        """
        INSERT INTO board_members (board_id, user_id, role, created_at)
        VALUES (?, ?, 'editor', ?)
        ON CONFLICT(board_id, user_id) DO NOTHING
        """,
        (board_id, user.user_id, ts),
    )
    conn.commit()
    return get_user_role(conn, board_id, user.user_id) or "editor"


@app.get("/health")
async def health() -> dict[str, Any]:
    redis_ok = True
    try:
        await redis_client.ping()
    except Exception:
        redis_ok = False
    return {"status": "ok" if redis_ok else "degraded", "redis": redis_ok}


@app.get("/board/{board_id}", response_class=HTMLResponse)
def board_page(request: Request, board_id: str, user: UserContext = Depends(authenticate_request)):
    conn = get_db()
    try:
        ensure_user_board_access(conn, board_id, user)
        ensure_board_exists(conn, board_id)
        require_role(conn, board_id, user.user_id, "viewer")
    finally:
        conn.close()

    token = request.query_params.get("token") or parse_bearer(request.headers.get("Authorization"))
    ws_tokens = issue_ws_tokens(user, board_id)
    return templates.TemplateResponse(
        request,
        "board.html",
        {
            "board_id": board_id,
            "token": token or "",
            "ws_token": ws_tokens["ws_token"],
            "ws_token_exp": ws_tokens["ws_token_exp"],
            "ws_refresh_token": ws_tokens["ws_refresh_token"],
            "ws_refresh_token_exp": ws_tokens["ws_refresh_token_exp"],
            "username": user.username,
            "user_role": user.role,
        },
    )


@app.get("/api/board/{board_id}/ws-token")
def get_ws_tokens(board_id: str, user: UserContext = Depends(authenticate_request)):
    conn = get_db()
    try:
        ensure_user_board_access(conn, board_id, user)
        ensure_board_exists(conn, board_id)
        require_role(conn, board_id, user.user_id, "viewer")
    finally:
        conn.close()
    return issue_ws_tokens(user, board_id)


@app.get("/api/board/{board_id}")
def get_board_state(board_id: str, user: UserContext = Depends(authenticate_request)):
    conn = get_db()
    try:
        ensure_user_board_access(conn, board_id, user)
        row = ensure_board_exists(conn, board_id)
        role = require_role(conn, board_id, user.user_id, "viewer")
        return {
            "board_id": row["board_id"],
            "canvas_json": _read_board_canvas(conn, board_id),
            "owner_id": row["owner_id"],
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
            "role": role,
            "allow_students_draw": bool(row["allow_students_draw"]),
        }
    finally:
        conn.close()


@app.post("/api/board/{board_id}")
def save_board_state(board_id: str, body: SaveBoardRequest, user: UserContext = Depends(authenticate_request)):
    conn = get_db()
    try:
        ensure_user_board_access(conn, board_id, user)
        ensure_board_exists(conn, board_id)
        require_role(conn, board_id, user.user_id, "editor")
        next_canvas = _normalize_canvas(body.canvas_json, user)
        try:
            _ensure_board_size_limit(next_canvas)
        except ValueError as exc:
            raise HTTPException(status_code=413, detail=str(exc)) from exc
        updated_at = _replace_board_canvas_baseline(conn, board_id, next_canvas)
        conn.commit()
        return {"ok": True, "board_id": board_id, "updated_at": updated_at}
    finally:
        conn.close()


def _process_and_store_image(board_id: str, raw: bytes) -> tuple[str, int, int]:
    """Sync helper kept for the Miro-import path (see below), which processes
    a batch of images synchronously as part of one already-backgrounded admin
    operation. Interactive single-image uploads instead go through Celery -
    see upload_image/upload_image_status."""
    return process_and_store_image(UPLOAD_DIR, board_id, raw)


@app.post("/api/board/{board_id}/upload-image", status_code=202)
async def upload_image(
    board_id: str, file: UploadFile = File(...), user: UserContext = Depends(authenticate_request)
):
    """
    Compressing/re-encoding an image is CPU-bound and, for large phone photos,
    can take noticeably longer than users will tolerate blocking on - rather
    than tie up a request (and an event-loop thread via asyncio.to_thread)
    for it, stage the raw upload to disk and hand it off to a Celery worker
    immediately. The frontend polls upload_image_status with the returned
    job_id and shows a loading placeholder on the canvas until it resolves.
    """
    conn = get_db()
    try:
        ensure_user_board_access(conn, board_id, user)
        ensure_board_exists(conn, board_id)
        require_role(conn, board_id, user.user_id, "editor")
    finally:
        conn.close()

    raw = await file.read()
    if not raw:
        raise HTTPException(status_code=400, detail="Empty file")
    if len(raw) > MAX_UPLOAD_IMAGE_BYTES:
        raise HTTPException(status_code=413, detail="Image exceeds upload size limit")

    pending_path = PENDING_UPLOAD_DIR / f"{uuid.uuid4().hex}.bin"
    await asyncio.to_thread(pending_path.write_bytes, raw)

    task = process_image_task.delay(board_id, str(pending_path))
    return {"job_id": task.id, "status": "processing"}


@app.get("/api/board/{board_id}/upload-image/{job_id}")
async def upload_image_status(
    board_id: str, job_id: str, user: UserContext = Depends(authenticate_request)
):
    conn = get_db()
    try:
        ensure_user_board_access(conn, board_id, user)
    finally:
        conn.close()

    result = celery_app.AsyncResult(job_id)
    if result.state == "SUCCESS":
        payload = result.result or {}
        return {"status": "done", **payload}
    if result.state == "FAILURE":
        return {"status": "error", "detail": str(result.result or "Image processing failed")}
    return {"status": "processing"}


# --- Miro import -----------------------------------------------------------
#
# Maps Miro REST API v2 board items (https://developers.miro.com/reference/get-items)
# onto this app's Fabric.js object model, so an imported board can be applied
# through the same op-based sync path as a live edit. Field names below match
# Miro's documented v2 item schema (data/style/position/geometry); since this
# couldn't be verified against a real Miro board/token in this environment,
# field lookups are defensive (multiple fallbacks, per-item try/except) so one
# unexpected shape doesn't abort the whole import - treat this as a first cut
# that should be checked against a real Miro board before relying on it.

MIRO_SHAPE_TO_LOCAL = {
    "rectangle": "rect",
    "round_rectangle": "rect",
    "square": "rect",
    "circle": "ellipse",
    "ellipse": "ellipse",
    "triangle": "triangle",
    "rhombus": "diamond",
    "diamond": "diamond",
}


def _miro_html_to_plain_text(value: Optional[str]) -> str:
    """Miro item text content is a small HTML fragment (e.g. "<p>Hello</p>").
    This app's text/sticker objects are plain text, so strip tags."""
    if not isinstance(value, str) or not value:
        return ""
    text = re.sub(r"<br\s*/?>", "\n", value, flags=re.IGNORECASE)
    text = re.sub(r"</p>\s*<p[^>]*>", "\n", text, flags=re.IGNORECASE)
    text = re.sub(r"<[^>]+>", "", text)
    return html.unescape(text).strip()


_HEX_COLOR_RE = re.compile(r"^[0-9a-fA-F]{3,8}$")


def _miro_hex_color(value: Any, default: str) -> str:
    if isinstance(value, str) and value.strip():
        v = value.strip()
        if v.startswith("#") or not _HEX_COLOR_RE.match(v):
            # Already prefixed, or a CSS keyword/function Miro sent as-is
            # (e.g. "transparent", "none", "rgba(...)") - pass through rather
            # than mangling it into an invalid "#transparent".
            return v
        return f"#{v}"
    return default


def _miro_item_geometry(item: dict[str, Any]) -> tuple[float, float, float, float]:
    """Returns (left, top, width, height) in local canvas units. Miro positions
    are center-origin by default (position.origin == "center")."""
    position = item.get("position") or {}
    geometry = item.get("geometry") or {}
    width = float(geometry.get("width") or 160)
    height = float(geometry.get("height") or 100)
    cx = float(position.get("x") or 0)
    cy = float(position.get("y") or 0)
    origin = str(position.get("origin") or "center")
    if origin == "center":
        left = cx - width / 2
        top = cy - height / 2
    else:
        left = cx
        top = cy
    return left, top, width, height


def _diamond_points(width: float, height: float) -> list[dict[str, float]]:
    return [
        {"x": width / 2, "y": 0},
        {"x": width, "y": height / 2},
        {"x": width / 2, "y": height},
        {"x": 0, "y": height / 2},
    ]


def _build_sticker_object(obj_id: str, user: UserContext, left, top, width, height, color, text) -> dict[str, Any]:
    pad = 14.0
    # Fabric Group children are positioned relative to the GROUP'S CENTER,
    # not in absolute canvas coordinates, regardless of the group's own
    # originX/Y. Confirmed by inspecting how the client's own arrow-group
    # serializes: its Line/Polygon children end up with small
    # center-relative left/top values, not their original absolute points.
    # Getting this wrong doesn't error - it just silently renders the
    # children far outside the group's visible bounding box.
    rect = {
        "type": "Rect",
        "left": -width / 2,
        "top": -height / 2,
        "width": width,
        "height": height,
        "rx": 12,
        "ry": 12,
        "fill": color,
        "stroke": "rgba(15,23,42,0.14)",
        "strokeWidth": 1,
        "originX": "left",
        "originY": "top",
    }
    textbox = {
        "type": "Textbox",
        "left": -width / 2 + pad,
        "top": -height / 2 + pad,
        "width": max(10.0, width - pad * 2),
        "text": text or "",
        "fontSize": 18,
        "fontFamily": "Montserrat, sans-serif",
        "fontWeight": "500",
        "fill": "#1f2937",
        "originX": "left",
        "originY": "top",
    }
    return {
        "type": "Group",
        "obj_id": obj_id,
        "author_id": user.user_id,
        "author_name": user.username,
        "shapeKind": "sticker",
        "left": left,
        "top": top,
        "width": width,
        "height": height,
        "originX": "left",
        "originY": "top",
        "subTargetCheck": False,
        "lockUniScaling": True,
        "lockScalingFlip": True,
        "objects": [rect, textbox],
    }


def _map_miro_item_to_object(item: dict[str, Any], user: UserContext) -> Optional[dict[str, Any]]:
    try:
        item_type = str(item.get("type") or "")
        data = item.get("data") or {}
        style = item.get("style") or {}
        obj_id = uuid.uuid4().hex

        if item_type == "sticky_note":
            left, top, width, height = _miro_item_geometry(item)
            color = _miro_hex_color(style.get("fillColor"), STICKER_DEFAULT_FILL)
            text = _miro_html_to_plain_text(data.get("content"))
            return _build_sticker_object(obj_id, user, left, top, width, height, color, text)

        if item_type == "text":
            left, top, _width, _height = _miro_item_geometry(item)
            return {
                "type": "IText",
                "obj_id": obj_id,
                "author_id": user.user_id,
                "author_name": user.username,
                "left": left,
                "top": top,
                "text": _miro_html_to_plain_text(data.get("content")) or " ",
                "fill": _miro_hex_color(style.get("color"), "#1f2937"),
                "fontSize": float(style.get("fontSize") or 20),
                "fontFamily": "Montserrat, sans-serif",
                "fontWeight": "500",
            }

        if item_type == "shape":
            left, top, width, height = _miro_item_geometry(item)
            miro_shape = str(data.get("shape") or "rectangle").lower()
            local_shape = MIRO_SHAPE_TO_LOCAL.get(miro_shape, "rect")
            stroke = _miro_hex_color(style.get("borderColor"), "#1f2937")
            stroke_width = float(style.get("borderWidth") or 2)
            fill = _miro_hex_color(style.get("fillColor"), "transparent")
            common = {
                "obj_id": obj_id,
                "author_id": user.user_id,
                "author_name": user.username,
                "left": left,
                "top": top,
                "fill": fill,
                "stroke": stroke,
                "strokeWidth": stroke_width,
                "originX": "left",
                "originY": "top",
            }
            if local_shape == "ellipse":
                common.update({"type": "Ellipse", "rx": width / 2, "ry": height / 2})
            elif local_shape == "diamond":
                common.update({"type": "Polygon", "points": _diamond_points(width, height)})
            elif local_shape == "triangle":
                common.update({"type": "Triangle", "width": width, "height": height})
            else:
                common.update({"type": "Rect", "width": width, "height": height})
            # Note: Miro shapes can carry a text label (data.content), but this
            # app's shape objects don't support an embedded label - it's
            # dropped here rather than guessing at a lossy overlay-text object.
            return common

        if item_type == "image":
            left, top, width, height = _miro_item_geometry(item)
            url = data.get("url") or data.get("imageUrl") or data.get("resourceUrl")
            if not isinstance(url, str) or not url:
                return None
            return {
                "type": "Image",
                "obj_id": obj_id,
                "author_id": user.user_id,
                "author_name": user.username,
                "left": left,
                "top": top,
                "width": width,
                "height": height,
                "_miro_source_url": url,  # consumed before the object is stored, stripped after
            }

        # frame/card/app_card/document/embed: no equivalent object type yet in
        # this app - skip rather than guess at a lossy mapping.
        return None
    except Exception as exc:
        logger.warning("Skipping unmappable Miro item %s: %s", item.get("id"), exc)
        return None


async def _fetch_miro_items(miro_board_id: str, miro_token: str) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    cursor: Optional[str] = None
    headers = {"Authorization": f"Bearer {miro_token}"}
    async with httpx.AsyncClient(timeout=MIRO_IMPORT_HTTP_TIMEOUT) as client:
        while len(items) < MIRO_IMPORT_MAX_ITEMS:
            params: dict[str, Any] = {"limit": MIRO_IMPORT_PAGE_SIZE}
            if cursor:
                params["cursor"] = cursor
            resp = await client.get(f"{MIRO_API_BASE}/boards/{miro_board_id}/items", headers=headers, params=params)
            if resp.status_code == 401:
                raise HTTPException(status_code=401, detail="Invalid or expired Miro token")
            if resp.status_code == 404:
                raise HTTPException(status_code=404, detail="Miro board not found or not accessible with this token")
            if resp.status_code >= 400:
                raise HTTPException(status_code=502, detail=f"Miro API error: {resp.status_code}")
            payload = resp.json()
            page = payload.get("data") or []
            items.extend(page)
            cursor = payload.get("cursor")
            if not cursor or not page:
                break
    return items[:MIRO_IMPORT_MAX_ITEMS]


async def _download_and_store_miro_image(board_id: str, url: str, miro_token: str) -> Optional[tuple[str, int, int]]:
    try:
        async with httpx.AsyncClient(timeout=MIRO_IMPORT_HTTP_TIMEOUT) as client:
            # Miro asset URLs are typically pre-signed/public; the token is
            # attached defensively in case a given board requires it.
            resp = await client.get(url, headers={"Authorization": f"Bearer {miro_token}"})
            resp.raise_for_status()
            raw = resp.content
    except Exception as exc:
        logger.warning("Failed to download Miro image %s: %s", url, exc)
        return None
    if len(raw) > MAX_UPLOAD_IMAGE_BYTES:
        return None
    try:
        return await asyncio.to_thread(_process_and_store_image, board_id, raw)
    except ValueError:
        return None


@app.post("/api/board/{board_id}/import/miro")
async def import_from_miro(board_id: str, body: MiroImportRequest, user: UserContext = Depends(authenticate_request)):
    conn = get_db()
    try:
        ensure_user_board_access(conn, board_id, user)
        ensure_board_exists(conn, board_id)
        require_role(conn, board_id, user.user_id, "editor")
    finally:
        conn.close()

    miro_items = await _fetch_miro_items(body.miro_board_id, body.miro_token)

    mapped_objects: list[dict[str, Any]] = []
    skipped = 0
    for item in miro_items:
        obj = _map_miro_item_to_object(item, user)
        if obj is None:
            skipped += 1
            continue
        mapped_objects.append(obj)

    # Images need their bytes downloaded from Miro and re-hosted locally
    # (this app stores images as files under UPLOAD_DIR, not inline base64 -
    # see _process_and_store_image) before they can be added as ops.
    resolved_objects: list[dict[str, Any]] = []
    for obj in mapped_objects:
        source_url = obj.pop("_miro_source_url", None)
        if source_url:
            result = await _download_and_store_miro_image(board_id, source_url, body.miro_token)
            if not result:
                skipped += 1
                continue
            url, width, height = result
            obj["src"] = url
            obj["width"] = width
            obj["height"] = height
            obj["crossOrigin"] = "anonymous"
        resolved_objects.append(obj)

    if not resolved_objects:
        return {"ok": True, "imported": 0, "skipped": skipped, "total": len(miro_items)}

    ops = [{"type": "add", "object": obj} for obj in resolved_objects]

    try:
        applied_ops, _inverse_ops, updated_at, op_ids, should_compact = await board_task_manager.run(
            board_id, _db_apply_ops, board_id, ops, user
        )
    except ValueError as exc:
        raise HTTPException(status_code=413, detail=str(exc)) from exc

    if should_compact:
        trigger_compaction_debounced(board_id)

    if applied_ops:
        synthetic_session = {"client_id": f"miro-import:{user.user_id}", "user_id": user.user_id}
        wire_ops = _decorate_ops_for_wire(applied_ops, synthetic_session, op_ids)
        payload = {"board_id": board_id, "ops": wire_ops, "updated_at": updated_at, "author": user.user_id}
        await sio.emit("batch_update", payload, room=board_id)

    return {
        "ok": True,
        "imported": len(applied_ops),
        "skipped": skipped,
        "total": len(miro_items),
    }


@app.delete("/api/board/{board_id}")
def delete_board(board_id: str, user: UserContext = Depends(authenticate_request)):
    conn = get_db()
    try:
        row = ensure_board_exists(conn, board_id)
        if row["owner_id"] != user.user_id:
            raise HTTPException(status_code=403, detail="Only owner can delete board")
        conn.execute("DELETE FROM boards WHERE board_id = ?", (board_id,))
        conn.commit()
        return {"ok": True, "board_id": board_id}
    finally:
        conn.close()


@app.post("/api/board")
def create_board(body: CreateBoardRequest, user: UserContext = Depends(authenticate_request)):
    board_id = body.board_id or uuid.uuid4().hex[:12]
    conn = get_db()
    try:
        exists = board_row(conn, board_id)
        if exists:
            raise HTTPException(status_code=409, detail="Board already exists")
        role = ensure_user_board_access(conn, board_id, user)
        row = ensure_board_exists(conn, board_id)
        return {"board_id": board_id, "owner_id": row["owner_id"], "created_at": row["created_at"], "role": role}
    finally:
        conn.close()


@app.get("/api/my-boards")
def my_boards(user: UserContext = Depends(authenticate_request)):
    conn = get_db()
    try:
        rows = conn.execute(
            """
            SELECT b.board_id, b.owner_id, b.created_at, b.updated_at, b.allow_students_draw,
                   COALESCE(m.role, CASE WHEN b.owner_id = ? THEN 'owner' END) AS role
            FROM boards b
            LEFT JOIN board_members m
              ON b.board_id = m.board_id
             AND m.user_id = ?
            WHERE b.owner_id = ? OR m.user_id = ?
            ORDER BY b.updated_at DESC
            """,
            (user.user_id, user.user_id, user.user_id, user.user_id),
        ).fetchall()
        return {
            "items": [
                {
                    "board_id": r["board_id"],
                    "owner_id": r["owner_id"],
                    "created_at": r["created_at"],
                    "updated_at": r["updated_at"],
                    "role": r["role"],
                    "allow_students_draw": bool(r["allow_students_draw"]) if "allow_students_draw" in r.keys() else True,
                }
                for r in rows
            ]
        }
    finally:
        conn.close()


@app.post("/api/admin/board/{board_id}/drawing")
async def set_board_drawing_policy(
    board_id: str,
    body: DrawingPolicyRequest,
    _: None = Depends(authenticate_service_key),
):
    conn = get_db()
    try:
        ensure_board_exists(conn, board_id)
        updated_at = now_iso()
        conn.execute(
            "UPDATE boards SET allow_students_draw = ?, updated_at = ? WHERE board_id = ?",
            (1 if body.allow_students_draw else 0, updated_at, board_id),
        )
        conn.commit()
    finally:
        conn.close()

    await sio.emit(
        "board_policy",
        {"board_id": board_id, "allow_students_draw": body.allow_students_draw, "updated_at": updated_at},
        room=board_id,
    )
    return {"ok": True, "board_id": board_id, "allow_students_draw": body.allow_students_draw, "updated_at": updated_at}


@app.delete("/api/admin/board/{board_id}")
def admin_delete_board(board_id: str, _: None = Depends(authenticate_service_key)):
    conn = get_db()
    try:
        ensure_board_exists(conn, board_id)
        conn.execute("DELETE FROM boards WHERE board_id = ?", (board_id,))
        conn.commit()
        return {"ok": True, "board_id": board_id}
    finally:
        conn.close()


@app.post("/api/board/{board_id}/members")
async def upsert_member(board_id: str, body: MemberRequest, user: UserContext = Depends(authenticate_request)):
    conn = get_db()
    try:
        row = ensure_board_exists(conn, board_id)
        if row["owner_id"] != user.user_id:
            raise HTTPException(status_code=403, detail="Only owner can manage members")

        conn.execute(
            """
            INSERT INTO board_members (board_id, user_id, role, created_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(board_id, user_id)
            DO UPDATE SET role = excluded.role
            """,
            (board_id, body.user_id, body.role, now_iso()),
        )
        conn.commit()
    finally:
        conn.close()

    await _push_role_update(board_id, body.user_id, body.role)
    return {"ok": True}


@app.delete("/api/board/{board_id}/members/{member_id}")
async def remove_member(board_id: str, member_id: str, user: UserContext = Depends(authenticate_request)):
    conn = get_db()
    try:
        row = ensure_board_exists(conn, board_id)
        if row["owner_id"] != user.user_id:
            raise HTTPException(status_code=403, detail="Only owner can manage members")
        if member_id == row["owner_id"]:
            raise HTTPException(status_code=400, detail="Cannot remove owner")

        conn.execute(
            "DELETE FROM board_members WHERE board_id = ? AND user_id = ?",
            (board_id, member_id),
        )
        conn.commit()
    finally:
        conn.close()

    await _push_role_update(board_id, member_id, "viewer")
    return {"ok": True}


@app.post("/api/ws-token/refresh")
def refresh_ws_token(body: WsTokenRefreshRequest):
    payload = decode_jwt(body.refresh_token)
    if str(payload.get("type") or "") != "ws_refresh":
        raise HTTPException(status_code=401, detail="Invalid refresh token")
    token_board_id = str(payload.get("board_id") or "")
    if token_board_id != body.board_id:
        raise HTTPException(status_code=403, detail="Refresh token board mismatch")

    user = UserContext(
        user_id=str(payload["user_id"]),
        username=str(payload.get("username") or payload.get("name") or payload["user_id"]),
        role=str(payload.get("role") or "editor"),
        payload=payload,
    )

    conn = get_db()
    try:
        ensure_user_board_access(conn, body.board_id, user)
        ensure_board_exists(conn, body.board_id)
        require_role(conn, body.board_id, user.user_id, "viewer")
    finally:
        conn.close()

    return issue_ws_tokens(user, body.board_id)


def decode_ws_token(token: Optional[str], expected_board_id: Optional[str] = None) -> UserContext:
    if DEBUG and not token:
        return UserContext(
            user_id=DEBUG_USER_ID,
            username="Debug",
            role="moderator",
            payload={"user_id": DEBUG_USER_ID, "debug": True, "username": "Debug", "role": "moderator"},
        )
    if not token:
        raise HTTPException(status_code=401, detail="Missing token")
    payload = decode_jwt(token)
    token_type = str(payload.get("type") or "")
    if token_type == "ws_refresh":
        raise HTTPException(status_code=401, detail="Invalid websocket token type")
    token_board_id = str(payload.get("board_id") or "")
    if expected_board_id and token_board_id and token_board_id != expected_board_id:
        raise HTTPException(status_code=403, detail="Websocket token board mismatch")
    return UserContext(
        user_id=str(payload["user_id"]),
        username=str(payload.get("username") or payload.get("name") or payload["user_id"]),
        role=str(payload.get("role") or "editor"),
        payload=payload,
    )

def _objects_map(canvas_json: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for obj in canvas_json.get("objects", []):
        if isinstance(obj, dict):
            obj_id = obj.get("obj_id")
            if isinstance(obj_id, str):
                out[obj_id] = obj
    return out


def _normalize_canvas(canvas_json: dict[str, Any], user: UserContext) -> dict[str, Any]:
    return _normalize_runtime_canvas(canvas_json, user)


def _build_action(old_canvas: dict[str, Any], new_canvas: dict[str, Any]) -> Optional[dict[str, Any]]:
    old_map = _objects_map(old_canvas)
    new_map = _objects_map(new_canvas)
    old_ids = set(old_map.keys())
    new_ids = set(new_map.keys())

    added_ids = sorted(new_ids - old_ids)
    removed_ids = sorted(old_ids - new_ids)
    common_ids = sorted(old_ids & new_ids)

    modified_ids = []
    for obj_id in common_ids:
        if json.dumps(old_map[obj_id], sort_keys=True) != json.dumps(new_map[obj_id], sort_keys=True):
            modified_ids.append(obj_id)

    if not added_ids and not removed_ids and not modified_ids:
        return None

    return {
        "added_ids": added_ids,
        "removed_objects": [old_map[obj_id] for obj_id in removed_ids],
        "modified_before": [old_map[obj_id] for obj_id in modified_ids],
        "modified_after": [new_map[obj_id] for obj_id in modified_ids],
    }


def _replace_or_append(objects: list[dict[str, Any]], incoming: dict[str, Any]) -> None:
    obj_id = incoming.get("obj_id")
    if not obj_id:
        return
    for i, obj in enumerate(objects):
        if isinstance(obj, dict) and obj.get("obj_id") == obj_id:
            objects[i] = incoming
            return
    objects.append(incoming)


def _apply_undo_action(canvas_json: dict[str, Any], action: dict[str, Any]) -> dict[str, Any]:
    objects = [o for o in canvas_json.get("objects", []) if isinstance(o, dict)]
    added_ids = set(action.get("added_ids", []))
    objects = [o for o in objects if o.get("obj_id") not in added_ids]

    for obj in action.get("removed_objects", []):
        if isinstance(obj, dict):
            _replace_or_append(objects, obj)
    for obj in action.get("modified_before", []):
        if isinstance(obj, dict):
            _replace_or_append(objects, obj)

    canvas_json["objects"] = objects
    return canvas_json


def _apply_redo_action(canvas_json: dict[str, Any], action: dict[str, Any]) -> dict[str, Any]:
    objects = [o for o in canvas_json.get("objects", []) if isinstance(o, dict)]

    for obj_id in [o.get("obj_id") for o in action.get("removed_objects", []) if isinstance(o, dict)]:
        objects = [o for o in objects if o.get("obj_id") != obj_id]

    for obj in action.get("modified_after", []):
        if isinstance(obj, dict):
            _replace_or_append(objects, obj)

    canvas_json["objects"] = objects
    return canvas_json


def _extract_added_objects(action: dict[str, Any], new_canvas: dict[str, Any]) -> None:
    added_ids = set(action.get("added_ids", []))
    new_map = _objects_map(new_canvas)
    action["added_objects"] = [new_map[obj_id] for obj_id in added_ids if obj_id in new_map]


def _apply_redo_action_with_added(canvas_json: dict[str, Any], action: dict[str, Any]) -> dict[str, Any]:
    canvas_json = _apply_redo_action(canvas_json, action)
    objects = [o for o in canvas_json.get("objects", []) if isinstance(o, dict)]
    for obj in action.get("added_objects", []):
        if isinstance(obj, dict):
            _replace_or_append(objects, obj)
    canvas_json["objects"] = objects
    return canvas_json


def _find_object_index(objects: list[dict[str, Any]], obj_id: str) -> int:
    for i, obj in enumerate(objects):
        if isinstance(obj, dict) and obj.get("obj_id") == obj_id:
            return i
    return -1


def _apply_prepared_ops(canvas_json: dict[str, Any], ops: list[dict[str, Any]], user: UserContext) -> dict[str, Any]:
    objects = [o for o in canvas_json.get("objects", []) if isinstance(o, dict)]
    for op in ops:
        if not isinstance(op, dict):
            continue
        op_type = str(op.get("type") or op.get("op") or "").lower()
        if op_type in {"add", "update"}:
            obj = op.get("object")
            if not isinstance(obj, dict):
                continue
            if not obj.get("obj_id"):
                obj["obj_id"] = uuid.uuid4().hex
            if not obj.get("author_id"):
                obj["author_id"] = user.user_id
            if not obj.get("author_name"):
                obj["author_name"] = user.username
            idx = _find_object_index(objects, str(obj["obj_id"]))
            if idx >= 0:
                objects[idx] = obj
            else:
                objects.append(obj)
        elif op_type == "remove":
            obj_id = op.get("obj_id") or op.get("object_id")
            if not isinstance(obj_id, str) or not obj_id:
                continue
            idx = _find_object_index(objects, obj_id)
            if idx >= 0:
                objects.pop(idx)
    canvas_json["objects"] = objects
    return canvas_json


def _apply_ops_build_inverse(
    canvas_json: dict[str, Any],
    incoming_ops: list[dict[str, Any]],
    user: UserContext,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    """
    Apply incoming ops and build both normalized applied ops and inverse ops for undo.
    """
    objects = [o for o in canvas_json.get("objects", []) if isinstance(o, dict)]
    applied_ops: list[dict[str, Any]] = []
    inverse_ops: list[dict[str, Any]] = []

    for raw in incoming_ops:
        if not isinstance(raw, dict):
            continue
        op_type = str(raw.get("type") or raw.get("op") or "").lower()
        if op_type not in {"add", "update", "remove"}:
            continue

        if op_type in {"add", "update"}:
            obj = raw.get("object")
            if not isinstance(obj, dict):
                continue
            if not obj.get("obj_id"):
                obj["obj_id"] = uuid.uuid4().hex
            if not obj.get("author_id"):
                obj["author_id"] = user.user_id
            if not obj.get("author_name"):
                obj["author_name"] = user.username
            obj_id = str(obj["obj_id"])

            idx = _find_object_index(objects, obj_id)
            prev = objects[idx] if idx >= 0 else None
            if idx >= 0:
                objects[idx] = obj
            else:
                objects.append(obj)

            applied_ops.append({"type": "add" if idx < 0 else "update", "object": obj})
            if prev is None:
                inverse_ops.append({"type": "remove", "obj_id": obj_id})
            else:
                inverse_ops.append({"type": "update", "object": prev})
            continue

        if op_type == "remove":
            obj_id = raw.get("obj_id") or raw.get("object_id")
            if not isinstance(obj_id, str) or not obj_id:
                continue
            idx = _find_object_index(objects, obj_id)
            if idx < 0:
                continue
            prev = objects.pop(idx)
            applied_ops.append({"type": "remove", "obj_id": obj_id})
            inverse_ops.append({"type": "add", "object": prev})

    canvas_json["objects"] = objects
    inverse_ops.reverse()
    return canvas_json, applied_ops, inverse_ops


def _decorate_ops_for_wire(ops: list[dict[str, Any]], session: dict[str, Any], op_ids: Optional[list[int]] = None) -> list[dict[str, Any]]:
    now_ms = int(time.time() * 1000)
    client_id = str(session.get("client_id") or "")
    decorated: list[dict[str, Any]] = []
    for index, op in enumerate(ops):
        if not isinstance(op, dict):
            continue
        op_name = str(op.get("type") or op.get("op") or "").lower()
        if op_name not in {"add", "update", "remove"}:
            continue
        seq_num = op_ids[index] if (op_ids and index < len(op_ids)) else (now_ms + index + 1)
        payload: dict[str, Any] = {
            "v": 1,
            "op": op_name,
            "client_id": client_id,
            "seq": seq_num,
            "ts": now_ms,
            "action_id": _new_id(),
            "surface_id": DEFAULT_SURFACE_ID,
        }
        if op_name in {"add", "update"} and isinstance(op.get("object"), dict):
            payload["object"] = op["object"]
        obj_id = op.get("obj_id") or op.get("object_id")
        if isinstance(obj_id, str) and obj_id:
            payload["obj_id"] = obj_id
            payload["object_id"] = obj_id
        decorated.append(payload)
    return decorated


class SocketBoardManager:
    """
    Presence and undo/redo history, backed by Redis instead of process memory.
    This is what makes both correct once more than one worker/instance is
    running: previously both lived in a plain dict on `self`, so a user whose
    reconnect landed on a different worker would see an empty/wrong undo stack
    and stale online counts.
    """

    def _presence_key(self, board_id: str) -> str:
        return f"wb:presence:{board_id}"

    def _history_key(self, board_id: str) -> str:
        return f"wb:history:{board_id}"

    async def add(self, board_id: str, sid: str, user_id: str, username: str, role: str, client_id: str) -> None:
        info = {"user_id": user_id, "username": username, "role": role, "client_id": client_id}
        try:
            await redis_client.hset(self._presence_key(board_id), sid, json.dumps(info, ensure_ascii=False))
        except Exception:
            logger.exception("presence add failed for board %s", board_id)

    async def remove(self, board_id: str, sid: str) -> Optional[dict[str, str]]:
        key = self._presence_key(board_id)
        try:
            raw = await redis_client.hget(key, sid)
            await redis_client.hdel(key, sid)
        except Exception:
            logger.exception("presence remove failed for board %s", board_id)
            return None
        return json.loads(raw) if raw else None

    async def online_count(self, board_id: str) -> int:
        try:
            return int(await redis_client.hlen(self._presence_key(board_id)))
        except Exception:
            logger.exception("presence count failed for board %s", board_id)
            return 0

    async def get_online(self, board_id: str) -> dict[str, dict[str, str]]:
        try:
            raw = await redis_client.hgetall(self._presence_key(board_id))
        except Exception:
            logger.exception("presence list failed for board %s", board_id)
            return {}
        return {sid: json.loads(v) for sid, v in raw.items()}

    async def get_user_history(self, board_id: str, user_id: str) -> dict[str, list[dict[str, Any]]]:
        try:
            raw = await redis_client.hget(self._history_key(board_id), user_id)
        except Exception:
            logger.exception("history read failed for board %s", board_id)
            raw = None
        if not raw:
            return {"undo": [], "redo": []}
        try:
            data = json.loads(raw)
        except Exception:
            return {"undo": [], "redo": []}
        return {"undo": data.get("undo") or [], "redo": data.get("redo") or []}

    async def save_user_history(self, board_id: str, user_id: str, hist: dict[str, list[dict[str, Any]]]) -> None:
        try:
            await redis_client.hset(self._history_key(board_id), user_id, json.dumps(hist, ensure_ascii=False))
        except Exception:
            logger.exception("history save failed for board %s", board_id)

    async def clear_history(self, board_id: str) -> None:
        try:
            await redis_client.delete(self._history_key(board_id))
        except Exception:
            logger.exception("history clear failed for board %s", board_id)


board_manager = SocketBoardManager()
sio = socketio.AsyncServer(
    async_mode="asgi",
    cors_allowed_origins=CORS_ORIGINS,
    logger=False,
    engineio_logger=False,
    max_http_buffer_size=SOCKET_MAX_BUFFER_BYTES,
    client_manager=socketio.AsyncRedisManager(REDIS_URL),
)


async def _check_socket_rate_limit(sid: str, event: str, limit: int = RATE_LIMIT_SOCKET_PER_10S) -> bool:
    """Returns True if sid is within budget for `event`, False if it should be dropped."""
    window = int(time.time() // 10)
    key = f"wb:ratelimit:ws:{event}:{sid}:{window}"
    try:
        count = await redis_client.incr(key)
        if count == 1:
            await redis_client.expire(key, 10)
    except Exception:
        return True  # Redis unavailable: fail open.
    return count <= limit


async def _push_role_update(board_id: str, user_id: str, new_role: str) -> None:
    """
    Update the cached board_role on every live socket session for this user and
    notify their client, so a permission grant/revoke takes effect immediately
    instead of only after the user reconnects (board_role was previously read
    once at connect() and never refreshed).
    """
    room = await board_manager.get_online(board_id)
    target_sids = [sid for sid, info in room.items() if info.get("user_id") == user_id]
    for sid in target_sids:
        try:
            session = await sio.get_session(sid)
        except Exception:
            continue
        session["board_role"] = new_role
        await sio.save_session(sid, session)
        await sio.emit("role_update", {"board_id": board_id, "role": new_role}, to=sid)


def _read_board_canvas(conn: sqlite3.Connection, board_id: str) -> dict[str, Any]:
    row = ensure_board_exists(conn, board_id)
    try:
        raw = json.loads(row["canvas_json"])
    except json.JSONDecodeError:
        raw = default_canvas_state()
    if isinstance(raw, dict) and raw.get("type") in {"multi_surface", "lite_canvas_relative"}:
        raw = _storage_to_runtime_canvas(raw)
    canvas = _normalize_runtime_canvas(raw)
    rows = conn.execute("SELECT op_json FROM board_ops WHERE board_id = ? ORDER BY id ASC", (board_id,)).fetchall()
    if not rows:
        return canvas
    ops: list[dict[str, Any]] = []
    for r in rows:
        try:
            op = json.loads(r["op_json"])
        except Exception:
            op = None
        if isinstance(op, dict):
            ops.append(op)
    if not ops:
        return canvas
    try:
        return _apply_prepared_ops(canvas, ops, UserContext("system", "system", "system", {}))
    except Exception as exc:
        # Keep board available even if some persisted op chain is broken.
        logger.exception("Board %s failed to replay ops, using baseline canvas: %s", board_id, exc)
        return canvas


def _replace_board_canvas_baseline(conn: sqlite3.Connection, board_id: str, canvas_json: dict[str, Any]) -> str:
    _ensure_board_size_limit(canvas_json)
    updated_at = now_iso()
    conn.execute(
        "UPDATE boards SET canvas_json = ?, updated_at = ? WHERE board_id = ?",
        (json.dumps(canvas_json, ensure_ascii=False), updated_at, board_id),
    )
    conn.execute("DELETE FROM board_ops WHERE board_id = ?", (board_id,))
    return updated_at


def _append_board_ops(conn: sqlite3.Connection, board_id: str, ops: list[dict[str, Any]]) -> str:
    if not ops:
        updated_at = now_iso()
        conn.execute("UPDATE boards SET updated_at = ? WHERE board_id = ?", (updated_at, board_id))
        return updated_at
    ts = now_iso()
    conn.executemany(
        "INSERT INTO board_ops (board_id, op_json, created_at) VALUES (?, ?, ?)",
        [(board_id, json.dumps(op, ensure_ascii=False), ts) for op in ops],
    )
    compact_stats = conn.execute(
        "SELECT COUNT(*) AS cnt, COALESCE(SUM(LENGTH(op_json)), 0) AS bytes FROM board_ops WHERE board_id = ?",
        (board_id,),
    ).fetchone()
    op_count = int(compact_stats["cnt"]) if compact_stats else 0
    op_bytes = int(compact_stats["bytes"]) if compact_stats else 0
    if op_count >= OPS_COMPACT_COUNT_THRESHOLD or op_bytes >= OPS_COMPACT_BYTES_THRESHOLD:
        try:
            compacted_canvas = _read_board_canvas(conn, board_id)
            _ensure_board_size_limit(compacted_canvas)
            conn.execute(
                "UPDATE boards SET canvas_json = ? WHERE board_id = ?",
                (json.dumps(compacted_canvas, ensure_ascii=False), board_id),
            )
            conn.execute("DELETE FROM board_ops WHERE board_id = ?", (board_id,))
        except Exception as exc:
            # Do not break user operations if compaction fails.
            logger.exception("Board %s op compaction skipped: %s", board_id, exc)
    updated_at = now_iso()
    conn.execute("UPDATE boards SET updated_at = ? WHERE board_id = ?", (updated_at, board_id))
    return updated_at


def _resolve_handshake(environ: dict[str, Any], auth: Any) -> tuple[str, Optional[str], str]:
    query = parse_qs(environ.get("QUERY_STRING", ""))
    board_id = ""
    token = query.get("token", [None])[0]
    history_id = str(query.get("history_id", [""])[0] or "")
    if isinstance(auth, dict):
        board_id = str(auth.get("board_id") or "")
        token = str(auth.get("token") or token or "") or None
        history_id = str(auth.get("history_id") or history_id or "")
    if not board_id:
        board_id = str(query.get("board_id", [""])[0] or "")
    return board_id, token, history_id


def _history_actor_key(session: dict[str, Any]) -> str:
    """
    In DEBUG mode without JWT, multiple real users often share the same debug user_id.
    To keep undo/redo isolated per client in that case, key history by client_id.
    """
    if session.get("debug_no_token"):
        history_id = str(session.get("history_id") or "")
        if history_id:
            return f"debug-history:{history_id}"
        return f"debug-client:{session.get('client_id', '')}"
    return str(session.get("user_id", ""))


def _can_edit(session: dict[str, Any]) -> bool:
    if DEBUG:
        return True
    if str(session.get("jwt_role") or "") == "moderator":
        return True
    board_id = str(session.get("board_id") or "")
    if not board_id:
        return False

    conn = get_db()
    try:
        allow_students_draw = board_allows_students_draw(conn, board_id)
    finally:
        conn.close()

    if not allow_students_draw:
        return False
    return ROLE_RANK.get(session.get("board_role", "viewer"), 0) >= ROLE_RANK["editor"]


@sio.event
async def connect(sid: str, environ: dict[str, Any], auth: Any):
    board_id, token, history_id = _resolve_handshake(environ, auth)
    if not board_id:
        raise ConnectionRefusedError("Missing board_id")

    try:
        user = decode_ws_token(token, expected_board_id=board_id)
    except HTTPException as exc:
        raise ConnectionRefusedError(exc.detail)

    last_seen_seq = 0
    if isinstance(auth, dict) and "last_seen_seq" in auth:
        try:
            last_seen_seq = int(auth["last_seen_seq"])
        except (ValueError, TypeError):
            pass

    try:
        board_role, allow_students_draw, canvas_json, last_compacted, max_op_id = await board_task_manager.run(
            board_id, _load_connect_data, board_id, user
        )
    except HTTPException as exc:
        raise ConnectionRefusedError(exc.detail)
    except Exception as exc:
        raise ConnectionRefusedError(str(exc))

    client_id = uuid.uuid4().hex[:12]
    await sio.save_session(
        sid,
        {
            "board_id": board_id,
            "user_id": user.user_id,
            "username": user.username,
            "jwt_role": user.role,
            "board_role": board_role,
            "allow_students_draw": allow_students_draw,
            "client_id": client_id,
            "debug_no_token": bool(DEBUG and not token),
            "history_id": history_id,
        },
    )

    await sio.enter_room(sid, board_id)
    await board_manager.add(board_id, sid, user.user_id, user.username, user.role, client_id)

    status = "init"
    missed_ops = []

    if last_seen_seq > 0 and last_seen_seq >= last_compacted:
        status = "sync"
        canvas_json = None
        try:
            missed_ops = await board_task_manager.run(board_id, _get_missed_ops, board_id, last_seen_seq)
        except Exception:
            pass

    await sio.emit(
        "init",
        {
            "board_id": board_id,
            "status": status,
            "canvas_json": canvas_json,
            "last_seq_id": max_op_id,
            "role": board_role,
            "client_id": client_id,
            "username": user.username,
            "user_id": user.user_id,
            "jwt_role": user.role,
            "can_clear": user.role == "moderator",
            "allow_students_draw": allow_students_draw,
            "debug_force_edit": bool(DEBUG),
            "can_edit": (user.role == "moderator") or (
                allow_students_draw and ROLE_RANK.get(board_role, 0) >= ROLE_RANK["editor"]
            ),
            "online": await board_manager.online_count(board_id),
        },
        to=sid,
    )

    if status == "sync" and missed_ops:
        session = await sio.get_session(sid)
        wire_ops = _decorate_ops_for_wire(missed_ops, session, [op["seq"] for op in missed_ops])
        payload = {
            "board_id": board_id,
            "ops": wire_ops,
            "updated_at": now_iso(),
            "author": "system"
        }
        await sio.emit("batch_update", payload, to=sid)

    await sio.emit("presence", {"online": await board_manager.online_count(board_id)}, room=board_id)


@sio.event
async def disconnect(sid: str):
    session = await sio.get_session(sid)
    if not session:
        return
    board_id = session["board_id"]
    client_id = session["client_id"]
    await board_manager.remove(board_id, sid)
    await sio.emit("cursor_remove", {"client_id": client_id}, room=board_id)
    await sio.emit("presence", {"online": await board_manager.online_count(board_id)}, room=board_id)


@sio.event
async def cursor(sid: str, data: Any):
    session = await sio.get_session(sid)
    if not session or not isinstance(data, dict):
        return
    # Cursor moves are frequent by design (client throttles to ~20/s); give them
    # a much higher budget than board-mutating events.
    if not await _check_socket_rate_limit(sid, "cursor", limit=RATE_LIMIT_SOCKET_PER_10S * 5):
        return

    x = data.get("x")
    y = data.get("y")
    if not isinstance(x, (int, float)) or not isinstance(y, (int, float)):
        return

    await sio.emit(
        "cursor",
        {
            "client_id": session["client_id"],
            "username": session["username"],
            "x": float(x),
            "y": float(y),
            "ts": int(time.time() * 1000),
        },
        room=session["board_id"],
        skip_sid=sid,
    )


@sio.event
async def update(sid: str, data: Any):
    session = await sio.get_session(sid)
    if not session or not _can_edit(session):
        await sio.emit("error_msg", {"message": "Read-only access"}, to=sid)
        return
    if not await _check_socket_rate_limit(sid, "update"):
        await sio.emit("error_msg", {"message": "Too many updates, slow down"}, to=sid)
        return
    if not isinstance(data, dict) or not isinstance(data.get("canvas_json"), dict):
        await sio.emit("error_msg", {"message": "canvas_json must be object"}, to=sid)
        return

    board_id = session["board_id"]
    user = UserContext(
        user_id=session["user_id"],
        username=session["username"],
        role=session["jwt_role"],
        payload={"user_id": session["user_id"]},
    )
    new_canvas = _normalize_canvas(data["canvas_json"], user)

    try:
        applied_ops, updated_at, op_ids, should_compact, action = await board_task_manager.run(
            board_id, _db_update, board_id, new_canvas, user
        )
    except ValueError:
        await sio.emit("error_msg", {"message": f"Board size exceeds {MAX_BOARD_BYTES // (1024 * 1024)} MB"}, to=sid)
        canvas_json, max_op_id = await board_task_manager.run(board_id, _db_read_canvas_and_max_id, board_id)
        await sio.emit("update", {
            "board_id": board_id,
            "canvas_json": canvas_json,
            "updated_at": now_iso(),
            "author": "system",
            "last_seq_id": max_op_id
        }, to=sid)
        return
    except Exception as exc:
        await sio.emit("error_msg", {"message": f"Database error: {str(exc)}"}, to=sid)
        return

    if should_compact:
        trigger_compaction_debounced(board_id)

    actor_key = _history_actor_key(session)
    user_hist = await board_manager.get_user_history(board_id, actor_key)
    if action:
        _extract_added_objects(action, new_canvas)
        user_hist["undo"].append(action)
        if len(user_hist["undo"]) > 100:
            user_hist["undo"] = user_hist["undo"][-100:]
        user_hist["redo"].clear()
        await board_manager.save_user_history(board_id, actor_key, user_hist)

    if applied_ops:
        wire_ops = _decorate_ops_for_wire(applied_ops, session, op_ids)
        payload = {
            "board_id": board_id,
            "ops": wire_ops,
            "updated_at": updated_at,
            "author": session["user_id"],
            "undo_available": bool(user_hist["undo"]),
            "redo_available": bool(user_hist["redo"]),
        }
        await sio.emit("batch_update", payload, room=board_id, skip_sid=sid)
    else:
        await sio.emit(
            "update",
            {
                "board_id": board_id,
                "canvas_json": new_canvas,
                "updated_at": updated_at,
                "author": session["user_id"],
                "undo_available": bool(user_hist["undo"]),
                "redo_available": bool(user_hist["redo"]),
            },
            room=board_id,
            skip_sid=sid,
        )
    await sio.emit(
        "history_state",
        {"undo_available": bool(user_hist["undo"]), "redo_available": bool(user_hist["redo"])},
        to=sid,
    )


@sio.event
async def batch_update(sid: str, data: Any):
    await _process_ops_event(sid, data)


async def _process_ops_event(sid: str, data: Any):
    session = await sio.get_session(sid)
    if not session or not _can_edit(session):
        await sio.emit("error_msg", {"message": "Read-only access"}, to=sid)
        return
    if not await _check_socket_rate_limit(sid, "ops"):
        await sio.emit("error_msg", {"message": "Too many updates, slow down"}, to=sid)
        return
    if not isinstance(data, dict) or not isinstance(data.get("ops"), list):
        await sio.emit("error_msg", {"message": "ops must be array"}, to=sid)
        return

    board_id = session["board_id"]
    user = UserContext(
        user_id=session["user_id"],
        username=session["username"],
        role=session["jwt_role"],
        payload={"user_id": session["user_id"]},
    )

    try:
        applied_ops, inverse_ops, updated_at, op_ids, should_compact = await board_task_manager.run(
            board_id, _db_apply_ops, board_id, data["ops"], user
        )
    except ValueError:
        await sio.emit("error_msg", {"message": f"Board size exceeds {MAX_BOARD_BYTES // (1024 * 1024)} MB"}, to=sid)
        canvas_json, max_op_id = await board_task_manager.run(board_id, _db_read_canvas_and_max_id, board_id)
        await sio.emit("update", {
            "board_id": board_id,
            "canvas_json": canvas_json,
            "updated_at": now_iso(),
            "author": "system",
            "last_seq_id": max_op_id
        }, to=sid)
        return
    except Exception as exc:
        await sio.emit("error_msg", {"message": f"Database error: {str(exc)}"}, to=sid)
        return

    if should_compact:
        trigger_compaction_debounced(board_id)

    actor_key = _history_actor_key(session)
    user_hist = await board_manager.get_user_history(board_id, actor_key)

    if not applied_ops:
        await sio.emit(
            "history_state",
            {"undo_available": bool(user_hist["undo"]), "redo_available": bool(user_hist["redo"])},
            to=sid,
        )
        return

    user_hist["undo"].append({"undo_ops": inverse_ops, "redo_ops": applied_ops})
    if len(user_hist["undo"]) > 200:
        user_hist["undo"] = user_hist["undo"][-200:]
    user_hist["redo"].clear()
    await board_manager.save_user_history(board_id, actor_key, user_hist)

    wire_ops = _decorate_ops_for_wire(applied_ops, session, op_ids)
    payload = {"board_id": board_id, "ops": wire_ops, "updated_at": updated_at, "author": session["user_id"]}
    await sio.emit("batch_update", payload, room=board_id, skip_sid=sid)
    await sio.emit(
        "history_state",
        {"undo_available": bool(user_hist["undo"]), "redo_available": bool(user_hist["redo"])},
        to=sid,
    )


@sio.event
async def clear(sid: str):
    session = await sio.get_session(sid)
    if not session or session.get("jwt_role") != "moderator":
        await sio.emit("error_msg", {"message": "Only moderator can clear board"}, to=sid)
        return

    board_id = session["board_id"]
    cleared = default_canvas_state()
    try:
        updated_at = await board_task_manager.run(board_id, _db_clear, board_id, cleared)
    except Exception as exc:
        await sio.emit("error_msg", {"message": f"Database error: {str(exc)}"}, to=sid)
        return
        
    await board_manager.clear_history(board_id)

    await sio.emit(
        "clear",
        {"board_id": board_id, "canvas_json": cleared, "updated_at": updated_at, "author": session["user_id"]},
        room=board_id,
    )


@sio.event
async def save(sid: str):
    await sio.emit("saved", {"ok": True}, to=sid)


@sio.event
async def history_state_request(sid: str):
    session = await sio.get_session(sid)
    if not session:
        return
    hist = await board_manager.get_user_history(session["board_id"], _history_actor_key(session))
    await sio.emit(
        "history_state",
        {"undo_available": bool(hist["undo"]), "redo_available": bool(hist["redo"])},
        to=sid,
    )


@sio.event
async def undo(sid: str):
    session = await sio.get_session(sid)
    if not session or not _can_edit(session):
        return

    board_id = session["board_id"]
    actor_key = _history_actor_key(session)
    user_hist = await board_manager.get_user_history(board_id, actor_key)
    if not user_hist["undo"]:
        await sio.emit("history_state", {"undo_available": False, "redo_available": bool(user_hist["redo"])}, to=sid)
        return

    action = user_hist["undo"].pop()
    user = UserContext(session["user_id"], session["username"], session["jwt_role"], {})
    try:
        applied_ops, next_canvas, updated_at, op_ids, should_compact = await board_task_manager.run(
            board_id, _db_undo, board_id, action, user
        )
    except ValueError:
        await sio.emit("error_msg", {"message": f"Board size exceeds {MAX_BOARD_BYTES // (1024 * 1024)} MB"}, to=sid)
        user_hist["undo"].append(action)
        await board_manager.save_user_history(board_id, actor_key, user_hist)
        return
    except Exception as exc:
        await sio.emit("error_msg", {"message": f"Database error: {str(exc)}"}, to=sid)
        user_hist["undo"].append(action)
        await board_manager.save_user_history(board_id, actor_key, user_hist)
        return

    if should_compact:
        trigger_compaction_debounced(board_id)

    user_hist["redo"].append(action)
    await board_manager.save_user_history(board_id, actor_key, user_hist)
    if applied_ops:
        wire_ops = _decorate_ops_for_wire(applied_ops, session, op_ids)
        payload = {"board_id": board_id, "ops": wire_ops, "updated_at": updated_at, "author": session["user_id"]}
        await sio.emit("batch_update", payload, room=board_id)
    else:
        await sio.emit(
            "update",
            {
                "board_id": board_id,
                "canvas_json": next_canvas,
                "updated_at": updated_at,
                "author": session["user_id"],
            },
            room=board_id,
        )
    await sio.emit(
        "history_state",
        {"undo_available": bool(user_hist["undo"]), "redo_available": bool(user_hist["redo"])},
        to=sid,
    )


@sio.event
async def redo(sid: str):
    session = await sio.get_session(sid)
    if not session or not _can_edit(session):
        return

    board_id = session["board_id"]
    actor_key = _history_actor_key(session)
    user_hist = await board_manager.get_user_history(board_id, actor_key)
    if not user_hist["redo"]:
        await sio.emit("history_state", {"undo_available": bool(user_hist["undo"]), "redo_available": False}, to=sid)
        return

    action = user_hist["redo"].pop()
    user = UserContext(session["user_id"], session["username"], session["jwt_role"], {})
    try:
        applied_ops, next_canvas, updated_at, op_ids, should_compact = await board_task_manager.run(
            board_id, _db_redo, board_id, action, user
        )
    except ValueError:
        await sio.emit("error_msg", {"message": f"Board size exceeds {MAX_BOARD_BYTES // (1024 * 1024)} MB"}, to=sid)
        user_hist["redo"].append(action)
        await board_manager.save_user_history(board_id, actor_key, user_hist)
        return
    except Exception as exc:
        await sio.emit("error_msg", {"message": f"Database error: {str(exc)}"}, to=sid)
        user_hist["redo"].append(action)
        await board_manager.save_user_history(board_id, actor_key, user_hist)
        return

    if should_compact:
        trigger_compaction_debounced(board_id)

    user_hist["undo"].append(action)
    await board_manager.save_user_history(board_id, actor_key, user_hist)
    if applied_ops:
        wire_ops = _decorate_ops_for_wire(applied_ops, session, op_ids)
        payload = {"board_id": board_id, "ops": wire_ops, "updated_at": updated_at, "author": session["user_id"]}
        await sio.emit("batch_update", payload, room=board_id)
    else:
        await sio.emit(
            "update",
            {
                "board_id": board_id,
                "canvas_json": next_canvas,
                "updated_at": updated_at,
                "author": session["user_id"],
            },
            room=board_id,
        )
    await sio.emit(
        "history_state",
        {"undo_available": bool(user_hist["undo"]), "redo_available": bool(user_hist["redo"])},
        to=sid,
    )


asgi_app = socketio.ASGIApp(sio, other_asgi_app=app)
