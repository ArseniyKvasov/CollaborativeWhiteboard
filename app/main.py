import json
import os
import sqlite3
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from urllib.parse import parse_qs

import jwt
import socketio
from fastapi import Depends, FastAPI, Header, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel, Field
from starlette.requests import Request

BASE_DIR = Path(__file__).resolve().parent

JWT_SECRET = os.getenv("JWT_SECRET", "change-me")
SERVICE_API_KEY = os.getenv("SERVICE_API_KEY", "")
DATABASE_URL = os.getenv("DATABASE_URL", str(BASE_DIR / "boards.db"))
CORS_ORIGINS = [o.strip() for o in os.getenv("CORS_ORIGINS", "*").split(",") if o.strip()]
DEBUG_RAW = os.getenv("DEBUG", "false")
DEBUG = str(DEBUG_RAW).strip().lower() in {"1", "true", "yes", "on"}
DEBUG_USER_ID = os.getenv("DEBUG_USER_ID", "debug-user")
JWT_LEEWAY_SECONDS = int(os.getenv("JWT_LEEWAY_SECONDS", "45"))
WS_ACCESS_TTL_SECONDS = int(os.getenv("WS_ACCESS_TTL_SECONDS", "300"))
WS_REFRESH_TTL_SECONDS = int(os.getenv("WS_REFRESH_TTL_SECONDS", "28800"))

app = FastAPI(title="Whiteboard Service", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))


ROLE_RANK = {"viewer": 1, "editor": 2, "owner": 3}
PENDING_OWNER_ID = "__pending_moderator__"
MAX_BOARD_BYTES = 15 * 1024 * 1024
DEFAULT_SURFACE_ID = "main"
SOCKET_MAX_BUFFER_BYTES = 20 * 1024 * 1024


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


def get_db() -> sqlite3.Connection:
    conn = sqlite3.connect(DATABASE_URL)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def init_db() -> None:
    conn = get_db()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS boards (
                board_id TEXT PRIMARY KEY,
                canvas_json TEXT NOT NULL,
                owner_id TEXT NOT NULL,
                allow_students_draw INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
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
        conn.commit()
    finally:
        conn.close()


@app.on_event("startup")
def on_startup() -> None:
    init_db()


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
        raise ValueError(f"Board size exceeds 15 MB ({size} bytes)")


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

    conn.execute(
        """
        INSERT INTO board_members (board_id, user_id, role, created_at)
        VALUES (?, ?, 'editor', ?)
        ON CONFLICT(board_id, user_id)
        DO UPDATE SET role = CASE WHEN board_members.role = 'owner' THEN board_members.role ELSE 'editor' END
        """,
        (board_id, user.user_id, ts),
    )
    conn.commit()
    return "editor"


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


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
def upsert_member(board_id: str, body: MemberRequest, user: UserContext = Depends(authenticate_request)):
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
        return {"ok": True}
    finally:
        conn.close()


@app.delete("/api/board/{board_id}/members/{member_id}")
def remove_member(board_id: str, member_id: str, user: UserContext = Depends(authenticate_request)):
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
        return {"ok": True}
    finally:
        conn.close()


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


def _decorate_ops_for_wire(ops: list[dict[str, Any]], session: dict[str, Any]) -> list[dict[str, Any]]:
    now_ms = int(time.time() * 1000)
    client_id = str(session.get("client_id") or "")
    decorated: list[dict[str, Any]] = []
    for index, op in enumerate(ops, start=1):
        if not isinstance(op, dict):
            continue
        op_name = str(op.get("type") or op.get("op") or "").lower()
        if op_name not in {"add", "update", "remove"}:
            continue
        payload: dict[str, Any] = {
            "v": 1,
            "op": op_name,
            "client_id": client_id,
            "seq": now_ms + index,
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
    def __init__(self):
        self.online: dict[str, dict[str, dict[str, str]]] = {}
        self.history: dict[str, dict[str, dict[str, list[dict[str, Any]]]]] = {}

    def add(self, board_id: str, sid: str, user_id: str, username: str, role: str, client_id: str) -> None:
        self.online.setdefault(board_id, {})[sid] = {
            "user_id": user_id,
            "username": username,
            "role": role,
            "client_id": client_id,
        }

    def remove(self, board_id: str, sid: str) -> Optional[dict[str, str]]:
        room = self.online.get(board_id, {})
        meta = room.pop(sid, None)
        if not room and board_id in self.online:
            del self.online[board_id]
        return meta

    def online_count(self, board_id: str) -> int:
        return len(self.online.get(board_id, {}))

    def get_user_history(self, board_id: str, user_id: str) -> dict[str, list[dict[str, Any]]]:
        board_hist = self.history.setdefault(board_id, {})
        return board_hist.setdefault(user_id, {"undo": [], "redo": []})


board_manager = SocketBoardManager()
sio = socketio.AsyncServer(
    async_mode="asgi",
    cors_allowed_origins=CORS_ORIGINS,
    logger=False,
    engineio_logger=False,
    max_http_buffer_size=SOCKET_MAX_BUFFER_BYTES,
)


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
    return _apply_prepared_ops(canvas, ops, UserContext("system", "system", "system", {}))


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

    conn = get_db()
    try:
        try:
            ensure_user_board_access(conn, board_id, user)
            ensure_board_exists(conn, board_id)
            board_role = require_role(conn, board_id, user.user_id, "viewer")
            canvas_json = _read_board_canvas(conn, board_id)
            allow_students_draw = board_allows_students_draw(conn, board_id)
        except HTTPException as exc:
            raise ConnectionRefusedError(exc.detail)
    finally:
        conn.close()

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
    board_manager.add(board_id, sid, user.user_id, user.username, user.role, client_id)

    await sio.emit(
        "init",
        {
            "board_id": board_id,
            "canvas_json": canvas_json,
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
            "online": board_manager.online_count(board_id),
        },
        to=sid,
    )
    await sio.emit("presence", {"online": board_manager.online_count(board_id)}, room=board_id)


@sio.event
async def disconnect(sid: str):
    session = await sio.get_session(sid)
    if not session:
        return
    board_id = session["board_id"]
    client_id = session["client_id"]
    board_manager.remove(board_id, sid)
    await sio.emit("cursor_remove", {"client_id": client_id}, room=board_id)
    await sio.emit("presence", {"online": board_manager.online_count(board_id)}, room=board_id)


@sio.event
async def cursor(sid: str, data: Any):
    session = await sio.get_session(sid)
    if not session or not isinstance(data, dict):
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

    applied_ops: list[dict[str, Any]] = []
    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        old_canvas = _read_board_canvas(conn, board_id)
        action = _build_action(old_canvas, new_canvas)
        if action:
            _extract_added_objects(action, new_canvas)
            actor_key = _history_actor_key(session)
            user_hist = board_manager.get_user_history(board_id, actor_key)
            user_hist["undo"].append(action)
            if len(user_hist["undo"]) > 100:
                user_hist["undo"] = user_hist["undo"][-100:]
            user_hist["redo"].clear()

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

        try:
            _ensure_board_size_limit(new_canvas)
            updated_at = _append_board_ops(conn, board_id, applied_ops)
            conn.commit()
        except ValueError:
            conn.rollback()
            await sio.emit("error_msg", {"message": "Board size exceeds 15 MB"}, to=sid)
            return
    finally:
        conn.close()

    user_hist = board_manager.get_user_history(board_id, _history_actor_key(session))
    if applied_ops:
        wire_ops = _decorate_ops_for_wire(applied_ops, session)
        payload = {
            "board_id": board_id,
            "ops": wire_ops,
            "updated_at": updated_at,
            "author": session["user_id"],
            "undo_available": bool(user_hist["undo"]),
            "redo_available": bool(user_hist["redo"]),
        }
        await sio.emit("batch_update", payload, room=board_id, skip_sid=sid)
        await sio.emit("ops", payload, room=board_id, skip_sid=sid)
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
async def ops(sid: str, data: Any):
    await _process_ops_event(sid, data)


@sio.event
async def batch_update(sid: str, data: Any):
    await _process_ops_event(sid, data)


async def _process_ops_event(sid: str, data: Any):
    session = await sio.get_session(sid)
    if not session or not _can_edit(session):
        await sio.emit("error_msg", {"message": "Read-only access"}, to=sid)
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

    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        current = _read_board_canvas(conn, board_id)
        next_canvas, applied_ops, inverse_ops = _apply_ops_build_inverse(current, data["ops"], user)
        if not applied_ops:
            conn.commit()
            hist = board_manager.get_user_history(board_id, _history_actor_key(session))
            await sio.emit(
                "history_state",
                {"undo_available": bool(hist["undo"]), "redo_available": bool(hist["redo"])},
                to=sid,
            )
            return

        try:
            _ensure_board_size_limit(next_canvas)
            updated_at = _append_board_ops(conn, board_id, applied_ops)
            conn.commit()
        except ValueError:
            conn.rollback()
            await sio.emit("error_msg", {"message": "Board size exceeds 15 MB"}, to=sid)
            return
    finally:
        conn.close()

    user_hist = board_manager.get_user_history(board_id, _history_actor_key(session))
    user_hist["undo"].append({"undo_ops": inverse_ops, "redo_ops": applied_ops})
    if len(user_hist["undo"]) > 200:
        user_hist["undo"] = user_hist["undo"][-200:]
    user_hist["redo"].clear()

    wire_ops = _decorate_ops_for_wire(applied_ops, session)
    payload = {"board_id": board_id, "ops": wire_ops, "updated_at": updated_at, "author": session["user_id"]}
    await sio.emit("batch_update", payload, room=board_id, skip_sid=sid)
    await sio.emit("ops", payload, room=board_id, skip_sid=sid)
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
    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        updated_at = now_iso()
        conn.execute("DELETE FROM board_ops WHERE board_id = ?", (board_id,))
        conn.execute("UPDATE boards SET updated_at = ? WHERE board_id = ?", (updated_at, board_id))
        conn.commit()
    finally:
        conn.close()
    board_manager.history[board_id] = {}

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
    hist = board_manager.get_user_history(session["board_id"], _history_actor_key(session))
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
    user_hist = board_manager.get_user_history(board_id, _history_actor_key(session))
    if not user_hist["undo"]:
        await sio.emit("history_state", {"undo_available": False, "redo_available": bool(user_hist["redo"])}, to=sid)
        return

    action = user_hist["undo"].pop()
    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        current = _read_board_canvas(conn, board_id)
        if isinstance(action, dict) and isinstance(action.get("undo_ops"), list):
            applied_ops = action["undo_ops"]
            next_canvas = _apply_prepared_ops(
                current,
                applied_ops,
                UserContext(session["user_id"], session["username"], session["jwt_role"], {}),
            )
        else:
            next_canvas = _apply_undo_action(current, action)
            next_canvas = _normalize_canvas(next_canvas, UserContext(session["user_id"], session["username"], session["jwt_role"], {}))
            applied_ops = []
        try:
            _ensure_board_size_limit(next_canvas)
            if applied_ops:
                updated_at = _append_board_ops(conn, board_id, applied_ops)
            else:
                updated_at = _replace_board_canvas_baseline(conn, board_id, next_canvas)
            conn.commit()
        except ValueError:
            conn.rollback()
            await sio.emit("error_msg", {"message": "Board size exceeds 15 MB"}, to=sid)
            return
    finally:
        conn.close()

    user_hist["redo"].append(action)
    if applied_ops:
        wire_ops = _decorate_ops_for_wire(applied_ops, session)
        payload = {"board_id": board_id, "ops": wire_ops, "updated_at": updated_at, "author": session["user_id"]}
        await sio.emit("batch_update", payload, room=board_id)
        await sio.emit("ops", payload, room=board_id)
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
    user_hist = board_manager.get_user_history(board_id, _history_actor_key(session))
    if not user_hist["redo"]:
        await sio.emit("history_state", {"undo_available": bool(user_hist["undo"]), "redo_available": False}, to=sid)
        return

    action = user_hist["redo"].pop()
    conn = get_db()
    try:
        conn.execute("BEGIN IMMEDIATE")
        current = _read_board_canvas(conn, board_id)
        if isinstance(action, dict) and isinstance(action.get("redo_ops"), list):
            applied_ops = action["redo_ops"]
            next_canvas = _apply_prepared_ops(
                current,
                applied_ops,
                UserContext(session["user_id"], session["username"], session["jwt_role"], {}),
            )
        else:
            next_canvas = _apply_redo_action_with_added(current, action)
            next_canvas = _normalize_canvas(next_canvas, UserContext(session["user_id"], session["username"], session["jwt_role"], {}))
            applied_ops = []
        try:
            _ensure_board_size_limit(next_canvas)
            if applied_ops:
                updated_at = _append_board_ops(conn, board_id, applied_ops)
            else:
                updated_at = _replace_board_canvas_baseline(conn, board_id, next_canvas)
            conn.commit()
        except ValueError:
            conn.rollback()
            await sio.emit("error_msg", {"message": "Board size exceeds 15 MB"}, to=sid)
            return
    finally:
        conn.close()

    user_hist["undo"].append(action)
    if applied_ops:
        wire_ops = _decorate_ops_for_wire(applied_ops, session)
        payload = {"board_id": board_id, "ops": wire_ops, "updated_at": updated_at, "author": session["user_id"]}
        await sio.emit("batch_update", payload, room=board_id)
        await sio.emit("ops", payload, room=board_id)
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
