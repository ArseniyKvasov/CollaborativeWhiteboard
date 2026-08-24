"""Persist and fetch processed media with S3 as the source of truth.

URL contract is unchanged for the frontend and the database: every image keeps
its historical ``/uploads/{board_id}/{filename}`` path. Only the backing store
changed - S3 when configured, local disk as a fallback for pre-migration files
and as the dev-mode store when MEDIA_S3_* is not configured.
"""

import logging
import re
from pathlib import Path
from typing import Optional

from fastapi.responses import FileResponse, StreamingResponse

from app.media.s3_storage import get_media_stream, object_key, put_media, s3_enabled

logger = logging.getLogger("whiteboard")

SAFE_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
SAFE_NAME_RE = re.compile(r"^[A-Za-z0-9._-]{1,200}$")

CONTENT_TYPES = {
    ".webp": "image/webp",
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".gif": "image/gif",
}

MEDIA_CACHE_HEADERS = {"Cache-Control": "public, max-age=31536000, immutable"}


def valid_media_path(board_id: str, filename: str) -> bool:
    return bool(SAFE_ID_RE.match(board_id)) and bool(SAFE_NAME_RE.match(filename))


def persist_media(board_id: str, filename: str, data: bytes) -> str:
    """Store processed bytes in S3 and return the public /uploads/... URL.

    Raises on any S3 failure so background tasks retry instead of silently
    dropping the image. Local disk is intentionally NOT written for new files -
    S3 is the single source of truth (the pending-staging file lifecycle is
    handled separately by the upload task).
    """
    ext = Path(filename).suffix.lower()
    content_type = CONTENT_TYPES.get(ext, "application/octet-stream")
    put_media(board_id, filename, data, content_type)
    logger.info("media stored in s3: %s (%d bytes)", object_key(board_id, filename), len(data))
    return f"/uploads/{board_id}/{filename}"


def open_media(board_id: str, filename: str, local_root: Path):
    """Resolve an /uploads/... path to a response-ready source.

    Returns a Starlette Response on success or None when the object does not
    exist anywhere. S3 first (source of truth), then the local uploads dir -
    that fallback serves everything uploaded before the migration ran, and is
    the primary store in dev where MEDIA_S3_* is usually unset.
    """
    if not valid_media_path(board_id, filename):
        return None

    if s3_enabled():
        try:
            stream = get_media_stream(object_key(board_id, filename))
        except Exception:
            stream = None  # transient S3 error -> serve from local copy if present
        if stream is not None:
            body, length = stream
            headers = dict(MEDIA_CACHE_HEADERS)
            if length:
                headers["Content-Length"] = str(length)
            ext = Path(filename).suffix.lower()
            return StreamingResponse(
                body,
                media_type=CONTENT_TYPES.get(ext, "application/octet-stream"),
                headers=headers,
            )

    local = local_root / board_id / filename
    if local.is_file():
        ext = Path(filename).suffix.lower()
        return FileResponse(
            local,
            media_type=CONTENT_TYPES.get(ext, "application/octet-stream"),
            headers=MEDIA_CACHE_HEADERS,
        )
    return None
