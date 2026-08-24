import os
import uuid
from pathlib import Path

from app.celery_app import celery_app
from app.image_processing import process_image_bytes
from app.media.store import persist_media

BASE_DIR = Path(__file__).resolve().parent
UPLOAD_DIR = Path(os.getenv("UPLOAD_DIR", str(BASE_DIR / "uploads")))
PENDING_UPLOAD_DIR = UPLOAD_DIR / "_pending"


@celery_app.task(name="tasks.process_image_task")
def process_image_task(board_id: str, pending_path: str) -> dict:
    """Reads the raw upload the API endpoint staged on disk, compresses it,
    stores the final WEBP in S3 (falls back to local disk when MEDIA_S3_* is
    not configured), and cleans up the staged file - this is the actual work
    the frontend polls for via the job_id it got back from POST /upload-image."""
    path = Path(pending_path)
    try:
        raw = path.read_bytes()
        webp_bytes, width, height = process_image_bytes(raw)
        url = persist_media(board_id, f"{uuid.uuid4().hex}.webp", webp_bytes)
        return {"url": url, "width": width, "height": height}
    finally:
        path.unlink(missing_ok=True)
