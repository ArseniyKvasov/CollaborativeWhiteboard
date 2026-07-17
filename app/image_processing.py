"""Pure image processing helpers - no FastAPI/Celery imports here, so this
module can be used both from the sync Miro-import path and from the Celery
worker without either one pulling in the other's dependencies."""

import io
import uuid
from pathlib import Path

from PIL import Image, ImageOps

UPLOAD_IMAGE_MAX_SIDE = 2400
UPLOAD_IMAGE_WEBP_QUALITY = 82


def process_image_bytes(raw: bytes) -> tuple[bytes, int, int]:
    """Downsize/re-encode raw image bytes to WEBP. Returns (webp_bytes, width, height)."""
    try:
        img = Image.open(io.BytesIO(raw))
        img.load()
    except Exception as exc:
        raise ValueError("Invalid or unsupported image file") from exc

    img = ImageOps.exif_transpose(img) or img
    if img.mode not in ("RGB", "RGBA"):
        img = img.convert("RGBA" if "A" in img.getbands() else "RGB")

    width, height = img.size
    longest = max(width, height)
    if longest > UPLOAD_IMAGE_MAX_SIDE:
        scale = UPLOAD_IMAGE_MAX_SIDE / longest
        width = max(1, round(width * scale))
        height = max(1, round(height * scale))
        img = img.resize((width, height), Image.LANCZOS)

    out = io.BytesIO()
    img.save(out, format="WEBP", quality=UPLOAD_IMAGE_WEBP_QUALITY, method=6)
    return out.getvalue(), width, height


def save_processed_image(upload_dir: Path, board_id: str, webp_bytes: bytes) -> str:
    """Writes already-processed WEBP bytes under upload_dir/board_id/ and
    returns the public /uploads/... URL."""
    board_dir = upload_dir / board_id
    board_dir.mkdir(parents=True, exist_ok=True)
    filename = f"{uuid.uuid4().hex}.webp"
    (board_dir / filename).write_bytes(webp_bytes)
    return f"/uploads/{board_id}/{filename}"


def process_and_store_image(upload_dir: Path, board_id: str, raw: bytes) -> tuple[str, int, int]:
    """Convenience wrapper: process then save in one call."""
    webp_bytes, width, height = process_image_bytes(raw)
    url = save_processed_image(upload_dir, board_id, webp_bytes)
    return url, width, height
