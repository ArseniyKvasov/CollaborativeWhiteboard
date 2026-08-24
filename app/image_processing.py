"""
Image processing module.

This module provides comprehensive image processing utilities for the Whiteboard application,
including validation, processing, compression, and error handling for uploaded images.
"""

import io
import logging
import uuid
from pathlib import Path
from typing import Tuple, Optional

from PIL import Image, ImageOps

UPLOAD_IMAGE_MAX_SIDE = 2400
UPLOAD_IMAGE_WEBP_QUALITY = 82

logger = logging.getLogger(__name__)


def validate_image_file(raw: bytes, board_id: str) -> Tuple[bool, Optional[str]]:
    """
    Validate image file before processing.

    Args:
        raw: Raw image bytes
        board_id: Board ID for error context

    Returns:
        Tuple[bool, Optional[str]]: (is_valid, error_message)
    """
    if not raw:
        error_msg = f"Empty image data for board {board_id}"
        logger.error(error_msg)
        return False, error_msg

    if len(raw) > 50 * 1024 * 1024:  # 50MB limit
        error_msg = f"Image data too large ({len(raw)} bytes) for board {board_id}"
        logger.error(error_msg)
        return False, error_msg

    try:
        # Try to open the image to validate it's a valid image format
        img = Image.open(io.BytesIO(raw))
        img.verify()  # Verify without loading
        img.close()
        logger.debug(f"Image validation passed for board {board_id}: {len(raw)} bytes")
        return True, None
    except Exception as exc:
        error_msg = f"Invalid image format for board {board_id}: {exc}"
        logger.error(error_msg)
        return False, error_msg


def process_image_bytes(raw: bytes) -> Tuple[bytes, int, int]:
    """Downsize/re-encode raw image bytes to WEBP. Returns (webp_bytes, width, height)."""
    try:
        img = Image.open(io.BytesIO(raw))
        img.load()
    except Exception as exc:
        error_msg = f"Failed to open image: {exc}"
        logger.error(error_msg)
        raise ValueError(f"Invalid or unsupported image file: {exc}") from exc

    try:
        img = ImageOps.exif_transpose(img) or img
    except Exception as exc:
        logger.error(f"Failed to transpose image: {exc}")
        raise

    if img.mode not in ("RGB", "RGBA"):
        try:
            img = img.convert("RGBA" if "A" in img.getbands() else "RGB")
        except Exception as exc:
            logger.error(f"Failed to convert image mode: {exc}")
            raise

    width, height = img.size
    longest = max(width, height)

    if longest > UPLOAD_IMAGE_MAX_SIDE:
        try:
            scale = UPLOAD_IMAGE_MAX_SIDE / longest
            new_width = max(1, round(width * scale))
            new_height = max(1, round(height * scale))
            img = img.resize((new_width, new_height), Image.LANCZOS)
            width, height = new_width, new_height
        except Exception as exc:
            logger.error(f"Failed to resize image: {exc}")
            raise

    try:
        out = io.BytesIO()
        img.save(out, format="WEBP", quality=UPLOAD_IMAGE_WEBP_QUALITY, method=6)
        webp_bytes = out.getvalue()
        logger.debug(f"Image compression completed: {width}x{height}, {len(raw)} -> {len(webp_bytes)} bytes")
        return webp_bytes, width, height
    except Exception as exc:
        logger.error(f"Failed to save compressed image: {exc}")
        raise


def save_processed_image(upload_dir: Path, board_id: str, webp_bytes: bytes) -> str:
    """Writes already-processed WEBP bytes under upload_dir/board_id/ and
    returns the public /uploads/... URL."""
    if not webp_bytes:
        raise ValueError("Cannot save empty webp bytes")

    if len(webp_bytes) > 10 * 1024 * 1024:  # 10MB limit per image
        raise ValueError(f"Processed image too large: {len(webp_bytes)} bytes")

    board_dir = upload_dir / board_id

    try:
        board_dir.mkdir(parents=True, exist_ok=True)
        logger.debug(f"Created/upload directory: {board_dir}")
    except Exception as exc:
        logger.error(f"Failed to create directory {board_dir}: {exc}")
        raise

    filename = f"{uuid.uuid4().hex}.webp"

    try:
        target_path = board_dir / filename
        target_path.write_bytes(webp_bytes)
        file_size = target_path.stat().st_size
        logger.info(f"Processed image saved: {target_path} ({file_size:,} bytes)")
        return f"/uploads/{board_id}/{filename}"
    except Exception as exc:
        logger.error(f"Failed to write image file {filename}: {exc}")
        raise


def process_and_store_image(upload_dir: Path, board_id: str, raw: bytes) -> Tuple[str, int, int]:
    """Convenience wrapper: process then save in one call."""
    # Validate input first
    if not upload_dir or not board_id or not raw:
        raise ValueError("Invalid parameters for process_and_store_image")

    # Process with error handling
    try:
        webp_bytes, width, height = process_image_bytes(raw)
        url = save_processed_image(upload_dir, board_id, webp_bytes)
        logger.info(f"Successfully processed and stored image for board {board_id}: {url}")
        return url, width, height
    except Exception as exc:
        logger.error(f"Failed to process and store image for board {board_id}: {exc}")
        raise
