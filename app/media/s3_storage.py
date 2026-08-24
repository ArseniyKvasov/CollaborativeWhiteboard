"""S3 media storage (Yandex Object Storage and any S3-compatible endpoint).

Settings come straight from the environment so the module stays
self-contained. S3 is considered enabled only when all four required vars are
set; otherwise every helper degrades gracefully and callers fall back to local
disk - which keeps deploys, dev runs and rollback safe.
"""

import logging
import os
from typing import Optional, Tuple

logger = logging.getLogger("whiteboard")

try:
    import boto3
    from botocore.config import Config as BotoConfig
    from botocore.exceptions import ClientError

    BOTO3_AVAILABLE = True
except ImportError:  # pragma: no cover - dev containers without boto3 yet
    BOTO3_AVAILABLE = False

S3_BUCKET: Optional[str] = os.getenv("MEDIA_S3_BUCKET")
S3_LOCATION: str = os.getenv("MEDIA_S3_LOCATION", "media").strip("/")
S3_ENDPOINT_URL: Optional[str] = os.getenv("MEDIA_S3_ENDPOINT_URL")
S3_REGION_NAME: str = os.getenv("MEDIA_S3_REGION_NAME", "ru-central1")
S3_ACCESS_KEY_ID: Optional[str] = os.getenv("MEDIA_S3_ACCESS_KEY_ID")
S3_SECRET_ACCESS_KEY: Optional[str] = os.getenv("MEDIA_S3_SECRET_ACCESS_KEY")

_client = None


def s3_enabled() -> bool:
    """True when boto3 is installed and all required settings are present."""
    return bool(
        BOTO3_AVAILABLE
        and S3_BUCKET
        and S3_ENDPOINT_URL
        and S3_ACCESS_KEY_ID
        and S3_SECRET_ACCESS_KEY
    )


def get_client():
    """Lazily build a process-wide boto3 client; returns None when disabled."""
    global _client
    if not s3_enabled():
        return None
    if _client is None:
        try:
            _client = boto3.client(
                "s3",
                endpoint_url=S3_ENDPOINT_URL,
                region_name=S3_REGION_NAME,
                aws_access_key_id=S3_ACCESS_KEY_ID,
                aws_secret_access_key=S3_SECRET_ACCESS_KEY,
                # Path-style addressing - Yandex Object Storage serves both,
                # but path-style avoids virtual-host DNS surprises.
                config=BotoConfig(signature_version="s3v4", addressing_style="path"),
            )
            logger.info("S3 media client ready: bucket=%s location=%s", S3_BUCKET, S3_LOCATION)
        except Exception:
            logger.exception("Failed to init S3 client - falling back to local disk")
            return None
    return _client


def object_key(board_id: str, filename: str) -> str:
    return f"{S3_LOCATION}/{board_id}/{filename}"


def put_media(board_id: str, filename: str, data: bytes, content_type: str) -> str:
    """Upload processed media bytes; raises on failure so Celery retries."""
    client = get_client()
    if client is None:
        raise RuntimeError("S3 storage is not configured (MEDIA_S3_* env)")
    key = object_key(board_id, filename)
    client.put_object(
        Bucket=S3_BUCKET,
        Key=key,
        Body=data,
        ContentType=content_type,
        CacheControl="public, max-age=31536000, immutable",
    )
    return key


def get_media_stream(key: str) -> Optional[Tuple[object, int]]:
    """Return (streaming body, content_length) or None if the key is absent."""
    client = get_client()
    if client is None:
        return None
    try:
        resp = client.get_object(Bucket=S3_BUCKET, Key=key)
        return resp["Body"], int(resp.get("ContentLength") or 0)
    except ClientError as exc:
        code = getattr(exc, "response", {}).get("Error", {}).get("Code", "")
        if code in {"NoSuchKey", "404", "NotFound"}:
            return None
        logger.error("S3 get failed for %s: %s", key, exc)
        raise
