"""Media storage: S3 client (Yandex Object Storage compatible) + helpers.

Public API lives in ``app.media.store`` (persist/open) and
``app.media.s3_storage`` (raw boto3 wrapper). MEDIA_S3_* env vars configure the
client; without them everything transparently falls back to local disk.
"""

from app.media.s3_storage import s3_enabled  # noqa: F401
