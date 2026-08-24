#!/usr/bin/env python
"""One-shot migration of locally stored uploads into S3.

Idempotent: files already present in the bucket (same key AND same size) are
skipped, so it is safe to re-run after an interrupted pass or a deploy.
The `_pending` staging directory is always skipped.

Usage (inside the container, where UPLOAD_DIR points at the volume):

    python scripts/migrate_uploads_to_s3.py            # copy only (dry-run: add --dry-run)
    python scripts/migrate_uploads_to_s3.py --dry-run  # list what would happen
    python scripts/migrate_uploads_to_s3.py --delete-local   # rm local copies after verified upload

Exit code is non-zero if any file failed to upload - do NOT run --delete-local
in that case; just fix the issue and re-run (already-copied files are skipped).
"""

import argparse
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from botocore.exceptions import ClientError  # noqa: E402
from app.media.s3_storage import get_client, object_key, s3_enabled  # noqa: E402
from app.media.store import CONTENT_TYPES  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="only print planned actions")
    parser.add_argument("--delete-local", action="store_true",
                        help="remove the local file after a VERIFIED s3 upload")
    args = parser.parse_args()

    upload_dir = Path(os.getenv("UPLOAD_DIR", str(Path(__file__).resolve().parent.parent / "uploads")))
    pending_dir = upload_dir / "_pending"

    files = [
        p for p in sorted(upload_dir.rglob("*"))
        if p.is_file() and pending_dir not in p.parents and p != pending_dir
    ]
    if not files:
        print(f"No media files found under {upload_dir} - nothing to migrate.")
        return 0

    if not s3_enabled():
        print("ERROR: S3 is not configured. Set MEDIA_S3_BUCKET / MEDIA_S3_ENDPOINT_URL / "
              "MEDIA_S3_ACCESS_KEY_ID / MEDIA_S3_SECRET_ACCESS_KEY first.")
        return 2
    client = get_client()
    if client is None:
        print("ERROR: S3 client init failed - check MEDIA_S3_* credentials and logs above.")
        return 2
    bucket = os.getenv("MEDIA_S3_BUCKET")

    uploaded = skipped = failed = deleted = 0
    failed_names: list[str] = []

    for path in files:
        board_id = path.parent.name
        filename = path.name
        key = object_key(board_id, filename)
        size = path.stat().st_size
        rel = str(path.relative_to(upload_dir))

        already = False
        try:
            head = client.head_object(Bucket=bucket, Key=key)
            already = int(head.get("ContentLength") or 0) == size
        except ClientError:
            already = False

        if already:
            skipped += 1
            action = "skip (exists)"
        elif args.dry_run:
            uploaded += 1
            action = "would upload"
        else:
            ext = path.suffix.lower()
            content_type = CONTENT_TYPES.get(ext, "application/octet-stream")
            try:
                client.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=path.read_bytes(),
                    ContentType=content_type,
                    CacheControl="public, max-age=31536000, immutable",
                )
                # verify
                head = client.head_object(Bucket=bucket, Key=key)
                ok = int(head.get("ContentLength") or 0) == size
            except Exception as exc:  # noqa: BLE001 - report and continue
                failed += 1
                failed_names.append(rel)
                print(f"FAIL {rel}: {exc}")
                continue
            if not ok:
                failed += 1
                failed_names.append(rel)
                print(f"FAIL {rel}: size mismatch after upload")
                continue
            uploaded += 1
            action = "uploaded"

        print(f"{action:>14}  {rel}  ({size:,} bytes)")

        if already and args.delete_local:
            path.unlink()
            deleted += 1
            print(f"{'rm local':>14}  {rel}")
        elif action == "uploaded" and args.delete_local and not args.dry_run:
            path.unlink()
            deleted += 1
            print(f"{'rm local':>14}  {rel}")

    print("\nSummary:", {"uploaded": uploaded, "skipped_existing": skipped,
                         "failed": failed, "removed_local": deleted})
    if failed:
        print("Failed files:", ", ".join(failed_names))
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
