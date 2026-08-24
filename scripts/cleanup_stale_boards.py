#!/usr/bin/env python
"""Delete boards that have not been touched for a long time (default 90 days).

Criterion: boards.updated_at < now - days. updated_at is the time of the last
MODIFICATION (op, baseline save, clear, owner/member change) - board views are
deliberately not tracked (no write per connect), so a board someone only
looked at still ages out. Tune STALE_BOARD_DAYS if that is too aggressive.

Deletion cascades to board_ops and board_members (FK ON DELETE CASCADE) and
also removes the boards' media: S3 objects under MEDIA_S3_LOCATION/{board_id}/
and local files under UPLOAD_DIR/{board_id}/, plus the Redis canvas cache key.

Dry-run by default - pass --yes to actually delete. Designed for a weekly
host-cron run, e.g.:

    0 4 * * 0 cd /opt/collaborative-whiteboard && docker compose \
      --env-file .env.production -f docker-compose.prod.yml exec -T whiteboard \
      python scripts/cleanup_stale_boards.py --yes >> /var/log/whiteboard-cleanup.log 2>&1
"""

import argparse
import asyncio
import os
import shutil
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--yes", action="store_true",
                        help="actually delete (default: dry-run)")
    parser.add_argument("--days", type=int,
                        default=int(os.getenv("STALE_BOARD_DAYS", "90")),
                        help="age threshold in days (default STALE_BOARD_DAYS=90)")
    parser.add_argument("--limit", type=int, default=500,
                        help="max boards per run (keeps transactions small)")
    args = parser.parse_args()

    if args.days < 1:
        parser.error("--days must be >= 1 (refusing to delete everything)")

    # Reuse the app's DB wrapper (SQLite/Postgres agnostic) and constants.
    from app.main import UPLOAD_DIR, get_db

    cutoff = (datetime.now(timezone.utc) - timedelta(days=args.days)).isoformat()

    conn = get_db()
    try:
        rows = conn.execute(
            "SELECT board_id, owner_id, updated_at FROM boards "
            "WHERE updated_at < ? ORDER BY updated_at ASC LIMIT ?",
            (cutoff, args.limit),
        ).fetchall()
    finally:
        conn.close()

    if not rows:
        print(f"[cleanup] no boards older than {args.days} days - nothing to do")
        return 0

    ids = [r["board_id"] for r in rows]
    mode = "DELETING" if args.yes else "DRY-RUN (would delete)"
    print(f"[cleanup] {mode} {len(ids)} board(s) with updated_at < {cutoff}")
    for r in rows:
        print(f"  - {r['board_id']}  owner={r['owner_id']}  last_update={r['updated_at']}")

    if not args.yes:
        print("[cleanup] dry-run only - re-run with --yes to delete")
        return 0

    # 1) Media in S3. Runs BEFORE the row delete; boards whose media cleanup
    #    fails are SKIPPED this run (rows stay) so storage never becomes an
    #    orphan - the next run retries them.
    from app.media.s3_storage import S3_LOCATION, get_client, s3_enabled

    s3_failed: set[str] = set()
    if s3_enabled():
        client = get_client()
        bucket = os.environ["MEDIA_S3_BUCKET"]
        for bid in ids:
            prefix = f"{S3_LOCATION}/{bid}/"
            try:
                resp = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
                objects = resp.get("Contents", [])
                if objects:
                    client.delete_objects(
                        Bucket=bucket,
                        Delete={"Objects": [{"Key": o["Key"]} for o in objects[:1000]],
                                "Quiet": True},
                    )
                    print(f"  s3: removed {len(objects)} object(s) under {prefix}")
            except Exception as exc:  # noqa: BLE001
                s3_failed.add(bid)
                print(f"  WARN: s3 cleanup failed for {prefix}: {exc} - board skipped this run")

    deletable = [b for b in ids if b not in s3_failed]
    if s3_failed:
        print(f"[cleanup] deferring {len(s3_failed)} board(s) with failed media cleanup: "
              f"{', '.join(sorted(s3_failed))}")

    # 2) Local media dirs (pre-migration files and dev leftovers).
    removed_dirs = 0
    for bid in deletable:
        board_dir = Path(UPLOAD_DIR) / bid
        if board_dir.is_dir():
            shutil.rmtree(board_dir, ignore_errors=True)
            removed_dirs += 1

    # 3) Redis canvas cache keys (best-effort).
    try:
        from app.main import redis_client

        async def _del_cache():
            await asyncio.gather(
                *(redis_client.delete(f"wb:canvas:{bid}") for bid in deletable),
                return_exceptions=True,
            )

        asyncio.run(_del_cache())
    except Exception as exc:  # noqa: BLE001
        print(f"  WARN: redis cache cleanup skipped: {exc}")

    # 4) DB rows (ops/members cascade at FK level).
    conn = get_db()
    deleted = 0
    try:
        for i in range(0, len(deletable), 200):
            chunk = deletable[i:i + 200]
            placeholders = ",".join("?" * len(chunk))
            conn.execute(f"DELETE FROM boards WHERE board_id IN ({placeholders})", tuple(chunk))
            conn.commit()
            deleted += len(chunk)
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    print(f"[cleanup] deleted boards: {deleted}; local media dirs removed: {removed_dirs}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
