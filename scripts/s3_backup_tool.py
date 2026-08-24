#!/usr/bin/env python
"""S3 helper for DB backups - runs INSIDE the whiteboard container (boto3 is
already installed there), so the host needs no awscli.

Reads config from env: PG_BACKUP_S3_PREFIX, AWS_ENDPOINT, AWS_REGION,
AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY (all come from .env.production via
compose env_file).

Subcommands:
  put KEY        object body from stdin; verifies size via head after upload
  cat KEY        stream object to stdout (for restore)
  prune --days N delete objects under the prefix older than N days
  ls             list objects under the prefix
"""

import argparse
import os
import sys
import time


def _cfg():
    prefix = os.environ.get("PG_BACKUP_S3_PREFIX", "").rstrip("/")
    endpoint = os.environ.get("AWS_ENDPOINT")
    keys = (os.environ.get("AWS_ACCESS_KEY_ID"), os.environ.get("AWS_SECRET_ACCESS_KEY"))
    if not prefix.startswith("s3://") or not endpoint or not all(keys):
        sys.exit("[s3tool] PG_BACKUP_S3_PREFIX / AWS_ENDPOINT / AWS_ACCESS_KEY_ID / "
                 "AWS_SECRET_ACCESS_KEY must be set (see .env.production)")
    bucket = prefix[5:].split("/", 1)[0]
    base_key = prefix[5:].split("/", 1)[1] if "/" in prefix[5:] else ""
    import boto3

    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        region_name=os.environ.get("AWS_REGION", "ru-central1"),
        aws_access_key_id=keys[0],
        aws_secret_access_key=keys[1],
    )
    return client, bucket, base_key


def cmd_put(key: str) -> int:
    client, bucket, base = _cfg()
    data = sys.stdin.buffer.read()
    if len(data) < 1000:
        sys.exit(f"[s3tool] stdin suspiciously small ({len(data)} bytes) - aborting")
    full = f"{base}/{key}" if base else key
    client.put_object(
        Bucket=bucket, Key=full, Body=data,
        ContentType="application/gzip",
        CacheControl="no-store",
    )
    head = client.head_object(Bucket=bucket, Key=full)
    remote = int(head.get("ContentLength") or 0)
    if remote != len(data):
        sys.exit(f"[s3tool] size mismatch after upload ({remote} != {len(data)})")
    print(f"[s3tool] uploaded s3://{bucket}/{full} ({remote:,} bytes, verified)")
    return 0


def cmd_cat(key: str) -> int:
    client, bucket, base = _cfg()
    full = f"{base}/{key}" if base else key
    obj = client.get_object(Bucket=bucket, Key=full)
    for chunk in iter(lambda: obj["Body"].read(1024 * 512), b""):
        sys.stdout.buffer.write(chunk)
    sys.stdout.buffer.flush()
    return 0


def cmd_prune(days: int) -> int:
    client, bucket, base = _cfg()
    cutoff = time.time() - days * 86400
    pruned = 0
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{base}/" if base else ""):
        for obj in page.get("Contents", []):
            if obj["LastModified"].timestamp() < cutoff:
                client.delete_object(Bucket=bucket, Key=obj["Key"])
                print(f"[s3tool] pruned s3://{bucket}/{obj['Key']}")
                pruned += 1
    print(f"[s3tool] retention {days}d: pruned {pruned} object(s)")
    return 0


def cmd_ls() -> int:
    client, bucket, base = _cfg()
    paginator = client.get_paginator("list_objects_v2")
    total = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=f"{base}/" if base else ""):
        for obj in page.get("Contents", []):
            print(f"{obj['LastModified'].isoformat()}  {obj['Size']:>12,}  {obj['Key']}")
            total += 1
    print(f"[s3tool] {total} object(s)")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="cmd", required=True)
    p_put = sub.add_parser("put"); p_put.add_argument("key")
    p_cat = sub.add_parser("cat"); p_cat.add_argument("key")
    p_prune = sub.add_parser("prune"); p_prune.add_argument("--days", type=int, required=True)
    sub.add_parser("ls")
    args = parser.parse_args()

    if args.cmd == "put":
        return cmd_put(args.key)
    if args.cmd == "cat":
        return cmd_cat(args.key)
    if args.cmd == "prune":
        if args.days < 1:
            sys.exit("[s3tool] --days must be >= 1")
        return cmd_prune(args.days)
    return cmd_ls()


if __name__ == "__main__":
    raise SystemExit(main())
