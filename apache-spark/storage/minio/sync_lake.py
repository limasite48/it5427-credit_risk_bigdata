"""Mirror the local lake (data/bronze|silver|gold) into the MinIO buckets bronze|silver|gold.

Used when lake.backend=local (Spark writes to disk, MinIO is the browsable Data Lake). Incremental:
an object is uploaded only when its size or local mtime differs from what MinIO already holds
(kept in object metadata), so re-running is cheap and never duplicates objects. Spark's
`_SUCCESS`/`.crc` side files and in-flight `.tmp` files are skipped.

    python -m storage.minio.sync_lake                 # all layers
    python -m storage.minio.sync_lake --layers gold --delete   # also remove objects deleted locally
"""
from __future__ import annotations

import argparse
import json
import time
from pathlib import Path

from common.config import load_config
from common.lake import local_layer_dir
from common.logging_utils import get_logger
from storage.minio.client import ensure_buckets, make_s3_client

log = get_logger("minio-sync")
SKIP_SUFFIXES = (".crc", ".tmp")


def _remote_index(s3, bucket: str) -> dict[str, dict]:
    idx = {}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket):
        for o in page.get("Contents", []):
            idx[o["Key"]] = {"size": o["Size"]}
    return idx


def sync_layer(s3, layer: str, bucket: str, delete: bool = False, cfg: dict | None = None) -> dict:
    root = local_layer_dir(layer, cfg)
    remote = _remote_index(s3, bucket)
    uploaded = skipped = deleted = bytes_up = 0
    local_keys = set()
    for p in sorted(root.rglob("*")):
        if not p.is_file() or p.name.endswith(SKIP_SUFFIXES) or p.name == ".gitkeep":
            continue
        key = p.relative_to(root).as_posix()
        local_keys.add(key)
        st = p.stat()
        if key in remote and remote[key]["size"] == st.st_size:
            head = s3.head_object(Bucket=bucket, Key=key)
            if head.get("Metadata", {}).get("local-mtime") == str(int(st.st_mtime)):
                skipped += 1
                continue
        s3.upload_file(str(p), bucket, key, ExtraArgs={"Metadata": {"local-mtime": str(int(st.st_mtime))}})
        uploaded += 1
        bytes_up += st.st_size
    if delete:
        for key in set(remote) - local_keys:
            s3.delete_object(Bucket=bucket, Key=key)
            deleted += 1
    return {"layer": layer, "bucket": bucket, "uploaded": uploaded, "skipped_unchanged": skipped,
            "deleted": deleted, "bytes_uploaded": bytes_up}


def run(layers=("bronze", "silver", "gold"), delete: bool = False) -> list[dict]:
    cfg = load_config()
    created = ensure_buckets(cfg)
    if created:
        log.info("created buckets %s", created)
    s3 = make_s3_client(cfg)
    out = []
    for layer in layers:
        t0 = time.time()
        r = sync_layer(s3, layer, cfg["lake"]["buckets"][layer], delete, cfg)
        r["seconds"] = round(time.time() - t0, 2)
        log.info("sync %s", r)
        out.append(r)
    return out


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--layers", nargs="*", default=["bronze", "silver", "gold"])
    ap.add_argument("--delete", action="store_true")
    a = ap.parse_args()
    print(json.dumps(run(a.layers, a.delete), indent=2))
