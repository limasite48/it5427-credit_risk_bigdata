"""boto3 S3 client for MinIO (works with any S3-compatible endpoint)."""
from __future__ import annotations

import boto3
from botocore.client import Config


def make_s3_client(cfg: dict):
    m = cfg["minio"]
    return boto3.client(
        "s3",
        endpoint_url=m["endpoint"],
        aws_access_key_id=m["access_key"],
        aws_secret_access_key=m["secret_key"],
        region_name=m.get("region", "us-east-1"),
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )


def ensure_buckets(cfg: dict) -> list[str]:
    s3 = make_s3_client(cfg)
    existing = {b["Name"] for b in s3.list_buckets().get("Buckets", [])}
    created = []
    for bucket in cfg["lake"]["buckets"].values():
        if bucket not in existing:
            s3.create_bucket(Bucket=bucket)
            created.append(bucket)
    return created
