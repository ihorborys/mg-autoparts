import os
from typing import Optional
import boto3
from botocore.client import Config


class StorageClient:
    def __init__(self):
        self.bucket = os.getenv("R2_BUCKET")
        self.public_base = (os.getenv("R2_PUBLIC_BASE") or "").rstrip("/")
        self.s3 = boto3.client(
            "s3",
            endpoint_url=os.getenv("R2_ENDPOINT"),
            aws_access_key_id=os.getenv("R2_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("R2_SECRET_ACCESS_KEY"),
            config=Config(signature_version="s3v4"),
            region_name="auto",
        )

    def latest_key(self, prefix: str) -> Optional[str]:
        resp = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
        items = resp.get("Contents", [])
        if not items:
            return None
        return max(items, key=lambda o: o["LastModified"])["Key"]

    def url_for(self, key: Optional[str], expires_sec: int = 3600) -> Optional[str]:
        if not key:
            return None
        if self.public_base:
            return f"{self.public_base}/{key}"
        return self.s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": self.bucket, "Key": key},
            ExpiresIn=expires_sec,
        )

    def upload_file(self, local_path: str, key: str, content_type: Optional[str] = None) -> str:
        extra = {"ContentType": content_type} if content_type else None
        self.s3.upload_file(local_path, self.bucket, key, ExtraArgs=extra)
        return self.url_for(key)
