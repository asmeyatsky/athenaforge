from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import boto3


class S3StorageAdapter:
    """Implements CloudStoragePort using Amazon S3 via boto3.

    Requires the ``boto3`` package at runtime.
    """

    def __init__(self, region: str) -> None:
        self._region = region
        self._client: boto3.client | None = None

    def _get_client(self) -> boto3.client:
        if self._client is None:
            try:
                import boto3 as boto3_mod
            except ImportError as exc:
                raise ImportError(
                    "boto3 is required for S3StorageAdapter. "
                    "Install it with: pip install boto3"
                ) from exc
            self._client = boto3_mod.client("s3", region_name=self._region)
        return self._client

    async def list_objects(self, bucket: str, prefix: str) -> list[str]:
        client = self._get_client()
        response = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        return [obj["Key"] for obj in response.get("Contents", [])]

    async def read_object(self, bucket: str, key: str) -> bytes:
        client = self._get_client()
        response = client.get_object(Bucket=bucket, Key=key)
        return response["Body"].read()

    async def write_object(self, bucket: str, key: str, data: bytes) -> None:
        client = self._get_client()
        client.put_object(Bucket=bucket, Key=key, Body=data)

    async def get_object_size(self, bucket: str, key: str) -> int:
        client = self._get_client()
        response = client.head_object(Bucket=bucket, Key=key)
        return response["ContentLength"]
