from __future__ import annotations

import asyncio
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

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    async def list_objects(self, bucket: str, prefix: str) -> list[str]:
        def _list():
            client = self._get_client()
            keys: list[str] = []
            continuation_token = None
            while True:
                kwargs = {"Bucket": bucket, "Prefix": prefix}
                if continuation_token:
                    kwargs["ContinuationToken"] = continuation_token
                response = client.list_objects_v2(**kwargs)
                for obj in response.get("Contents", []):
                    keys.append(obj["Key"])
                if not response.get("IsTruncated"):
                    break
                continuation_token = response.get("NextContinuationToken")
            return keys
        return await asyncio.to_thread(_list)

    async def read_object(self, bucket: str, key: str) -> bytes:
        def _read():
            client = self._get_client()
            response = client.get_object(Bucket=bucket, Key=key)
            return response["Body"].read()
        return await asyncio.to_thread(_read)

    async def write_object(self, bucket: str, key: str, data: bytes) -> None:
        def _write():
            client = self._get_client()
            client.put_object(Bucket=bucket, Key=key, Body=data)
        await asyncio.to_thread(_write)

    async def get_object_size(self, bucket: str, key: str) -> int:
        def _size():
            client = self._get_client()
            response = client.head_object(Bucket=bucket, Key=key)
            return response["ContentLength"]
        return await asyncio.to_thread(_size)
