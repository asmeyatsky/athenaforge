from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from google.cloud import storage as gcs_storage


class GcsStorageAdapter:
    """Implements CloudStoragePort using Google Cloud Storage.

    Requires the ``google-cloud-storage`` package at runtime.
    """

    def __init__(self, project_id: str) -> None:
        self._project_id = project_id
        self._client: gcs_storage.Client | None = None

    def _get_client(self) -> gcs_storage.Client:
        if self._client is None:
            try:
                from google.cloud import storage as gcs_storage_mod
            except ImportError as exc:
                raise ImportError(
                    "google-cloud-storage is required for GcsStorageAdapter. "
                    "Install it with: pip install google-cloud-storage"
                ) from exc
            self._client = gcs_storage_mod.Client(project=self._project_id)
        return self._client

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    async def list_objects(self, bucket: str, prefix: str) -> list[str]:
        def _list():
            client = self._get_client()
            blobs = client.list_blobs(bucket, prefix=prefix)
            return [blob.name for blob in blobs]
        return await asyncio.to_thread(_list)

    async def read_object(self, bucket: str, key: str) -> bytes:
        def _read():
            client = self._get_client()
            blob = client.bucket(bucket).blob(key)
            return blob.download_as_bytes()
        return await asyncio.to_thread(_read)

    async def write_object(self, bucket: str, key: str, data: bytes) -> None:
        def _write():
            client = self._get_client()
            blob = client.bucket(bucket).blob(key)
            blob.upload_from_string(data)
        await asyncio.to_thread(_write)

    async def get_object_size(self, bucket: str, key: str) -> int:
        def _size():
            client = self._get_client()
            blob = client.bucket(bucket).blob(key)
            blob.reload()
            return blob.size or 0
        return await asyncio.to_thread(_size)
