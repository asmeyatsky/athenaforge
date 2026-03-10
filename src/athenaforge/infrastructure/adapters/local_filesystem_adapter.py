from __future__ import annotations

import asyncio
import os
from glob import glob
from pathlib import Path


class LocalFilesystemAdapter:
    """Implements CloudStoragePort using the local filesystem.

    Maps ``bucket``/``key`` pairs to ``<base_dir>/<bucket>/<key>`` on disk.
    Useful for local development and testing without cloud credentials.
    """

    def __init__(self, base_dir: str) -> None:
        self._base_dir = base_dir

    def _resolve(self, bucket: str, key: str) -> Path:
        path = Path(self._base_dir) / bucket / key
        resolved = path.resolve()
        base_resolved = Path(self._base_dir).resolve()
        if not resolved.is_relative_to(base_resolved):
            raise ValueError(f"Path traversal detected: {bucket}/{key}")
        return resolved

    async def list_objects(self, bucket: str, prefix: str) -> list[str]:
        def _list():
            search_root = Path(self._base_dir) / bucket / prefix
            pattern = str(search_root) + "**/*" if str(search_root).endswith("/") else str(search_root) + "*"
            matches = glob(pattern, recursive=True)
            bucket_root = str(Path(self._base_dir) / bucket) + "/"
            return [
                m.replace(bucket_root, "")
                for m in matches
                if os.path.isfile(m)
            ]
        return await asyncio.to_thread(_list)

    async def read_object(self, bucket: str, key: str) -> bytes:
        def _read():
            path = self._resolve(bucket, key)
            return path.read_bytes()
        return await asyncio.to_thread(_read)

    async def write_object(self, bucket: str, key: str, data: bytes) -> None:
        def _write():
            path = self._resolve(bucket, key)
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(data)
        await asyncio.to_thread(_write)

    async def get_object_size(self, bucket: str, key: str) -> int:
        def _size():
            path = self._resolve(bucket, key)
            return os.path.getsize(path)
        return await asyncio.to_thread(_size)
