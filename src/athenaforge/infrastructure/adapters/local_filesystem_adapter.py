from __future__ import annotations

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
        return Path(self._base_dir) / bucket / key

    async def list_objects(self, bucket: str, prefix: str) -> list[str]:
        search_root = Path(self._base_dir) / bucket / prefix
        pattern = str(search_root) + "**/*" if str(search_root).endswith("/") else str(search_root) + "*"
        matches = glob(pattern, recursive=True)
        bucket_root = str(Path(self._base_dir) / bucket) + "/"
        return [
            m.replace(bucket_root, "")
            for m in matches
            if os.path.isfile(m)
        ]

    async def read_object(self, bucket: str, key: str) -> bytes:
        path = self._resolve(bucket, key)
        return path.read_bytes()

    async def write_object(self, bucket: str, key: str, data: bytes) -> None:
        path = self._resolve(bucket, key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(data)

    async def get_object_size(self, bucket: str, key: str) -> int:
        path = self._resolve(bucket, key)
        return os.path.getsize(path)
