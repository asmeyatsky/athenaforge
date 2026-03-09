from __future__ import annotations

from typing import Protocol


class ConfigPort(Protocol):
    """Port for loading and saving configuration manifests."""

    def load_manifest(self, path: str) -> dict: ...

    def save_manifest(self, path: str, data: dict) -> None: ...
