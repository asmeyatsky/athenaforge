from __future__ import annotations

import yaml
from pathlib import Path

from athenaforge.domain.ports.config_port import ConfigPort


class YamlConfigAdapter:
    """Implements ConfigPort using PyYAML."""

    def load_manifest(self, path: str) -> dict:
        with open(path, "r") as f:
            return yaml.safe_load(f) or {}

    def save_manifest(self, path: str, data: dict) -> None:
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            yaml.dump(data, f, default_flow_style=False, sort_keys=False)
