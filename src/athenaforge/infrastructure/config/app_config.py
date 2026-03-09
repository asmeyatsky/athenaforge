from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import os
import yaml


@dataclass(frozen=True)
class AppConfig:
    gcp_project_id: str = ""
    gcp_location: str = "asia-south1"
    aws_region: str = "ap-south-1"
    data_dir: str = "./data"
    template_dir: str = ""
    pattern_dir: str = ""
    max_concurrency: int = 10

    @classmethod
    def from_env(cls) -> AppConfig:
        return cls(
            gcp_project_id=os.environ.get("GCP_PROJECT_ID", ""),
            gcp_location=os.environ.get("GCP_LOCATION", "asia-south1"),
            aws_region=os.environ.get("AWS_REGION", "ap-south-1"),
            data_dir=os.environ.get("ATHENAFORGE_DATA_DIR", "./data"),
            template_dir=os.environ.get(
                "ATHENAFORGE_TEMPLATE_DIR",
                str(Path(__file__).parent.parent / "templates"),
            ),
            pattern_dir=os.environ.get(
                "ATHENAFORGE_PATTERN_DIR",
                str(Path(__file__).parent.parent / "patterns"),
            ),
            max_concurrency=int(
                os.environ.get("ATHENAFORGE_MAX_CONCURRENCY", "10")
            ),
        )

    @classmethod
    def from_yaml(cls, path: str) -> AppConfig:
        with open(path) as f:
            data = yaml.safe_load(f) or {}
        return cls(**{k: v for k, v in data.items() if k in cls.__dataclass_fields__})
