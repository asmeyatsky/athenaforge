from __future__ import annotations

from datetime import datetime, timedelta
from decimal import Decimal
from pathlib import Path

import pytest

from athenaforge.domain.entities.table_inventory import TableEntry, TableInventory
from athenaforge.domain.value_objects.cost import Currency, Money
from athenaforge.domain.value_objects.lob import LOB, LOBManifest
from athenaforge.domain.value_objects.sql_pattern import (
    PatternCategory,
    PatternExample,
    SqlTranslationPattern,
)
from athenaforge.domain.value_objects.tier import Tier, TierClassification
from athenaforge.infrastructure.config import AppConfig, DependencyContainer


@pytest.fixture
def sample_lob():
    return LOB(
        name="payments",
        owner="payments-team",
        datasets=["raw_payments", "curated_payments"],
    )


@pytest.fixture
def sample_table_entry():
    return TableEntry(
        table_name="transactions",
        database="payments_db",
        size_bytes=500_000_000_000,  # 500GB
        row_count=1_000_000_000,
        last_queried=datetime.utcnow() - timedelta(days=30),
        partitioned=True,
        format="PARQUET",
        has_maps=False,
    )


@pytest.fixture
def sample_tier_classification():
    return TierClassification(
        table_name="transactions",
        tier=Tier.TIER_1,
        reason="Active and < 1TB",
        size_bytes=500_000_000_000,
        last_queried_days_ago=30,
    )


@pytest.fixture
def sample_translation_pattern():
    return SqlTranslationPattern(
        name="try_cast_to_safe_cast",
        category=PatternCategory.TRY_CAST,
        description="Convert TRY_CAST to SAFE_CAST",
        presto_pattern=r"TRY_CAST\((.+?)\s+AS\s+(.+?)\)",
        googlesql_replacement=r"SAFE_CAST(\1 AS \2)",
        examples=(
            PatternExample(
                presto_sql="SELECT TRY_CAST(x AS INT)",
                googlesql="SELECT SAFE_CAST(x AS INT)",
            ),
        ),
    )


@pytest.fixture
def app_config(tmp_path):
    return AppConfig(
        gcp_project_id="test-project",
        gcp_location="asia-south1",
        aws_region="ap-south-1",
        data_dir=str(tmp_path / "data"),
        template_dir=str(
            Path(__file__).parent.parent
            / "src"
            / "athenaforge"
            / "infrastructure"
            / "templates"
        ),
        pattern_dir=str(
            Path(__file__).parent.parent
            / "src"
            / "athenaforge"
            / "infrastructure"
            / "patterns"
        ),
        max_concurrency=5,
    )


@pytest.fixture
def container(app_config):
    return DependencyContainer(app_config)
