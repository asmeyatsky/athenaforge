"""Application-layer tests for Dependency module use cases."""

from __future__ import annotations

import pytest

from athenaforge.application.commands.dependency.scan_spark_flink_jobs import (
    ScanSparkFlinkJobsUseCase,
)
from athenaforge.application.commands.dependency.rewrite_dags import (
    RewriteDAGsUseCase,
)
from athenaforge.application.commands.dependency.migrate_kafka_topics import (
    MigrateKafkaTopicsUseCase,
)
from athenaforge.application.commands.dependency.rewrite_lambdas import (
    RewriteLambdasUseCase,
)
from athenaforge.application.commands.dependency.map_iam_permissions import (
    MapIAMPermissionsUseCase,
)
from athenaforge.domain.events.event_base import DomainEvent
from athenaforge.domain.events.dependency_events import (
    DAGRewriteCompleted,
    DependencyScanCompleted,
    IAMMappingGenerated,
    KafkaTopicMigrated,
    LambdaRewritten,
)
from athenaforge.domain.services.dag_rewriter_service import DAGRewriterService
from athenaforge.domain.services.dependency_scanner import DependencyScanner


# ── Stub ports ───────────────────────────────────────────────────────────────


class StubEventBus:
    """Collects all published events."""

    def __init__(self) -> None:
        self.events: list[DomainEvent] = []

    async def publish(self, event: DomainEvent) -> None:
        self.events.append(event)

    def subscribe(self, event_type: type, handler: object) -> None:
        pass


class StubCloudStoragePort:
    """In-memory cloud storage stub."""

    def __init__(
        self,
        objects: dict[str, bytes] | None = None,
        sizes: dict[str, int] | None = None,
    ) -> None:
        self._objects = objects or {}
        self._sizes = sizes or {}

    async def list_objects(self, bucket: str, prefix: str = "") -> list[str]:
        return [k for k in self._objects if k.startswith(prefix)]

    async def read_object(self, bucket: str, key: str) -> bytes:
        return self._objects.get(key, b"")

    async def write_object(self, bucket: str, key: str, data: bytes) -> None:
        self._objects[key] = data

    async def get_object_size(self, bucket: str, key: str) -> int:
        return self._sizes.get(key, len(self._objects.get(key, b"")))


# ── ScanSparkFlinkJobsUseCase ────────────────────────────────────────────────


async def test_scan_detects_s3_path_references_in_spark_jobs():
    """S3 path references in source code should be detected as SPARK jobs."""
    spark_code = b"""
from pyspark.sql import SparkSession
df = spark.read.parquet("s3://my-bucket/data/table_a")
df.write.parquet("s3://my-bucket/output/table_b")
"""
    objects = {"jobs/spark_etl.py": spark_code}
    storage = StubCloudStoragePort(objects=objects)
    scanner = DependencyScanner()
    bus = StubEventBus()

    uc = ScanSparkFlinkJobsUseCase(scanner, storage, bus)
    report = await uc.execute("my-bucket", ["jobs/"])

    assert report.spark_jobs >= 1
    assert report.total_references >= 2  # two s3:// paths


async def test_scan_detects_athena_operator_in_dag_files():
    """AthenaOperator references should be detected as AIRFLOW_DAG jobs."""
    dag_code = b"""
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
task = AthenaOperator(task_id='run_query', query='SELECT 1')
"""
    objects = {"dags/my_dag.py": dag_code}
    storage = StubCloudStoragePort(objects=objects)
    scanner = DependencyScanner()
    bus = StubEventBus()

    uc = ScanSparkFlinkJobsUseCase(scanner, storage, bus)
    report = await uc.execute("bucket", ["dags/"])

    assert report.dags >= 1
    assert report.total_references >= 1


async def test_scan_returns_correct_job_type_counts():
    """Each file's job type should be counted correctly."""
    spark_file = b'df = spark.read.parquet("s3://bucket/path")'
    dag_file = b"task = AthenaOperator(task_id='t1')"
    lambda_file = b"client = boto3.client('athena')"

    objects = {
        "jobs/spark.py": spark_file,
        "dags/dag.py": dag_file,
        "lambdas/fn.py": lambda_file,
    }
    storage = StubCloudStoragePort(objects=objects)
    scanner = DependencyScanner()
    bus = StubEventBus()

    uc = ScanSparkFlinkJobsUseCase(scanner, storage, bus)
    # Use a prefix that matches all keys
    report = await uc.execute("bucket", ["jobs/", "dags/", "lambdas/"])

    assert report.spark_jobs >= 1
    assert report.dags >= 1
    assert report.lambdas >= 1


async def test_scan_publishes_dependency_scan_completed_event():
    """A DependencyScanCompleted event should be published after the scan."""
    objects = {"code/app.py": b'path = "s3://bucket/data"'}
    storage = StubCloudStoragePort(objects=objects)
    scanner = DependencyScanner()
    bus = StubEventBus()

    uc = ScanSparkFlinkJobsUseCase(scanner, storage, bus)
    await uc.execute("bucket", ["code/"])

    scan_events = [
        e for e in bus.events if isinstance(e, DependencyScanCompleted)
    ]
    assert len(scan_events) == 1
    assert scan_events[0].spark_jobs >= 1


async def test_scan_handles_empty_prefix_list():
    """An empty prefix list should produce zero counts and still publish an event."""
    storage = StubCloudStoragePort()
    scanner = DependencyScanner()
    bus = StubEventBus()

    uc = ScanSparkFlinkJobsUseCase(scanner, storage, bus)
    report = await uc.execute("bucket", [])

    assert report.spark_jobs == 0
    assert report.flink_jobs == 0
    assert report.dags == 0
    assert report.lambdas == 0
    assert report.total_references == 0
    # Event is still published
    scan_events = [
        e for e in bus.events if isinstance(e, DependencyScanCompleted)
    ]
    assert len(scan_events) == 1


# ── RewriteDAGsUseCase ──────────────────────────────────────────────────────


async def test_rewrite_dags_rewrites_athena_to_bigquery():
    """AthenaOperator should be rewritten to BigQueryInsertJobOperator."""
    rewriter = DAGRewriterService()
    bus = StubEventBus()

    dag_content = (
        "from airflow.providers.amazon.aws.operators.athena import AthenaOperator\n"
        "task = AthenaOperator(task_id='run_query')\n"
    )

    uc = RewriteDAGsUseCase(rewriter, bus)
    report = await uc.execute({"dag.py": dag_content})

    assert report.dags_processed == 1
    assert report.dags_rewritten == 1
    assert report.operators_replaced >= 1
    # Verify a change mentions BigQuery
    change_texts = [c["change"] for c in report.changes]
    assert any("BigQueryInsertJobOperator" in ch for ch in change_texts)


async def test_rewrite_dags_preserves_dags_without_aws_operators():
    """DAGs that have no AWS operators should not be rewritten."""
    rewriter = DAGRewriterService()
    bus = StubEventBus()

    dag_content = (
        "from airflow.operators.python import PythonOperator\n"
        "task = PythonOperator(task_id='my_task', python_callable=fn)\n"
    )

    uc = RewriteDAGsUseCase(rewriter, bus)
    report = await uc.execute({"clean_dag.py": dag_content})

    assert report.dags_processed == 1
    assert report.dags_rewritten == 0
    assert report.operators_replaced == 0


async def test_rewrite_dags_counts_operators_replaced_correctly():
    """Multiple AWS operators in the same file should all be counted."""
    rewriter = DAGRewriterService()
    bus = StubEventBus()

    dag_content = (
        "from airflow.providers.amazon.aws.operators.athena import AthenaOperator\n"
        "from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor\n"
        "task1 = AthenaOperator(task_id='t1')\n"
        "sensor = S3KeySensor(task_id='s1', bucket_key='s3://bucket/key')\n"
    )

    uc = RewriteDAGsUseCase(rewriter, bus)
    report = await uc.execute({"multi_op.py": dag_content})

    assert report.dags_processed == 1
    assert report.dags_rewritten == 1
    # At minimum: import replacement + operator replacement + s3->gs
    assert report.operators_replaced >= 3


async def test_rewrite_dags_publishes_dag_rewrite_completed_event():
    """A DAGRewriteCompleted event should be published."""
    rewriter = DAGRewriterService()
    bus = StubEventBus()

    dag_content = "task = AthenaOperator(task_id='t1')\n"

    uc = RewriteDAGsUseCase(rewriter, bus)
    await uc.execute({"dag.py": dag_content})

    rewrite_events = [
        e for e in bus.events if isinstance(e, DAGRewriteCompleted)
    ]
    assert len(rewrite_events) == 1
    assert rewrite_events[0].dags_rewritten == 1


async def test_rewrite_dags_handles_empty_dict():
    """An empty DAG dict should produce zero counts."""
    rewriter = DAGRewriterService()
    bus = StubEventBus()

    uc = RewriteDAGsUseCase(rewriter, bus)
    report = await uc.execute({})

    assert report.dags_processed == 0
    assert report.dags_rewritten == 0
    assert report.operators_replaced == 0
    assert report.changes == []
    # Event is still published
    rewrite_events = [
        e for e in bus.events if isinstance(e, DAGRewriteCompleted)
    ]
    assert len(rewrite_events) == 1


# ── MigrateKafkaTopicsUseCase ────────────────────────────────────────────────


async def test_migrate_kafka_topics_with_schema():
    """Topics that include a schema field should be counted in schemas_updated."""
    bus = StubEventBus()

    uc = MigrateKafkaTopicsUseCase(bus)
    report = await uc.execute([
        {"topic": "events.user_login", "schema": "avro://schema-registry/user_login"},
        {"topic": "events.page_view", "schema": "avro://schema-registry/page_view"},
    ])

    assert report.topics_migrated == 2
    assert report.schemas_updated == 2
    assert len(report.details) == 2


async def test_migrate_kafka_topics_returns_correct_counts():
    """A mix of topics with and without schemas should have correct counts."""
    bus = StubEventBus()

    uc = MigrateKafkaTopicsUseCase(bus)
    report = await uc.execute([
        {"topic": "topic-a", "schema": "avro://schema"},
        {"topic": "topic-b"},  # no schema
        {"topic": "topic-c"},  # no schema
    ])

    assert report.topics_migrated == 3
    assert report.schemas_updated == 1


async def test_migrate_kafka_topics_publishes_event():
    """A KafkaTopicMigrated event should be published."""
    bus = StubEventBus()

    uc = MigrateKafkaTopicsUseCase(bus)
    await uc.execute([{"topic": "t1"}])

    kafka_events = [
        e for e in bus.events if isinstance(e, KafkaTopicMigrated)
    ]
    assert len(kafka_events) == 1
    assert kafka_events[0].topics_migrated == 1


# ── RewriteLambdasUseCase ────────────────────────────────────────────────────


async def test_rewrite_lambdas_detects_athena_patterns():
    """Lambda functions with boto3 Athena client calls should be flagged for rewrite."""
    scanner = DependencyScanner()
    bus = StubEventBus()

    lambda_source = """
import boto3
client = boto3.client('athena')
response = client.start_query_execution(QueryString='SELECT 1')
"""

    uc = RewriteLambdasUseCase(scanner, bus)
    report = await uc.execute({"athena_handler": lambda_source})

    assert report.functions_processed == 1
    assert report.functions_rewritten == 1
    assert report.details[0]["status"] == "needs_rewrite"


async def test_rewrite_lambdas_preserves_lambda_without_athena():
    """Lambda functions without Athena patterns should not be rewritten."""
    scanner = DependencyScanner()
    bus = StubEventBus()

    lambda_source = """
import boto3
client = boto3.client('s3')
response = client.get_object(Bucket='my-bucket', Key='data.csv')
"""

    uc = RewriteLambdasUseCase(scanner, bus)
    report = await uc.execute({"s3_handler": lambda_source})

    assert report.functions_processed == 1
    assert report.functions_rewritten == 0
    assert report.details[0]["status"] == "no_athena_references"


async def test_rewrite_lambdas_publishes_lambda_rewritten_event():
    """A LambdaRewritten event should be published."""
    scanner = DependencyScanner()
    bus = StubEventBus()

    lambda_source = "client = boto3.client('athena')"

    uc = RewriteLambdasUseCase(scanner, bus)
    await uc.execute({"fn1": lambda_source})

    lambda_events = [
        e for e in bus.events if isinstance(e, LambdaRewritten)
    ]
    assert len(lambda_events) == 1
    assert lambda_events[0].functions_rewritten == 1


# ── MapIAMPermissionsUseCase ─────────────────────────────────────────────────


async def test_map_iam_select_to_data_viewer():
    """SELECT permission should map to roles/bigquery.dataViewer."""
    bus = StubEventBus()

    uc = MapIAMPermissionsUseCase(bus)
    report = await uc.execute([
        {"permission": "SELECT", "resource": "db.table1", "principal": "user@example.com"},
    ])

    assert report.policies_mapped == 1
    assert report.mappings[0]["bigquery_role"] == "roles/bigquery.dataViewer"


async def test_map_iam_insert_to_data_editor():
    """INSERT permission should map to roles/bigquery.dataEditor."""
    bus = StubEventBus()

    uc = MapIAMPermissionsUseCase(bus)
    report = await uc.execute([
        {"permission": "INSERT", "resource": "db.table1", "principal": "user@example.com"},
    ])

    assert report.policies_mapped == 1
    assert report.mappings[0]["bigquery_role"] == "roles/bigquery.dataEditor"


async def test_map_iam_all_to_data_owner():
    """ALL permission should map to roles/bigquery.dataOwner."""
    bus = StubEventBus()

    uc = MapIAMPermissionsUseCase(bus)
    report = await uc.execute([
        {"permission": "ALL", "resource": "db.table1", "principal": "admin@example.com"},
    ])

    assert report.policies_mapped == 1
    assert report.mappings[0]["bigquery_role"] == "roles/bigquery.dataOwner"


async def test_map_iam_publishes_event():
    """An IAMMappingGenerated event should be published."""
    bus = StubEventBus()

    uc = MapIAMPermissionsUseCase(bus)
    await uc.execute([
        {"permission": "SELECT", "resource": "db.t1", "principal": "user@example.com"},
        {"permission": "INSERT", "resource": "db.t2", "principal": "user@example.com"},
    ])

    iam_events = [
        e for e in bus.events if isinstance(e, IAMMappingGenerated)
    ]
    assert len(iam_events) == 1
    assert iam_events[0].policies_mapped == 2


async def test_map_iam_unknown_permission_defaults_to_data_viewer():
    """An unknown permission should default to roles/bigquery.dataViewer."""
    bus = StubEventBus()

    uc = MapIAMPermissionsUseCase(bus)
    report = await uc.execute([
        {"permission": "EXECUTE", "resource": "db.proc1", "principal": "user@example.com"},
    ])

    assert report.policies_mapped == 1
    assert report.mappings[0]["bigquery_role"] == "roles/bigquery.dataViewer"
    assert report.mappings[0]["lake_formation_permission"] == "EXECUTE"
