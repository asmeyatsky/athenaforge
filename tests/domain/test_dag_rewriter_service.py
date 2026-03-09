"""Tests for DAGRewriterService — pure domain logic, no mocks."""
from __future__ import annotations

import pytest

from athenaforge.domain.services.dag_rewriter_service import DAGRewriterService


class TestOperatorReplacement:
    def test_athena_operator_replaced_with_bigquery(self):
        svc = DAGRewriterService()
        content = "task = AthenaOperator(task_id='run_query')"
        rewritten, changes = svc.rewrite(content)

        assert "BigQueryInsertJobOperator" in rewritten
        assert "AthenaOperator" not in rewritten
        assert any("AthenaOperator" in c for c in changes)

    def test_emr_operator_replaced_with_dataproc(self):
        svc = DAGRewriterService()
        content = "step = EmrAddStepsOperator(task_id='emr_step')"
        rewritten, changes = svc.rewrite(content)

        assert "DataprocSubmitJobOperator" in rewritten
        assert "EmrAddStepsOperator" not in rewritten
        assert any("EmrAddStepsOperator" in c for c in changes)

    def test_s3_key_sensor_replaced_with_gcs(self):
        svc = DAGRewriterService()
        content = "sensor = S3KeySensor(task_id='wait_for_file')"
        rewritten, changes = svc.rewrite(content)

        assert "GCSObjectExistenceSensor" in rewritten
        assert "S3KeySensor" not in rewritten
        assert any("S3KeySensor" in c for c in changes)


class TestStoragePathReplacement:
    def test_s3_path_replaced_with_gs(self):
        svc = DAGRewriterService()
        content = 'bucket = "s3://my-bucket/path/to/data"'
        rewritten, changes = svc.rewrite(content)

        assert "gs://my-bucket/path/to/data" in rewritten
        assert "s3://" not in rewritten
        assert any("s3://" in c for c in changes)

    def test_multiple_s3_paths_all_replaced(self):
        svc = DAGRewriterService()
        content = (
            'input = "s3://bucket-a/raw"\n'
            'output = "s3://bucket-b/processed"'
        )
        rewritten, changes = svc.rewrite(content)

        assert "s3://" not in rewritten
        assert "gs://bucket-a/raw" in rewritten
        assert "gs://bucket-b/processed" in rewritten


class TestNonAWSOperatorsPreserved:
    def test_python_operator_unchanged(self):
        svc = DAGRewriterService()
        content = "task = PythonOperator(task_id='my_task', python_callable=my_func)"
        rewritten, changes = svc.rewrite(content)

        assert rewritten == content
        assert changes == []

    def test_bash_operator_unchanged(self):
        svc = DAGRewriterService()
        content = "task = BashOperator(task_id='run_script', bash_command='echo hello')"
        rewritten, changes = svc.rewrite(content)

        assert rewritten == content
        assert changes == []


class TestImportStatementUpdates:
    def test_athena_import_replaced(self):
        svc = DAGRewriterService()
        content = "from airflow.providers.amazon.aws.operators.athena import AthenaOperator"
        rewritten, changes = svc.rewrite(content)

        assert "airflow.providers.google.cloud.operators.bigquery" in rewritten
        assert "airflow.providers.amazon.aws.operators.athena" not in rewritten

    def test_emr_import_replaced(self):
        svc = DAGRewriterService()
        content = "from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator"
        rewritten, changes = svc.rewrite(content)

        assert "airflow.providers.google.cloud.operators.dataproc" in rewritten
        assert "airflow.providers.amazon.aws.operators.emr" not in rewritten

    def test_s3_sensor_import_replaced(self):
        svc = DAGRewriterService()
        content = "from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor"
        rewritten, changes = svc.rewrite(content)

        assert "airflow.providers.google.cloud.sensors.gcs" in rewritten
        assert "airflow.providers.amazon.aws.sensors.s3" not in rewritten


class TestNoAWSReferencesUnchanged:
    def test_no_aws_references_returns_unchanged(self):
        svc = DAGRewriterService()
        content = (
            "from airflow import DAG\n"
            "from airflow.operators.python import PythonOperator\n"
            "\n"
            "dag = DAG('my_dag')\n"
            "task = PythonOperator(task_id='t1', python_callable=fn)\n"
        )
        rewritten, changes = svc.rewrite(content)

        assert rewritten == content
        assert changes == []

    def test_empty_content_returns_empty(self):
        svc = DAGRewriterService()
        rewritten, changes = svc.rewrite("")

        assert rewritten == ""
        assert changes == []


class TestFullDAGRewrite:
    def test_complete_dag_rewrite(self):
        svc = DAGRewriterService()
        content = (
            "from airflow.providers.amazon.aws.operators.athena import AthenaOperator\n"
            "from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor\n"
            "\n"
            "wait = S3KeySensor(task_id='wait', bucket_key='s3://bucket/key')\n"
            "query = AthenaOperator(task_id='run')\n"
        )
        rewritten, changes = svc.rewrite(content)

        assert "BigQueryInsertJobOperator" in rewritten
        assert "GCSObjectExistenceSensor" in rewritten
        assert "gs://bucket/key" in rewritten
        assert "AthenaOperator" not in rewritten
        assert "S3KeySensor" not in rewritten
        assert "s3://" not in rewritten
        assert len(changes) >= 3  # 2 imports, 2 operators, 1 storage path
