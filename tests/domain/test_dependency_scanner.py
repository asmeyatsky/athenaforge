"""Tests for DependencyScanner — pure domain logic, no mocks."""
from __future__ import annotations

import pytest

from athenaforge.domain.services.dependency_scanner import DependencyScanner
from athenaforge.domain.value_objects.dependency_ref import JobType


class TestS3PathDetection:
    def test_detects_s3_path(self):
        scanner = DependencyScanner()
        source = 'path = "s3://my-bucket/data/file.parquet"'
        results = scanner.scan(source, "dag.py")

        assert len(results) == 1
        assert results[0].job_type == JobType.SPARK
        assert "s3://my-bucket/data/file.parquet" in results[0].references

    def test_detects_multiple_s3_paths(self):
        scanner = DependencyScanner()
        source = (
            'input = "s3://bucket-a/raw"\n'
            'output = "s3://bucket-b/processed"'
        )
        results = scanner.scan(source, "etl.py")

        s3_refs = [r for r in results if r.job_type == JobType.SPARK]
        assert len(s3_refs) == 1
        assert len(s3_refs[0].references) == 2


class TestAthenaOperatorDetection:
    def test_detects_athena_operator(self):
        scanner = DependencyScanner()
        source = "task = AthenaOperator(task_id='run_query')"
        results = scanner.scan(source, "dag.py")

        athena_refs = [r for r in results if r.job_type == JobType.AIRFLOW_DAG]
        assert len(athena_refs) == 1
        assert "AthenaOperator" in athena_refs[0].references

    def test_detects_aws_athena_operator(self):
        scanner = DependencyScanner()
        source = "task = AwsAthenaOperator(task_id='q1')"
        results = scanner.scan(source, "dag.py")

        athena_refs = [r for r in results if r.job_type == JobType.AIRFLOW_DAG]
        assert len(athena_refs) == 1
        assert "AwsAthenaOperator" in athena_refs[0].references


class TestMapTypeDetection:
    def test_detects_maptype_pattern(self):
        scanner = DependencyScanner()
        source = "schema = StructField('data', MapType(StringType(), IntegerType())) # map"
        results = scanner.scan(source, "spark_job.py")

        spark_map_refs = [
            r for r in results
            if r.job_type == JobType.SPARK and any("MapType" in ref or "map" in ref.lower() for ref in r.references)
        ]
        assert len(spark_map_refs) >= 1


class TestBoto3AthenaClientDetection:
    def test_detects_boto3_athena_client(self):
        scanner = DependencyScanner()
        source = "client = boto3.client('athena')"
        results = scanner.scan(source, "lambda.py")

        lambda_refs = [r for r in results if r.job_type == JobType.LAMBDA]
        assert len(lambda_refs) == 1
        assert "boto3.client('athena')" in lambda_refs[0].references

    def test_detects_boto3_athena_client_double_quotes(self):
        scanner = DependencyScanner()
        source = 'client = boto3.client("athena")'
        results = scanner.scan(source, "lambda.py")

        lambda_refs = [r for r in results if r.job_type == JobType.LAMBDA]
        assert len(lambda_refs) == 1


class TestNoMatches:
    def test_no_matches_returns_empty_list(self):
        scanner = DependencyScanner()
        source = "print('hello world')\nx = 1 + 2"
        results = scanner.scan(source, "clean_code.py")

        assert results == []

    def test_empty_string_returns_empty_list(self):
        scanner = DependencyScanner()
        results = scanner.scan("", "empty.py")

        assert results == []


class TestMultiplePatternsInOneFile:
    def test_detects_s3_and_athena_operator(self):
        scanner = DependencyScanner()
        source = (
            "from airflow.providers.amazon.aws.operators.athena import AthenaOperator\n"
            'data_path = "s3://my-bucket/data"\n'
            "task = AthenaOperator(task_id='run')\n"
        )
        results = scanner.scan(source, "mixed_dag.py")

        job_types = {r.job_type for r in results}
        assert JobType.SPARK in job_types
        assert JobType.AIRFLOW_DAG in job_types

    def test_detects_s3_and_boto3_athena(self):
        scanner = DependencyScanner()
        source = (
            'input_path = "s3://bucket/input"\n'
            "client = boto3.client('athena')\n"
        )
        results = scanner.scan(source, "lambda_etl.py")

        job_types = {r.job_type for r in results}
        assert JobType.SPARK in job_types
        assert JobType.LAMBDA in job_types

    def test_all_source_paths_set_correctly(self):
        scanner = DependencyScanner()
        source = (
            'path = "s3://b/data"\n'
            "task = AthenaOperator(task_id='x')\n"
            "client = boto3.client('athena')\n"
        )
        results = scanner.scan(source, "multi.py")

        assert all(r.source_path == "multi.py" for r in results)
