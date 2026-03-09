from __future__ import annotations

import re

from athenaforge.domain.value_objects.dependency_ref import DependencyRef, JobType


class DependencyScanner:
    """Pure domain service that scans source code for infrastructure dependencies."""

    _S3_PATTERN = re.compile(r"s3://[\w\-./]+")
    _ATHENA_OPERATOR_PATTERN = re.compile(
        r"\b(?:AthenaOperator|AwsAthenaOperator)\b"
    )
    _SPARK_MAP_PATTERN = re.compile(
        r"\b(?:MapType|StructField)\b.*\bmap\b"
        r"|\bmap\b.*\b(?:MapType|StructField)\b",
        re.IGNORECASE,
    )
    _LAMBDA_PATTERN = re.compile(r"""boto3\.client\(\s*['"]athena['"]\s*\)""")

    def scan(self, source_code: str, file_path: str) -> list[DependencyRef]:
        """Scan *source_code* for known dependency patterns."""
        results: list[DependencyRef] = []

        # S3 paths → S3 dependency
        s3_matches = self._S3_PATTERN.findall(source_code)
        if s3_matches:
            results.append(
                DependencyRef(
                    source_path=file_path,
                    job_type=JobType.SPARK,
                    references=tuple(s3_matches),
                    athena_queries=(),
                )
            )

        # AthenaOperator / AwsAthenaOperator → Airflow DAG dependency
        athena_matches = self._ATHENA_OPERATOR_PATTERN.findall(source_code)
        if athena_matches:
            results.append(
                DependencyRef(
                    source_path=file_path,
                    job_type=JobType.AIRFLOW_DAG,
                    references=tuple(athena_matches),
                    athena_queries=(),
                )
            )

        # MapType / StructField with "map" → Spark MAP dependency
        spark_map_matches = self._SPARK_MAP_PATTERN.findall(source_code)
        if spark_map_matches:
            results.append(
                DependencyRef(
                    source_path=file_path,
                    job_type=JobType.SPARK,
                    references=tuple(spark_map_matches),
                    athena_queries=(),
                )
            )

        # boto3.client('athena') → Lambda dependency
        lambda_matches = self._LAMBDA_PATTERN.findall(source_code)
        if lambda_matches:
            results.append(
                DependencyRef(
                    source_path=file_path,
                    job_type=JobType.LAMBDA,
                    references=tuple(lambda_matches),
                    athena_queries=(),
                )
            )

        return results
