from __future__ import annotations

import re


# Mapping of AWS operator names → GCP operator names
_OPERATOR_MAP: dict[str, str] = {
    "AthenaOperator": "BigQueryInsertJobOperator",
    "AwsAthenaOperator": "BigQueryInsertJobOperator",
    "EmrAddStepsOperator": "DataprocSubmitJobOperator",
    "EmrCreateJobFlowOperator": "DataprocSubmitJobOperator",
    "S3KeySensor": "GCSObjectExistenceSensor",
}

# Mapping of AWS import modules → GCP import statements
_IMPORT_MAP: dict[str, str] = {
    "airflow.providers.amazon.aws.operators.athena": "airflow.providers.google.cloud.operators.bigquery",
    "airflow.providers.amazon.aws.operators.emr": "airflow.providers.google.cloud.operators.dataproc",
    "airflow.providers.amazon.aws.sensors.s3": "airflow.providers.google.cloud.sensors.gcs",
    "airflow.providers.amazon.aws.operators.emr_add_steps": "airflow.providers.google.cloud.operators.dataproc",
    "airflow.providers.amazon.aws.operators.emr_create_job_flow": "airflow.providers.google.cloud.operators.dataproc",
    "airflow.providers.amazon.aws.sensors.s3_key": "airflow.providers.google.cloud.sensors.gcs",
}


class DAGRewriterService:
    """Pure domain service that rewrites Airflow DAGs from AWS to GCP."""

    def rewrite(self, dag_content: str) -> tuple[str, list[str]]:
        """Rewrite *dag_content* from AWS operators to GCP equivalents.

        Returns ``(rewritten_content, list_of_changes)``.
        """
        rewritten = dag_content
        changes: list[str] = []

        # Replace import paths
        for aws_module, gcp_module in _IMPORT_MAP.items():
            if aws_module in rewritten:
                rewritten = rewritten.replace(aws_module, gcp_module)
                changes.append(f"Import: {aws_module} → {gcp_module}")

        # Replace operator class names
        for aws_op, gcp_op in _OPERATOR_MAP.items():
            pattern = re.compile(r"\b" + re.escape(aws_op) + r"\b")
            if pattern.search(rewritten):
                rewritten = pattern.sub(gcp_op, rewritten)
                changes.append(f"Operator: {aws_op} → {gcp_op}")

        # Replace s3:// with gs://
        if "s3://" in rewritten:
            rewritten = rewritten.replace("s3://", "gs://")
            changes.append("Storage path: s3:// → gs://")

        return rewritten, changes
