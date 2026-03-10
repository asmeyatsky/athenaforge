from airflow import DAG
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.providers.amazon.aws.sensors.athena import AthenaSensor
from airflow.providers.amazon.aws.operators.s3 import S3CopyObjectOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-engineering',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'daily_etl_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    extract_revenue = AthenaOperator(
        task_id='extract_revenue',
        query='SELECT * FROM analytics.daily_revenue WHERE dt = {{ ds }}',
        database='analytics',
        output_location='s3://acme-athena-results/etl/',
    )

    transform_metrics = AthenaOperator(
        task_id='transform_metrics',
        query="""
            INSERT INTO analytics.revenue_summary
            SELECT region, SUM(amount) as total
            FROM analytics.daily_revenue
            WHERE dt = '{{ ds }}'
            GROUP BY region
        """,
        database='analytics',
        output_location='s3://acme-athena-results/etl/',
    )

    copy_to_archive = S3CopyObjectOperator(
        task_id='archive_raw_data',
        source_bucket_name='acme-raw-data',
        source_bucket_key='revenue/{{ ds }}/',
        dest_bucket_name='acme-archive',
        dest_bucket_key='revenue/{{ ds }}/',
    )

    extract_revenue >> transform_metrics >> copy_to_archive
