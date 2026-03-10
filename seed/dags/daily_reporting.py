from airflow import DAG
from airflow.providers.amazon.aws.operators.athena import AthenaOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'analytics-team',
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    'daily_reporting_dag',
    default_args=default_args,
    schedule_interval='0 6 * * *',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    campaign_report = AthenaOperator(
        task_id='generate_campaign_report',
        query="""
            SELECT campaign_id, SUM(spend) as total_spend,
                   SUM(conversions) as total_conversions
            FROM analytics.campaign_events
            WHERE event_date = '{{ ds }}'
            GROUP BY campaign_id
        """,
        database='analytics',
        output_location='s3://acme-athena-results/reports/',
    )

    user_report = AthenaOperator(
        task_id='generate_user_report',
        query="""
            SELECT DATE_TRUNC('hour', event_timestamp) as hour,
                   COUNT(DISTINCT user_id) as active_users
            FROM analytics.user_events
            WHERE event_date = '{{ ds }}'
            GROUP BY 1
        """,
        database='analytics',
        output_location='s3://acme-athena-results/reports/',
    )

    def send_notification(**kwargs):
        print(f"Reports generated for {kwargs['ds']}")

    notify = PythonOperator(
        task_id='send_notification',
        python_callable=send_notification,
    )

    [campaign_report, user_report] >> notify
