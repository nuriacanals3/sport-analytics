import sys
import os

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(project_root)

from ingestion import main as ingest_bronze_data
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


default_args = {
    'owner': 'nuria',
    'depends_on_past': False, # Don't wait for yesterday's run to finish
    'email_on_failure': False,
    'retries': 2, # If API times out, try again twice
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'nba_bronze_ingestion_daily',
    default_args=default_args,
    description='Nightly extraction of NBA PbP data to S3',
    schedule='0 8 * * *', # Every day at 8 AM UTC
    start_date=datetime(2026, 3, 10), # Start date for the DAG
    catchup=False,
    tags=['nba_project', 'bronze'],
) as dag:
    ingest_task = PythonOperator(
        task_id='extract_nba_api_to_s3',
        python_callable=ingest_bronze_data
    )

    # Next steps would be to add tasks for Silver and Gold layers, and set dependencies
    # ingest_task >> dbt_silver_task