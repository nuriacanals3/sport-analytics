import sys
import os

project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
sys.path.append(project_root)

from ingestion.nba.play_by_play import main as ingest_bronze_data
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

DBT_PROJECT_DIR = os.path.join(project_root, 'transform/nba')
DBT_BIN = os.path.join(project_root, 'venv/bin/dbt')

default_args = {
    'owner': 'nuria',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'nba_pipeline_daily',
    default_args=default_args,
    description='Nightly NBA pipeline: Bronze ingestion → dbt Silver → dbt Gold',
    schedule='0 8 * * *',
    start_date=datetime(2026, 3, 10),
    catchup=False,
    tags=['nba_project', 'bronze', 'silver', 'gold'],
) as dag:

    # Task 1: Bronze — fetch from nba_api and upload raw JSON to S3
    ingest_task = PythonOperator(
        task_id='extract_nba_api_to_s3',
        python_callable=ingest_bronze_data,
    )

    # Task 2: Silver — dbt staging models clean and flatten the raw S3 JSON
    dbt_silver = BashOperator(
        task_id='dbt_run_silver',
        bash_command=(
            f'{DBT_BIN} run --select staging '
            f'--project-dir {DBT_PROJECT_DIR} '
            f'--profiles-dir {DBT_PROJECT_DIR}'
        ),
        env={**os.environ},
    )

    # Task 3: Gold — dbt mart models produce aggregated stats tables
    dbt_gold = BashOperator(
        task_id='dbt_run_gold',
        bash_command=(
            f'{DBT_BIN} run --select marts '
            f'--project-dir {DBT_PROJECT_DIR} '
            f'--profiles-dir {DBT_PROJECT_DIR}'
        ),
        env={**os.environ},
    )

    # Task 4: Validate data quality across all layers
    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command=(
            f'{DBT_BIN} test '
            f'--project-dir {DBT_PROJECT_DIR} '
            f'--profiles-dir {DBT_PROJECT_DIR}'
        ),
        env={**os.environ},
    )

    ingest_task >> dbt_silver >> dbt_gold >> dbt_test
