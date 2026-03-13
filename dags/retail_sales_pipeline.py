from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime, timedelta
import subprocess
import sys
import os

# Default arguments for all tasks
default_args = {
    'owner': 'eswar',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

# DAG definition
with DAG(
    dag_id='retail_sales_pipeline',
    default_args=default_args,
    description='End to end retail sales pipeline - CSV to Snowflake via DBT',
    schedule='0 6 * * *',        # runs every day at 6am
    start_date=datetime(2024, 1, 1),
    catchup=False,                # dont backfill missed runs
    tags=['retail', 'snowflake', 'dbt'],
) as dag:

    # Task 1 - Load CSV files into Snowflake Staging
    def load_staging():
        import importlib.util
        import sys

        script_path = '/opt/airflow/scripts/load_to_staging.py'
        spec = importlib.util.spec_from_file_location("load_to_staging", script_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        module.main()

    task_load_staging = PythonOperator(
        task_id='load_csv_to_staging',
        python_callable=load_staging,
    )

    # Task 2 - Run DBT Bronze models
    # Task 2 - Run DBT Bronze models
    task_bronze = BashOperator(
        task_id='run_bronze_models',
        bash_command=(
            'cd /opt/airflow/retail_sales_platform_kaggel && '
            'dbt deps --profiles-dir /opt/airflow/retail_sales_platform_kaggel && '
            'dbt run --select bronze --profiles-dir /opt/airflow/retail_sales_platform_kaggel'
        ),
    )

    # Task 3 - Run DBT Silver models
    task_silver = BashOperator(
        task_id='run_silver_models',
        bash_command='cd /opt/airflow/retail_sales_platform_kaggel && dbt run --select silver --profiles-dir /opt/airflow/retail_sales_platform_kaggel',
    )

    # Task 4 - Run DBT Gold models
    task_gold = BashOperator(
        task_id='run_gold_models',
        bash_command='cd /opt/airflow/retail_sales_platform_kaggel && dbt run --select gold --profiles-dir /opt/airflow/retail_sales_platform_kaggel',
    )

    # Task 5 - Run DBT tests
    task_tests = BashOperator(
        task_id='run_dbt_tests',
        bash_command='cd /opt/airflow/retail_sales_platform_kaggel && dbt test --profiles-dir /opt/airflow/retail_sales_platform_kaggel',
    )

    # Define task dependencies - this is the pipeline order
    task_load_staging >> task_bronze >> task_silver >> task_gold >> task_tests