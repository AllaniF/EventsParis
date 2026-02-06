from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Add src to path so we can import the module
sys.path.append('/opt/airflow/src')

try:
    from speedtest_logger import main as run_speedtest_task
except ImportError:
    # Fallback/Mock for parsing if src is not mounted yet
    def run_speedtest_task():
        print("Module not found, creating dummy function")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'speedtest_monitor',
    default_args=default_args,
    description='Run internet speedtest and save to Mongo',
    schedule_interval='*/30 * * * *', # Every 30 minutes
    start_date=datetime(2026, 2, 5),
    catchup=False,
    is_paused_upon_creation=False,
    tags=['monitoring'],
) as dag:

    speedtest_task = PythonOperator(
        task_id='run_speedtest_logger',
        python_callable=run_speedtest_task,
    )

    speedtest_task
