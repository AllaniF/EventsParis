from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="extract_paris_events",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
) as dag:

    extract = BashOperator(
        task_id="extract_events",
        bash_command="python /opt/airflow/src/extract.py"
    )
