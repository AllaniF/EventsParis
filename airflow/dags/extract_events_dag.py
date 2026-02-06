from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="extract_paris_events",
    default_args=default_args,
    description="Pipeline ETL pour les événements de Paris (Bronze -> Silver)",
    start_date=datetime(2026, 2, 1),
    schedule_interval="@daily",
    catchup=False,
    is_paused_upon_creation=False,
    tags=['etl', 'paris', 'events']
) as dag:

    dag.doc_md = """
    # Pipeline ETL Événements Paris
    Ce DAG orchestre le flux de données :
    1. **Extraction** : API -> MongoDB 
    2. **Transformation & Chargement** : MongoDB -> PostgreSQL 
    """

    extract = BashOperator(
        task_id="extract_events",
        bash_command="python /opt/airflow/src/extract.py",
        doc_md="Exécute le script d'extraction vers MongoDB"
    )

    transform = BashOperator(
        task_id="transform_load_events",
        bash_command="python /opt/airflow/src/transform_load.py",
        doc_md="Nettoie les données (HTML, doublons) et charge dans le Data Warehouse PostgreSQL"
    )

    # Définition de l'ordre d'exécution (Dependency)
    # extract doit réussir avant que transform ne commence
    extract >> transform
