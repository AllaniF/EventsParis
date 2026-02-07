from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

# Ajout du dossier src au path pour les imports
sys.path.append('/opt/airflow/src')

# Import dynamique sécurisé
try:
    from extract import fetch_all_events
    from transform_load import main as transform_main
except ImportError as e:
    print(f"Erreur d'import : {e}")
    # Fallback pour éviter que le DAG crashe au chargement si src n'est pas monté
    def fetch_all_events(): print("Import Failed")
    def transform_main(): print("Import Failed")

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
    # Pipeline ETL Événements Paris (Refactored)
    
    Ce DAG orchestre le flux de données en utilisant `PythonOperator` pour meilleure gestion des ressources.
    """

    task_extract = PythonOperator(
        task_id="extract_events_to_mongo",
        python_callable=fetch_all_events,
        doc_md="Récupère les données API et fait un UPSERT dans MongoDB (Idempotent)."
    )

    task_transform = PythonOperator(
        task_id="transform_load_to_postgres",
        python_callable=transform_main,
        doc_md="Stream les données depuis MongoDB et charge dans Postgres."
    )

    task_extract >> task_transform
