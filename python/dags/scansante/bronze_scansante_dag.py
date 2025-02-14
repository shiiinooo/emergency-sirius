from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import sys


sys.path.append('/home/airflow/scripts/scansante')
from fetch_scansante_data import fetch_scansante_data


# Arguments par défaut pour le DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 31),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Définir le DAG pour la couche Bronze
bronze_dag = DAG(
    'bronze_scansante_dag',
    default_args=default_args,
    description='DAG pour télécharger les données brutes ScanSanté (Bronze Layer)',
    schedule_interval=timedelta(days=1),  # Exécution quotidienne
)

# Tâche pour télécharger les données ScanSanté
fetch_scansante_data_task = PythonOperator(
    task_id='fetch_scansante_data',
    python_callable=fetch_scansante_data,
    dag=bronze_dag,
)

# Tâche pour déclencher le DAG Silver
trigger_silver_dag_task = TriggerDagRunOperator(
    task_id='trigger_silver_scansante_dag',
    trigger_dag_id='silver_scansante_dag',  # ID du DAG Silver à déclencher
    dag=bronze_dag,
)

# Dépendances entre les tâches
fetch_scansante_data_task >> trigger_silver_dag_task