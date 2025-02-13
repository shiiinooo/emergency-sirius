from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import sys

# Add the path to the scripts folder
sys.path.append('/home/airflow/scripts/temps_attente')

# Import the fetch function
from fetch_temps_data import download_kaggle_dataset

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 31),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the Bronze DAG
bronze_dag = DAG(
    'bronze_temps_dag',
    default_args=default_args,
    description='A DAG to fetch raw waiting time calls data from Kaggle (Bronze Layer)',
    schedule_interval=timedelta(days=1),
)

# Task to fetch Kaggle data
fetch_kaggle_data_task = PythonOperator(
    task_id='fetch_temps_data',
    python_callable=download_kaggle_dataset,
    op_kwargs={'dataset_name': "nadianassiri/urgences-data", 'dataset_folder': "urgences-data"},
    dag=bronze_dag,
)


# Task to trigger the Silver DAG
trigger_silver_dag_task = TriggerDagRunOperator(
    task_id='trigger_silver_temps_dag',
    trigger_dag_id='silver_temps_dag',
    dag=bronze_dag,
)

# Set task dependencies
fetch_kaggle_data_task >> trigger_silver_dag_task