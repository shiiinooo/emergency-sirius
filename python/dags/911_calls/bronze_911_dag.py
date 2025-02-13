from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import sys

# Add the path to the scripts folder
sys.path.append('/home/airflow/scripts/911_calls')

# Import the fetch function
from fetch_kaggle_data import fetch_kaggle_data

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
    'bronze_911_dag',
    default_args=default_args,
    description='A DAG to fetch raw 911 calls data from Kaggle (Bronze Layer)',
    schedule_interval=timedelta(days=1),
)

# Task to fetch Kaggle data
fetch_kaggle_data_task = PythonOperator(
    task_id='fetch_kaggle_data',
    python_callable=fetch_kaggle_data,
    dag=bronze_dag,
)

# Task to trigger the Silver DAG
trigger_silver_dag_task = TriggerDagRunOperator(
    task_id='trigger_silver_911_dag',
    trigger_dag_id='silver_911_dag',
    dag=bronze_dag,
)

# Set task dependencies
fetch_kaggle_data_task >> trigger_silver_dag_task