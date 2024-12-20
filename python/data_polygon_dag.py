import sys
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Add the path to the scripts folder
sys.path.append('/home/airflow/scripts')

# Import the ETL function from data_polygon.py
from data_polygon import fetch_and_save_polygon_data

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 10),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG
dag = DAG(
    'polygon_data_dag',
    default_args=default_args,
    description='A DAG to fetch and process data from Polygon.io',
    schedule_interval=timedelta(days=1),
)

# Define the task to run the ETL process
fetch_and_save_data_task = PythonOperator(
    task_id='fetch_polygon_data',
    python_callable=fetch_and_save_polygon_data,
    dag=dag,
)

# Define the task to trigger the transfer_data_dag
trigger_transfer_data_dag_task = TriggerDagRunOperator(
    task_id='trigger_transfer_data_dag',
    trigger_dag_id='transfer_data_dag',
    dag=dag
)

# Set task dependencies
fetch_and_save_data_task >> trigger_transfer_data_dag_task

