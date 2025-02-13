from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 17),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the Gold DAG
gold_dag = DAG(
    'gold_911_dag',
    default_args=default_args,
    description='A DAG to aggregate and enrich 911 calls data for final use (Gold Layer)',
    schedule_interval=None,  # Triggered by Silver DAG
)

# Task to run the run_spark_job.sh script on the Hadoop machine
run_spark_job_script_task = BashOperator(
    task_id='run_hdfs_upload_script',
    bash_command=(
        'ssh hadoop@192.168.4.50 "bash /opt/hadoop/scripts/911_calls/run_spark_job.sh"'
    ),
    dag=gold_dag,
)

# Only one task in this DAG
run_spark_job_script_task