from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 17),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the Silver DAG
silver_dag = DAG(
    'silver_temps_dag',
    default_args=default_args,
    description='A DAG to transform raw waiting time calls data into structured data (Silver Layer)',
    schedule_interval=None,  # Triggered by Bronze DAG
)

## Task to transfer data from bronze to silver using SCP
transfer_data_task = BashOperator(
    task_id='transfer_data_to_silver',
    bash_command=(
        'scp /home/airflow/data/bronze/urgences-data/Urgence_data.csv hadoop@192.168.4.50:/opt/hadoop/data/silver/temps_attente'
    ),
    dag=silver_dag,
)
# Task to run the upload_to_hdfs.sh script on the Hadoop machine
run_hdfs_upload_script_task = BashOperator(
    task_id='run_hdfs_upload_script',
    bash_command=(
        'ssh hadoop@192.168.4.50 "bash /opt/hadoop/scripts/temps_attente/upload_to_hdfs.sh"'
    ),
    dag=silver_dag,
)
# Task to trigger the Gold DAG
trigger_gold_dag_task = TriggerDagRunOperator(
    task_id='trigger_gold_temps_dag',
    trigger_dag_id='gold_temps_dag',
    dag=silver_dag,
)

# Set task dependencies
transfer_data_task >> trigger_gold_dag_task