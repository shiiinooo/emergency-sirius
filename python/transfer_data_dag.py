from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 11, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG
transfer_data_dag = DAG(
    'transfer_data_dag',
    default_args=default_args,
    description='A DAG to transfer data to hadoop cluster',
    schedule_interval=None,  # This is triggered by the first DAG
)

# Define the task to transfer data using SCP
transfer_data_task = BashOperator(
    task_id='transfer_data',
    bash_command=(
	'/usr/bin/bash -c "echo $PATH && '
        'scp /home/airflow/data/output_stock_data_*.csv hadoop@192.168.4.50:/opt/hadoop/data/ && '
        'ssh hadoop@192.168.4.50 \\"bash /opt/hadoop/scripts/demo_script.sh && echo \'Script executed\'\\""'
    ),
    dag=transfer_data_dag,
)

# Only one task in this DAG, no dependencies needed
