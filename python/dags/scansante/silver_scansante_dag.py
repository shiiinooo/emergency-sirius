from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import sys


sys.path.append('/home/airflow/scripts/scansante')
from transform_xlsx_to_csv import transform_xlsx_to_csv_task


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
    'silver_scansante_dag',
    default_args=default_args,
    description='A DAG to transform raw scansante data into structured data (Silver Layer)',
    schedule_interval=None,  # Triggered by Bronze DAG
)

# Task to transform XLSX to CSV
transform_xlsx_to_csv_operator = PythonOperator(
    task_id='transform_xlsx_to_csv',
    python_callable=transform_xlsx_to_csv_task,  # Appel de la fonction définie ci-dessus
    provide_context=True,
    dag=silver_dag,
)

# Task to transfer data from bronze to silver using SCP
transfer_data_task = BashOperator(
    task_id='transfer_data_to_silver',
    bash_command=(
        'scp /home/airflow/data/bronze/scansante/scansante_data*.csv hadoop@192.168.4.50:/opt/hadoop/data/silver/scansante'
    ),
    dag=silver_dag,
)



# Task to run the upload_to_hdfs.sh script on the Hadoop machine
run_hdfs_upload_script_task = BashOperator(
    task_id='run_hdfs_upload_script',
    bash_command=(
        'ssh hadoop@192.168.4.50 "bash /opt/hadoop/scripts/scansante/upload_data.sh"'
    ),
    dag=silver_dag,
)

# Task to trigger the Gold DAG
trigger_gold_dag_task = TriggerDagRunOperator(
    task_id='trigger_scansante_dag',
    trigger_dag_id='gold_scansante_dag',
    dag=silver_dag,
)

# Set task dependencies
transform_xlsx_to_csv_operator >> transfer_data_task >> run_hdfs_upload_script_task >> trigger_gold_dag_task


#transfer_data_task >> run_hdfs_upload_script_task