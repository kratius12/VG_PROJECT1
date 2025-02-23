from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from orm import truncate_table, execute_stored_procedure
from bulk_insert import bulk_insert_staging

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 2, 22),
    'email_on_failure': True,
    'retries': 1
}

dag = DAG(
    'bulk_insert_pipeline',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
)

def truncate_task():
    truncate_table("staging_table")

def bulk_insert_task():
    bulk_insert_staging("data.csv")

def migration_task():
    execute_stored_procedure("Move_Staging_To_Final")

truncate_op = PythonOperator(
    task_id='truncate_staging',
    python_callable=truncate_task,
    dag=dag
)

bulk_insert_op = PythonOperator(
    task_id='bulk_insert_staging',
    python_callable=bulk_insert_task,
    dag=dag
)

migration_op = PythonOperator(
    task_id='execute_stored_procedure',
    python_callable=migration_task,
    dag=dag
)

truncate_op >> bulk_insert_op >> migration_op
