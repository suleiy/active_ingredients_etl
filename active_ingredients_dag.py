from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime
from active_ingredients_etl import run_active_ingredients_etl

default_args = {
    'owner': 'airflow',
    'depands_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'active_ingredients_dag',
    default_args=default_args,
    description="Active Ingredients ETL code"
)

run_etl = PythonOperator(task_id = 'complete_active_ingredients_etl',python_callable=run_active_ingredients_etl,dag = dag)

run_etl