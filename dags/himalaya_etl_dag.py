from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from etl.extract import extract_data
from etl.transform import transform_data
from etl.load import load_data
from init.create_tables import create_staging_tables

with DAG("himalaya_etl_dag", start_date=datetime(2024, 1, 1), schedule_interval="@daily", catchup=False) as dag:
    create_tables = PythonOperator(
        task_id="create_staging_tables",
        python_callable=create_staging_tables
    )

    extract = PythonOperator(task_id="extract", python_callable=extract_data)
    transform = PythonOperator(task_id="transform", python_callable=transform_data)
    load = PythonOperator(task_id="load", python_callable=load_data)

    create_tables >> extract >> transform >> load
