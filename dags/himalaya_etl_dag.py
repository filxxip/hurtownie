from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from etl.extract import extract_peaks, extract_members, extract_exped
from etl.transform import transform_peaks, transform_members, transform_exped
from etl.load import load_peaks, load_members, load_exped
from init.create_tables import create_required_tables
from init.drop_tables import drop_tables
# from etl.convert import convert_csv_to_pickle
from post.cleanup import cleanup_pickles

with DAG("himalaya_etl_dag", start_date=datetime(2024, 1, 1), schedule_interval="@daily", catchup=False) as dag:
    drop_tables_task = PythonOperator(
        task_id="drop_tables",
        python_callable=drop_tables
    )
    
    create_tables = PythonOperator(
        task_id="create_required_tables",
        python_callable=create_required_tables
    )

    cleanup_task = PythonOperator(
        task_id="cleanup_pickles",
        python_callable=cleanup_pickles
    )

    # === PEAKS PATH ===
    extract_peaks_task = PythonOperator(task_id="extract_peaks", python_callable=extract_peaks)
    transform_peaks_task = PythonOperator(task_id="transform_peaks", python_callable=transform_peaks)
    load_peaks_task = PythonOperator(task_id="load_peaks", python_callable=load_peaks)

    # === MEMBERS PATH ===
    extract_members_task = PythonOperator(task_id="extract_members", python_callable=extract_members)
    transform_members_task = PythonOperator(task_id="transform_members", python_callable=transform_members)
    load_members_task = PythonOperator(task_id="load_members", python_callable=load_members)

    # === EXPED PATH ===
    extract_exped_task = PythonOperator(task_id="extract_exped", python_callable=extract_exped)
    transform_exped_task = PythonOperator(task_id="transform_exped", python_callable=transform_exped)
    load_exped_task = PythonOperator(task_id="load_exped", python_callable=load_exped)

    # DAG Dependencies
    drop_tables_task >> create_tables >>  extract_peaks_task >> transform_peaks_task >> load_peaks_task >> cleanup_task
    drop_tables_task >> create_tables >>  extract_members_task >> transform_members_task >> load_members_task >> cleanup_task
    drop_tables_task >> create_tables >>  extract_exped_task >> transform_exped_task >> load_exped_task >> cleanup_task
