from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from init.drop_tables import drop_datawarehouse_tables
from init.create_tables import create_datawarehouse_tables
from etl.dw_loader import (
    load_dim_roles,
    load_dim_time,
    load_dim_members,
    load_dim_peak,
    load_dim_expedition,
    load_dim_member_expedition_details,
    load_fact_member_expedition,
)

with DAG("dw_etl_dag", start_date=datetime(2024, 1, 1), schedule_interval=None, catchup=False) as dag:
    drop_tables = PythonOperator(
        task_id="drop_dw_tables",
        python_callable=drop_datawarehouse_tables
    )

    create_tables = PythonOperator(
        task_id="create_dw_tables",
        python_callable=create_datawarehouse_tables
    )

    load_roles = PythonOperator(task_id="load_dim_roles", python_callable=load_dim_roles)
    load_time = PythonOperator(task_id="load_dim_time", python_callable=load_dim_time)
    load_members = PythonOperator(task_id="load_dim_members", python_callable=load_dim_members)
    load_peaks = PythonOperator(task_id="load_dim_peak", python_callable=load_dim_peak)

    load_expedition = PythonOperator(task_id="load_dim_expedition", python_callable=load_dim_expedition)
    load_details = PythonOperator(task_id="load_dim_member_expedition_details", python_callable=load_dim_member_expedition_details)

    load_fact = PythonOperator(task_id="load_fact_member_expedition", python_callable=load_fact_member_expedition)

    drop_tables >> create_tables

    # Independent dims
    create_tables >> [load_roles, load_time, load_members, load_peaks, load_details]

    # Dependent dims (need peak, time, members, etc.)
    [load_peaks, load_time] >> load_expedition

    # Fact depends on everything else
    [load_roles, load_time, load_members, load_peaks, load_expedition, load_details] >> load_fact
