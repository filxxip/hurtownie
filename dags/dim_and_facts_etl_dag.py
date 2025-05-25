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
    load_dim_country_economy
)
from post.clean_nationality import cleanup_dim_member_nationalities
from post.clean_roles import clean_roles
from post.cleanup import cleanup_tmp_tables
from post.cleanup_dim_member import cleanup_dim_member_nulls

with DAG("dw_etl_dag", start_date=datetime(2024, 1, 1), schedule_interval=None, catchup=False) as dag:
    # Drop and recreate warehouse tables
    drop_tables = PythonOperator(
        task_id="drop_dw_tables",
        python_callable=drop_datawarehouse_tables
    )

    create_tables = PythonOperator(
        task_id="create_dw_tables",
        python_callable=create_datawarehouse_tables
    )

    # Dimension loaders
    load_roles = PythonOperator(task_id="load_dim_roles", python_callable=load_dim_roles)
    clean_roles_task = PythonOperator(task_id="clean_roles", python_callable=clean_roles)

    load_time = PythonOperator(task_id="load_dim_time", python_callable=load_dim_time)

    load_members = PythonOperator(task_id="load_dim_members", python_callable=load_dim_members)
    cleanup_nationalities_task = PythonOperator(
        task_id="cleanup_dim_member_nationalities",
        python_callable=cleanup_dim_member_nationalities
    )

    load_peaks = PythonOperator(task_id="load_dim_peak", python_callable=load_dim_peak)

    load_expedition = PythonOperator(task_id="load_dim_expedition", python_callable=load_dim_expedition)

    load_details = PythonOperator(
        task_id="load_dim_member_expedition_details",
        python_callable=load_dim_member_expedition_details
    )

    load_country_economy = PythonOperator(
        task_id="load_dim_country_economy",
        python_callable=load_dim_country_economy
    )

    # Fact loader
    load_fact = PythonOperator(
        task_id="load_fact_member_expedition",
        python_callable=load_fact_member_expedition
    )

    # Cleanup tmp tables
    cleanup_tmp = PythonOperator(
        task_id="cleanup_tmp_tables",
        python_callable=cleanup_tmp_tables
    )

    cleanup_dim_member = PythonOperator(
        task_id="cleanup_dim_member_nulls",
        python_callable=cleanup_dim_member_nulls
    )

    # Dependencies
    drop_tables >> create_tables

    create_tables >> [load_time, load_peaks, load_details, load_country_economy]
    create_tables >> load_roles >> clean_roles_task
    create_tables >> load_members >> cleanup_nationalities_task >> cleanup_dim_member

    [load_peaks, load_time] >> load_expedition

    [
        clean_roles_task,
        load_time,
        cleanup_dim_member,
        load_peaks,
        load_expedition,
        load_details,
        load_country_economy
    ] >> load_fact >> cleanup_tmp
