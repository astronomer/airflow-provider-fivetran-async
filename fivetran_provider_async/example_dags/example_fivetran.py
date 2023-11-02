from datetime import datetime, timedelta

from airflow import DAG

from fivetran_provider_async.operators import FivetranOperator

default_args = {
    "owner": "Airflow",
    "start_date": datetime(2021, 4, 6),
}

dag = DAG(
    dag_id="example_fivetran",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False,
)

with dag:
    fivetran_sync_start = FivetranOperator(
        task_id="fivetran_task",
        fivetran_conn_id="fivetran_default",
        connector_id="{{ var.value.connector_id }}",
        deferrable=False,
    )
