from datetime import datetime, timedelta

try:
    from airflow.sdk.definitions.dag import DAG
except ImportError:
    from airflow import DAG

from fivetran_provider_async.operators import FivetranOperator

dag = DAG(
    dag_id="example_fivetran",
    schedule=timedelta(days=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
)

with dag:
    fivetran_sync_start = FivetranOperator(
        task_id="fivetran_task",
        fivetran_conn_id="fivetran_default",
        connector_id="{{ var.value.connector_id }}",
    )
