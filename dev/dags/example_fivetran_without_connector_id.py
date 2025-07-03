from datetime import datetime, timedelta

try:
    from airflow.sdk.definitions.dag import DAG
except ImportError:
    from airflow import DAG

from fivetran_provider_async.operators import FivetranOperator

with DAG(
    "example_fivetran_without_conn_id",
    schedule=timedelta(days=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    fivetran_sync_start = FivetranOperator(
        task_id="fivetran_task",
        fivetran_conn_id="fivetran_default",
        connector_name="{{ var.value.connector_name }}",
        destination_name="{{ var.value.destination_name }}",
    )
