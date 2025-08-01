from datetime import datetime, timedelta

from airflow import DAG

from fivetran_provider_async.operators import FivetranOperator

default_args = {
    "owner": "Airflow",
    "start_date": datetime(2021, 4, 6),
}


with DAG(
    "example_fivetran_without_conn_id",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    fivetran_sync_start = FivetranOperator(
        task_id="fivetran_task",
        fivetran_conn_id="fivetran_default",
        connector_name="{{ var.value.connector_name }}",
        destination_name="{{ var.value.destination_name }}",  # integration_team_ci
    )
