from datetime import datetime, timedelta

from airflow import DAG

from fivetran_provider_async.operators import FivetranOperator

default_args = {
    "owner": "Airflow",
    "start_date": datetime(2022, 4, 14),
    "fivetran_conn_id": "fivetran_default",
}

dag = DAG(
    dag_id="example_fivetran_async",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False,
)

with dag:
    # Both of these tasks will start a Fivetran sync,
    # and will wait for the Fivetran sync to complete before marking
    # the task as success.

    # However, the async operator uses the triggerer instance to do this,
    # which frees up a worker slot.

    fivetran_async_op = FivetranOperator(
        task_id="fivetran_async_op",
        connector_id="bronzing_largely",
    )

    fivetran_sync_op = FivetranOperator(
        task_id="fivetran_sync_op", connector_id="bronzing_largely", deferrable=False
    )

    fivetran_async_op >> fivetran_sync_op
