from datetime import datetime, timedelta

from airflow import DAG
from fivetran_provider.operators.fivetran import FivetranOperator

from fivetran_provider_async.operators.fivetran import FivetranOperatorAsync
from fivetran_provider_async.sensors.fivetran import FivetranSensorAsync

default_args = {
    "owner": "Airflow",
    "start_date": datetime(2022, 4, 14),
}

dag = DAG(
    dag_id="example_fivetran_async",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False,
)

with dag:
    fivetran_async_op = FivetranOperatorAsync(
        task_id="fivetran_async_op",
        fivetran_conn_id="fivetran_default",
        connector_id="bronzing_largely",
    )

    fivetran_sync_op = FivetranOperator(
        task_id="fivetran_sync_op",
        fivetran_conn_id="fivetran_default",
        connector_id="bronzing_largely",
    )

    fivetran_async_sensor = FivetranSensorAsync(
        task_id="fivetran_async_sensor",
        fivetran_conn_id="fivetran_default",
        connector_id="bronzing_largely",
        poke_interval=5,
        xcom="{{ task_instance.xcom_pull('fivetran_sync_op', key='return_value') }}",
    )

    fivetran_async_op >> fivetran_sync_op >> fivetran_async_sensor
