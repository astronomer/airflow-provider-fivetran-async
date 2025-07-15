import time
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from fivetran_provider_async.operators import FivetranOperator
from fivetran_provider_async.sensors import FivetranSensor

default_args = {
    "owner": "Airflow",
    "start_date": datetime(2021, 4, 6),
    "provide_context": True,
}

dag = DAG(
    dag_id="example_fivetran_xcom",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False,
)

with dag:
    fivetran_operator = FivetranOperator(
        task_id="fivetran-operator",
        fivetran_conn_id="fivetran_default",
        connector_id="{{ var.value.connector_id }}",
        wait_for_completion=False,
    )

    delay_task = PythonOperator(task_id="delay_python_task", python_callable=lambda: time.sleep(60))

    fivetran_sensor = FivetranSensor(
        task_id="fivetran-sensor",
        fivetran_conn_id="fivetran_default",
        connector_id="{{ var.value.connector_id }}",
        poke_interval=5,
        completed_after_time="{{ task_instance.xcom_pull('fivetran-operator', key='return_value') }}",
    )

    fivetran_operator >> delay_task >> fivetran_sensor
