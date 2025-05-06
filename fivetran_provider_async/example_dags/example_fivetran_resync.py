from datetime import datetime, timedelta

from airflow import DAG

from fivetran_provider_async.operators import FivetranResyncOperator

default_args = {
    "owner": "Airflow",
    "start_date": datetime(2021, 4, 6),
}

dag = DAG(
    dag_id="example_fivetran_resync",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False,
)

with dag:
    # Preform a full resync all schemas in the connector
    fivetran_resync_all = FivetranResyncOperator(
        task_id="fivetran_resync_all",
        fivetran_conn_id="fivetran_default",
        connector_id="{{ var.value.connector_id }}",
    )

    # Only resync certain schemas.
    # https://fivetran.com/docs/rest-api/api-reference/connections/resync-connection
    # The "schema" depends on what's configured in fivetran, so for salesforce it might look like
    # scope={"salesforce":["Account", "Campaign"]}
    fivetran_resync_scoped = FivetranResyncOperator(
        task_id="fivetran_resync_scoped",
        fivetran_conn_id="fivetran_default",
        connector_id="{{ var.value.connector_id }}",
        scope={"schema": ["table1", "table2"]},
    )
