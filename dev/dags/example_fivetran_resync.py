from datetime import datetime, timedelta

try:
    from airflow.sdk.definitions.dag import DAG
except ImportError:
    from airflow import DAG

from fivetran_provider_async.operators import FivetranResyncOperator

dag = DAG(
    dag_id="example_fivetran_resync",
    schedule=timedelta(days=1),
    start_date=datetime(2025, 1, 1),
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
        scope={"ci_airflow_provider_fivetran_async": ["ci_airflow_provider_fivetran_async"]},
    )
