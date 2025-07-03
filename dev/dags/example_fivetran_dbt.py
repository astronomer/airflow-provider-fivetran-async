from datetime import datetime, timedelta

try:
    from airflow.sdk.definitions.dag import DAG
except ImportError:
    from airflow import DAG

from airflow.providers.ssh.operators.ssh import SSHOperator

from fivetran_provider_async.operators import FivetranOperator

with DAG(
    dag_id="ad_reporting_dag",
    schedule=timedelta(days=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:
    linkedin_sync = FivetranOperator(
        task_id="linkedin-ads-sync",
        connector_id="{{ var.value.linkedin_connector_id }}",
    )

    twitter_sync = FivetranOperator(
        task_id="twitter-ads-sync",
        connector_id="{{ var.value.twitter_connector_id }}",
    )

    dbt_run = SSHOperator(
        task_id="dbt_ad_reporting",
        command="cd dbt_ad_reporting ; ~/.local/bin/dbt run -m +ad_reporting",
        ssh_conn_id="dbtvm",
    )

    [linkedin_sync, twitter_sync] >> dbt_run
