from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.ssh.operators.ssh import SSHOperator

from fivetran_provider_async.operators import FivetranOperator

default_args = {
    "owner": "Airflow",
    "start_date": datetime(2021, 4, 6),
}

with DAG(
    dag_id="ad_reporting_dag",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
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
