from typing import Any, Dict, Optional

from airflow.exceptions import AirflowException
from airflow.utils.context import Context
from fivetran_provider.sensor.fivetran import FivetranSensor
from fivetran_provider_async.triggers.fivetran import FivetranSensorTrigger


class FivetranSensorAsync(FivetranSensor):
    """
    `FivetranSensorAsync` asynchronously monitors a Fivetran sync job for completion.
    Monitoring with `FivetranSensor` allows you to trigger downstream processes only
    when the Fivetran sync jobs have completed, ensuring data consistency. You can
    use multiple instances of `FivetranSensor` to monitor multiple Fivetran
    connectors. Note, it is possible to monitor a sync that is scheduled and managed
    from Fivetran; in other words, you can use `FivetranSensor` without using
    `FivetranOperator`. If used in this way, your DAG will wait until the sync job
    starts on its Fivetran-controlled schedule and then completes. `FivetranSensor`
    requires that you specify the `connector_id` of the sync job to start. You can
    find `connector_id` in the Settings page of the connector you configured in the
    `Fivetran dashboard <https://fivetran.com/dashboard/connectors>`_.


    :param fivetran_conn_id: `Conn ID` of the Connection to be used to configure
        the hook.
    :type fivetran_conn_id: str
    :param connector_id: ID of the Fivetran connector to sync, found on the
        Connector settings page in the Fivetran Dashboard.
    :type connector_id: str
    :param poke_interval: Time in seconds that the job should wait in
        between each tries
    :type poke_interval: int
    :param fivetran_retry_limit: # of retries when encountering API errors
    :type fivetran_retry_limit: Optional[int]
    :param fivetran_retry_delay: Time to wait before retrying API request
    :type fivetran_retry_delay: int
    """

    def __init__(
        self,
        *,
        connector_id: str,
        fivetran_conn_id: str = "fivetran",
        poke_interval: int = 60,
        fivetran_retry_limit: int = 3,
        fivetran_retry_delay: int = 1,
        xcom: str = "",
        **kwargs: Any,
    ):
        super().__init__(**kwargs)

    def execute(self, context: Dict[str, Any]) -> None:
        """Check for the target_status and defers using the trigger"""
        self.defer(
            timeout=self.execution_timeout,
            trigger=FivetranSensorTrigger(
                task_id=self.task_id,
                fivetran_conn_id=self.fivetran_conn_id,
                connector_id=self.connector_id,
                previous_completed_at=self.previous_completed_at,
                polling_period_seconds=self.poke_interval,
            ),
            method_name="execute_complete",
        )

    def execute_complete(self, context: "Context", event: Optional[Dict[Any, Any]] = None) -> None:
        """
        Callback for when the trigger fires - returns immediately.
        Relies on trigger to throw an exception, otherwise it assumes execution was
        successful.
        """
        if event:
            if "status" in event and event["status"] == "error":
                msg = "{0}: {1}".format(event["status"], event["message"])
                raise AirflowException(msg)
            if "status" in event and event["status"] == "success":
                self.log.info("%s completed successfully.", self.task_id)
                self.log.info(
                    "Fivetran connector %s finished syncing at  %s",
                    self.connector_id,
                    self.previous_completed_at,
                )
                return None
        self.log.info("%s completed successfully.", self.task_id)
        return None
