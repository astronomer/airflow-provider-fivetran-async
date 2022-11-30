from typing import Any, Dict, Optional

from airflow.exceptions import AirflowException
from airflow.utils.context import Context
from fivetran_provider.operators.fivetran import FivetranOperator

from fivetran_provider_async.triggers import FivetranTrigger


class FivetranOperatorAsync(FivetranOperator):
    """
    `FivetranOperatorAsync` submits a Fivetran sync job , and polls for its status on the
    airflow trigger.`FivetranOperatorAsync` requires that you specify the `connector_id` of
    the sync job to start. You can find `connector_id` in the Settings page of the connector
    you configured in the `Fivetran dashboard <https://fivetran.com/dashboard/connectors>`_.

    :param fivetran_conn_id: `Conn ID` of the Connection to be used to configure
        the hook.
    :param connector_id: ID of the Fivetran connector to sync, found on the
        Connector settings page in the Fivetran Dashboard.
    :param poke_interval: Time in seconds that the job should wait in
        between each tries
    """

    def execute(self, context: Dict[str, Any]) -> None:
        """Start the sync using synchronous hook"""
        hook = self._get_hook()
        hook.prep_connector(self.connector_id, self.schedule_type)
        hook.start_fivetran_sync(self.connector_id)

        # Defer and poll the sync status on the Triggerer
        self.defer(
            timeout=self.execution_timeout,
            trigger=FivetranTrigger(
                task_id=self.task_id,
                fivetran_conn_id=self.fivetran_conn_id,
                connector_id=self.connector_id,
                poke_interval=self.poll_frequency,
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
                self.log.info(
                    event["message"],
                )
                return event["return_value"]
