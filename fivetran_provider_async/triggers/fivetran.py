import asyncio
from typing import Any, AsyncIterator, Dict, Tuple

import pendulum
from airflow.triggers.base import BaseTrigger, TriggerEvent

from fivetran_provider_async.hooks.fivetran import FivetranHookAsync


class FivetranTrigger(BaseTrigger):
    """
    FivetranTrigger is fired as deferred class with params to run the task in trigger worker

    :param task_id: Reference to task id of the Dag
    :param polling_period_seconds:  polling period in seconds to check for the status
    :param connector_id: Reference to the Fivetran connector id being used
    :param fiventran_conn_id: Reference to Fivetran connection id
    :param previous_completed_at: The last time the connector ran, collected on Sensor
        initialization.
    :param xcom: If used, FivetranSensorAsync receives timestamp of previously
        completed sync
    :type xcom: str
    """

    def __init__(
        self,
        task_id: str,
        polling_period_seconds: float,
        connector_id: str,
        fivetran_conn_id: str,
        previous_completed_at: pendulum.DateTime,
        xcom: str = "",
        poll_interval: float = 4.0,
    ):
        super().__init__()
        self.task_id = task_id
        self.polling_period_seconds = polling_period_seconds
        self.connector_id = connector_id
        self.fivetran_conn_id = fivetran_conn_id
        self.previous_completed_at = previous_completed_at
        self.xcom = xcom
        self.poll_interval = poll_interval

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes FivetranTrigger arguments and classpath."""
        return (
            "fivetran_provider_async.triggers.fivetran.FivetranTrigger",
            {
                "task_id": self.task_id,
                "polling_period_seconds": self.polling_period_seconds,
                "connector_id": self.connector_id,
                "fivetran_conn_id": self.fivetran_conn_id,
                "previous_completed_at": self.previous_completed_at,
                "poll_interval": self.poll_interval,
            },
        )

    async def run(self) -> AsyncIterator["TriggerEvent"]:  # type: ignore[override]
        """
        Make async connection to Fivetran,
        Trigger will yield when connector's sync finishes
        """
        try:
            hook = FivetranHookAsync(fivetran_conn_id=self.fivetran_conn_id)
            if self.previous_completed_at is None:
                self.previous_completed_at = await hook.get_last_sync_async(self.connector_id)
            while True:
                res = await hook.get_sync_status_async(self.connector_id, self.previous_completed_at)
                if res == "success":
                    self.previous_completed_at = await hook.get_last_sync_async(self.connector_id)
                    msg = "Fivetran connector %s finished syncing at %s" % (
                        self.connector_id,
                        self.previous_completed_at,
                    )
                    yield TriggerEvent({"status": "success", "message": msg})
                elif res == "pending":
                    self.log.info("sync is still running...")
                    self.log.info("sleeping for %s seconds.", self.polling_period_seconds)
                    await asyncio.sleep(self.polling_period_seconds)
                else:
                    yield TriggerEvent({"status": "error", "message": "error"})
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})
