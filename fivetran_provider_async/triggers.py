from __future__ import annotations

import asyncio
from typing import Any, AsyncIterator, Dict, Tuple

import pendulum
from airflow.triggers.base import BaseTrigger, TriggerEvent

from fivetran_provider_async.hooks import FivetranHookAsync


class FivetranTrigger(BaseTrigger):
    """
    FivetranTrigger is fired as deferred class with params to run the task in trigger worker

    :param task_id: Reference to task id of the Dag
    :param connector_id: Reference to the Fivetran connector id being used
    :param fivetran_conn_id: Reference to Fivetran connection id
    :param previous_completed_at: The last time the connector ran, collected on Sensor
        initialization.
    :param xcom: If used, FivetranSensor receives timestamp of previously
        completed sync
    :param poke_interval:  polling period in seconds to check for the status
    :param reschedule_wait_time: Optional, if connector is in reset state,
            number of seconds to wait before restarting the sync.
    """

    def __init__(
        self,
        task_id: str,
        connector_id: str,
        fivetran_conn_id: str,
        previous_completed_at: pendulum.DateTime | None = None,
        xcom: str = "",
        poke_interval: float = 4.0,
        reschedule_wait_time: int | None = None,
    ):
        super().__init__()
        self.task_id = task_id
        self.connector_id = connector_id
        self.fivetran_conn_id = fivetran_conn_id
        self.previous_completed_at = previous_completed_at
        self.xcom = xcom
        self.poke_interval = poke_interval
        self.reschedule_wait_time = reschedule_wait_time

    def serialize(self) -> Tuple[str, Dict[str, Any]]:
        """Serializes FivetranTrigger arguments and classpath."""
        return (
            "fivetran_provider_async.triggers.FivetranTrigger",
            {
                "task_id": self.task_id,
                "poke_interval": self.poke_interval,
                "connector_id": self.connector_id,
                "fivetran_conn_id": self.fivetran_conn_id,
                "previous_completed_at": self.previous_completed_at,
                "xcom": self.xcom,
                "reschedule_wait_time": self.reschedule_wait_time,
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
                self.previous_completed_at = await hook.get_last_sync_async(self.connector_id, self.xcom)
            while True:
                res = await hook.get_sync_status_async(
                    self.connector_id, self.previous_completed_at, self.reschedule_wait_time
                )
                if res == "success":
                    self.previous_completed_at = await hook.get_last_sync_async(self.connector_id)
                    msg = "Fivetran connector %s finished syncing at %s" % (
                        self.connector_id,
                        self.previous_completed_at,
                    )
                    yield TriggerEvent(
                        {
                            "status": "success",
                            "message": msg,
                            "return_value": self.previous_completed_at.to_iso8601_string(),
                        }
                    )
                    return
                elif res == "pending":
                    self.log.info("sync is still running...")
                    self.log.info("sleeping for %s seconds.", self.poke_interval)
                    await asyncio.sleep(self.poke_interval)
                else:
                    yield TriggerEvent({"status": "error", "message": "error"})
                    return
        except Exception as e:
            yield TriggerEvent({"status": "error", "message": str(e)})
            return
