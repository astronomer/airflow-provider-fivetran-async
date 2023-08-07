from __future__ import annotations

from typing import TYPE_CHECKING, Any

from airflow.exceptions import AirflowException
from airflow.sensors.base import BaseSensorOperator

if TYPE_CHECKING:
    from airflow.utils.context import Context

from fivetran_provider_async.hooks import FivetranHook
from fivetran_provider_async.triggers import FivetranTrigger


class FivetranSensor(BaseSensorOperator):
    """
    `FivetranSensor` asynchronously monitors a Fivetran sync job for completion.
    Monitoring with `FivetranSensor` allows you to trigger downstream processes only
    when the Fivetran sync jobs have completed, ensuring data consistency. You can
    use multiple instances of `FivetranSensor` to monitor multiple Fivetran
    connectors. `FivetranSensor` requires that you specify the `connector_id` of the sync
    job to start. You can find `connector_id` in the Settings page of the connector you configured in the
    `Fivetran dashboard <https://fivetran.com/dashboard/connectors>`_.
    If you do not want to run `FivetranSensor` in async mode you can set `deferrable` to
    False in sensor.


    :param fivetran_conn_id: `Conn ID` of the Connection to be used to configure
        the hook.
    :param connector_id: ID of the Fivetran connector to sync, found on the
        Connector settings page in the Fivetran Dashboard.
    :param poke_interval: Time in seconds that the job should wait in
        between each tries
    :param fivetran_retry_limit: # of retries when encountering API errors
    :param fivetran_retry_delay: Time to wait before retrying API request
    :param reschedule_wait_time: Optional, if connector is in reset state
            number of seconds to wait before restarting, else Fivetran suggestion used
    :param deferrable: Run sensor in deferrable mode. default is True.
    """

    template_fields = ["connector_id", "xcom"]

    def __init__(
        self,
        connector_id: str,
        fivetran_conn_id: str = "fivetran",
        poke_interval: int = 60,
        fivetran_retry_limit: int = 3,
        fivetran_retry_delay: int = 1,
        xcom: str = "",
        reschedule_wait_time: int = 0,
        reschedule_time: int = 0,
        deferrable: bool = True,
        **kwargs: Any,
    ) -> None:
        self.fivetran_conn_id = fivetran_conn_id
        self.connector_id = connector_id
        self.poke_interval = poke_interval
        self.previous_completed_at = None
        self.fivetran_retry_limit = fivetran_retry_limit
        self.fivetran_retry_delay = fivetran_retry_delay
        self.hook = None
        self.xcom = xcom
        self.reschedule_wait_time = reschedule_wait_time
        self.reschedule_time = reschedule_time
        self.deferrable = deferrable
        super().__init__(**kwargs)

    def execute(self, context: Context) -> None:
        """Check for the target_status and defers using the trigger"""
        if not self.deferrable:
            super().execute(context=context)
        elif not self.poke(context):
            self.defer(
                timeout=self.execution_timeout,
                trigger=FivetranTrigger(
                    task_id=self.task_id,
                    fivetran_conn_id=self.fivetran_conn_id,
                    connector_id=self.connector_id,
                    previous_completed_at=self.previous_completed_at,
                    xcom=self.xcom,
                    poke_interval=self.poke_interval,
                    reschedule_wait_time=self.reschedule_wait_time,
                ),
                method_name="execute_complete",
            )

    def _get_hook(self) -> FivetranHook:
        if self.hook is None:
            self.hook = FivetranHook(
                self.fivetran_conn_id,
                retry_limit=self.fivetran_retry_limit,
                retry_delay=self.fivetran_retry_delay,
            )
        return self.hook

    def poke(self, context):
        hook = self._get_hook()
        if self.previous_completed_at is None:
            self.previous_completed_at = hook.get_last_sync(self.connector_id, self.xcom)
        return hook.get_sync_status(self.connector_id, self.previous_completed_at, self.reschedule_time)

    def execute_complete(self, context: Context, event: dict[Any, Any] | None = None) -> None:
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


class FivetranSensorAsync(FivetranSensor):
    """This sensor has been deprecated. Please use `FivetranSensor`."""

    template_fields = ["connector_id", "xcom"]

    def __init__(self, *args, **kwargs: Any) -> None:
        import warnings

        super().__init__(*args, **kwargs)

        warnings.warn(
            "FivetranSensorAsync has been deprecated. Please use `FivetranSensor`.",
            DeprecationWarning,
            stacklevel=2,
        )
