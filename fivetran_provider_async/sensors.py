from __future__ import annotations

from datetime import datetime
from functools import cached_property
from typing import TYPE_CHECKING, Any, Optional

from airflow.exceptions import AirflowException
from airflow.sensors.base import BaseSensorOperator
from airflow.utils import timezone

if TYPE_CHECKING:
    import pendulum
    from airflow.utils.context import Context

from fivetran_provider_async.hooks import FivetranHook
from fivetran_provider_async.triggers import FivetranTrigger


class FivetranSensor(BaseSensorOperator):
    """
    `FivetranSensor` asynchronously monitors for a Fivetran sync job to complete
    after the sensor starts running.

    Monitoring with `FivetranSensor` allows you to trigger downstream processes only
    when the Fivetran sync jobs have completed, ensuring data consistency. You can
    use multiple instances of `FivetranSensor` to monitor multiple Fivetran
    connectors.

    By default, `FivetranSensor` starts monitoring for a new sync to complete
    starting from the clock time the sensor is triggered. Alternatively, you can
    specify the `completed_after_time`, in which case the sensor will wait for
    a success which occurred after this timestamp.

    This sensor does not take into account when a Fivetran sync job is started;
    it only looks at when it completes. For example, if the Fivetran sync job
    starts at `2020-01-01 02:55:00` and ends at `2020-01-01 03:05:00`, and the
    `completed_after_time` is `2020-01-01 03:00:00`, then the sensor will
    stop waiting at `03:05:00`.

    `FivetranSensor` requires that you specify either the `connector_id` or
    both `connector_name` and `destination_name` of the sync job to start.
    You can find `connector_id` in the Settings page of the
    connector you configured in the
    `Fivetran dashboard <https://fivetran.com/dashboard/connectors>`_.

    If you do not want to run `FivetranSensor` in async mode, you can set
    `deferrable` to False in sensor.

    :param fivetran_conn_id: `Conn ID` of the Connection to be used to configure
        the hook.
    :param connector_id: Optional. ID of the Fivetran connector to sync, found on the
        Connector settings page in the Fivetran Dashboard.
    :param connector_name: Optional. Name of the Fivetran connector to sync, found on the
        Connectors page in the Fivetran Dashboard.
    :param destination_name: Optional. Destination of the Fivetran connector to sync, found on the
        Connectors page in the Fivetran Dashboard.
    :param poke_interval: Time in seconds that the job should wait in
        between each try
    :param fivetran_retry_limit: # of retries when encountering API errors
    :param fivetran_retry_delay: Time to wait before retrying API request
    :param completed_after_time: Optional. The time we are comparing the
        Fivetran `succeeded_at` and `failed_at` timestamps to. This field
        is templated; common use cases may be to use XCOM or to use
        "{{ data_interval_end }}". If left as None, then the Sensor will
        set the `completed_after_time` to be the latest completed time,
        meaning the Sensor will pass only when a new sync succeeds after
        the time this Sensor started executing.
    :param reschedule_wait_time: Optional. If connector is in a rescheduled
        state, this will be the number of seconds to wait before restarting. If
        None, then Fivetran's suggestion is used instead.
    :param always_wait_when_syncing: If True, then this method will
        always return False when the connector's sync_state is "syncing",
        no matter what.
    :param propagate_failures_forward: If True, then this method will always
        raise an AirflowException when the most recent connector status is
        a failure and there are no successes dated after the target time.
        Specifically, this makes it so that
        `completed_after_time > failed_at > succeeded_at` is considered a
        fail condition.
    :param deferrable: Run sensor in deferrable mode. default is True.
    """

    template_fields = ["connector_id", "completed_after_time"]

    def __init__(
        self,
        connector_id: Optional[str] = None,
        connector_name: Optional[str] = None,
        destination_name: Optional[str] = None,
        fivetran_conn_id: str = "fivetran_default",
        poke_interval: int = 60,
        fivetran_retry_limit: int = 3,
        fivetran_retry_delay: int = 1,
        completed_after_time: str | datetime | None = None,
        reschedule_wait_time: int | None = None,
        always_wait_when_syncing: bool = False,
        propagate_failures_forward: bool = False,
        deferrable: bool = True,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.fivetran_conn_id = fivetran_conn_id
        self._connector_id = connector_id
        self.connector_name = connector_name
        self.destination_name = destination_name
        self.poke_interval = poke_interval
        self.previous_completed_at: pendulum.DateTime | None = None
        self.fivetran_retry_limit = fivetran_retry_limit
        self.fivetran_retry_delay = fivetran_retry_delay

        if "xcom" in kwargs:
            import warnings

            warnings.warn(
                "kwarg `xcom` is deprecated. Please use `completed_after_time` instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            if completed_after_time is None:
                completed_after_time = kwargs.pop("xcom", None)

        # (Similar parsing logic as airflow.sensors.date_time.DateTimeSensor, except allow None.)
        # self.completed_after_time can't be a datetime object as it is a template_field
        if isinstance(completed_after_time, datetime):
            self.completed_after_time = completed_after_time.isoformat()
        elif isinstance(completed_after_time, str):
            self.completed_after_time = completed_after_time
        elif completed_after_time is None:
            self.completed_after_time = ""
        else:
            raise TypeError(
                "Expected None, str, or datetime.datetime type for completed_after_time."
                f" Got {type(completed_after_time)}"
            )

        self._completed_after_time_rendered: datetime | None = None

        if "reschedule_time" in kwargs:
            import warnings

            warnings.warn(
                "kwarg `reschedule_time` is deprecated. Please use `reschedule_wait_time` instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            if reschedule_wait_time is None:
                reschedule_wait_time = kwargs.pop("reschedule_time", None)

        self.reschedule_wait_time = reschedule_wait_time
        self.always_wait_when_syncing = always_wait_when_syncing
        self.propagate_failures_forward = propagate_failures_forward
        self.deferrable = deferrable

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
                    xcom=self.completed_after_time,
                    poke_interval=self.poke_interval,
                    reschedule_wait_time=self.reschedule_wait_time,
                ),
                method_name="execute_complete",
            )

    @cached_property
    def hook(self) -> FivetranHook:
        """Create and return a FivetranHook."""
        return FivetranHook(
            self.fivetran_conn_id,
            retry_limit=self.fivetran_retry_limit,
            retry_delay=self.fivetran_retry_delay,
        )

    @cached_property
    def connector_id(self) -> str:
        if self._connector_id:
            return self._connector_id
        elif self.connector_name and self.destination_name:
            return self.hook.get_connector_id(
                connector_name=self.connector_name, destination_name=self.destination_name
            )

        raise ValueError("No value specified for connector_id or to both connector_name and destination_name")

    @property
    def xcom(self) -> str:
        import warnings

        warnings.warn(
            "`xcom` attribute is deprecated. Use `completed_after_time` attribute instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        return self.completed_after_time

    @xcom.setter
    def xcom(self, value: str) -> None:
        import warnings

        warnings.warn(
            "`xcom` attribute is deprecated. Use `completed_after_time` attribute instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        self.completed_after_time = value

    def poke(self, context: Context):
        if self._completed_after_time_rendered is None:
            if not self.completed_after_time:
                self._completed_after_time_rendered = self.hook.get_last_sync(self.connector_id)
            else:
                self._completed_after_time_rendered = timezone.parse(self.completed_after_time)

        return self.hook.is_synced_after_target_time(
            connector_id=self.connector_id,
            completed_after_time=self._completed_after_time_rendered,
            reschedule_wait_time=self.reschedule_wait_time,
            always_wait_when_syncing=self.always_wait_when_syncing,
            propagate_failures_forward=self.propagate_failures_forward,
        )

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
