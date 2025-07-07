from __future__ import annotations

from functools import cached_property
from time import sleep
from typing import TYPE_CHECKING, Any, Dict, Optional

import pendulum
from airflow.exceptions import AirflowException
from airflow.models import BaseOperator, BaseOperatorLink

if TYPE_CHECKING:
    from airflow.utils.context import Context

from fivetran_provider_async import __version__
from fivetran_provider_async.hooks import FivetranHook
from fivetran_provider_async.triggers import FivetranTrigger
from fivetran_provider_async.utils.operator_utils import datasets


class RegistryLink(BaseOperatorLink):
    """Link to Registry"""

    name = "Astronomer Registry"

    def get_link(self, operator, dttm):
        """Get link to registry page."""

        return (
            f"https://registry.astronomer.io/providers/airflow-provider-fivetran-async/versions/"
            f"{__version__}/modules/{operator.operator_name}"
        )


class FivetranOperator(BaseOperator):
    """
    `FivetranOperator` submits a Fivetran sync job , and polls for its status on the
    airflow trigger.`FivetranOperator` requires that you specify the `connector_id` of
    the sync job to start. You can find `connector_id` in the Settings page of the connector
    you configured in the `Fivetran dashboard <https://fivetran.com/dashboard/connectors>`_.
    If you do not want to run `FivetranOperator` in async mode you can set `deferrable` to
    False in operator.

    :param fivetran_conn_id: `Conn ID` of the Connection to be used to configure the hook.
    :param fivetran_retry_limit: # of retries when encountering API errors
    :param fivetran_retry_delay: Time to wait before retrying API request
    :param run_name: Fivetran run name
    :param timeout_seconds: Timeout in seconds
    :param connector_id: Optional, ID of the Fivetran connector to sync, found on the Connector settings page.
    :param connector_name: Optional, Name of the Fivetran connector to sync, found on the
        Connectors page in the Fivetran Dashboard.
    :param destination_name: Optional, Destination of the Fivetran connector to sync, found on the
        Connectors page in the Fivetran Dashboard.
    :param schedule_type: schedule type. Default is "manual" which takes the connector off Fivetran schedule.
    :param poll_frequency: Time in seconds that the job should wait in between each try.
    :param reschedule_wait_time: Optional, if connector is in reset state,
            number of seconds to wait before restarting the sync.
    :param deferrable: Run operator in deferrable mode. Default is True.
    :param wait_for_completion: Wait for Fivetran sync to complete to finish the task.
    """

    operator_extra_links = (RegistryLink(),)

    template_fields = ["connector_id", "connector_name", "destination_name"]

    def __init__(
        self,
        connector_id: Optional[str] = None,
        connector_name: Optional[str] = None,
        destination_name: Optional[str] = None,
        run_name: Optional[str] = None,
        fivetran_conn_id: str = "fivetran_default",
        fivetran_retry_limit: int = 3,
        fivetran_retry_delay: int = 1,
        poll_frequency: int = 15,
        schedule_type: str = "manual",
        reschedule_wait_time: int = 0,
        deferrable: bool = True,
        wait_for_completion: bool = True,
        **kwargs,
    ) -> None:
        self.connector_id = connector_id
        self.connector_name = connector_name
        self.destination_name = destination_name
        self.fivetran_conn_id = fivetran_conn_id
        self.run_name = run_name

        if "timeout_seconds" in kwargs:
            import warnings

            warnings.warn(
                "kwarg `timeout_seconds` is deprecated. Please use `execution_timeout` instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            if "execution_timeout" not in kwargs:
                kwargs["execution_timeout"] = kwargs.pop("timeout_seconds", None)

        self.fivetran_retry_limit = fivetran_retry_limit
        self.fivetran_retry_delay = fivetran_retry_delay
        self.poll_frequency = poll_frequency
        self.schedule_type = schedule_type
        self.reschedule_wait_time = reschedule_wait_time
        self.wait_for_completion = wait_for_completion
        self.deferrable = deferrable
        super().__init__(**kwargs)

    def execute(self, context: Context) -> None | str:
        """Start the sync using synchronous hook"""
        hook = self.hook
        hook.prep_connector(self._connector_id, self.schedule_type)
        last_sync = self._sync(hook)

        if not self.wait_for_completion:
            return last_sync

        last_sync_dt: pendulum.DateTime = pendulum.parse(last_sync)

        is_completed = self.hook.is_synced_after_target_time(
            self._connector_id,
            last_sync_dt,
            propagate_failures_forward=False,
            always_wait_when_syncing=True,
        )

        if is_completed:
            return None

        if self.deferrable:
            self.defer(
                timeout=self.execution_timeout,
                trigger=FivetranTrigger(
                    task_id=self.task_id,
                    fivetran_conn_id=self.fivetran_conn_id,
                    connector_id=self._connector_id,
                    poke_interval=self.poll_frequency,
                    reschedule_wait_time=self.reschedule_wait_time,
                    previous_completed_at=last_sync_dt,
                ),
                method_name="execute_complete",
            )

        self._wait_synchronously(last_sync_dt)
        return None

    def _wait_synchronously(self, last_sync: pendulum.DateTime) -> None:
        """
        Wait for the task synchronously.

        It is recommended that you do not use this, and instead set
        `deferrable=True` and use a Triggerer if you want to wait for the task
        to complete.
        """
        while True:
            is_completed = self.hook.is_synced_after_target_time(
                self._connector_id, last_sync, propagate_failures_forward=False, always_wait_when_syncing=True
            )
            if is_completed:
                return
            else:
                self.log.info("sync is still running...")
                self.log.info("sleeping for %s seconds.", self.poll_frequency)
                sleep(self.poll_frequency)

    @cached_property
    def hook(self) -> FivetranHook:
        """Create and return a FivetranHook."""
        return FivetranHook(
            self.fivetran_conn_id,
            retry_limit=self.fivetran_retry_limit,
            retry_delay=self.fivetran_retry_delay,
        )

    @cached_property
    def _connector_id(self) -> str:
        if self.connector_id:
            return self.connector_id
        elif self.connector_name and self.destination_name:
            return self.hook.get_connector_id(
                connector_name=self.connector_name, destination_name=self.destination_name
            )

        raise ValueError("No value specified for connector_id or to both connector_name and destination_name")

    def execute_complete(self, context: Context, event: Optional[Dict[Any, Any]] = None) -> None:
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

    def get_openlineage_facets_on_start(self):
        """
        Default extractor method that OpenLineage will call on execute completion.
        """
        from fivetran_provider_async import (
            DocumentationJobFacet,
            ErrorMessageRunFacet,
            OperatorLineage,
            OwnershipJobFacet,
            OwnershipJobFacetOwners,
        )

        # Should likely use the sync hook here to ensure that OpenLineage data is
        # returned before the timeout.
        hook = self.hook
        connector_response = hook.get_connector(self.connector_id)
        groups_response = hook.get_groups(connector_response["group_id"])
        destinations_response = hook.get_destinations(connector_response["group_id"])
        schema_response = hook.get_connector_schemas(self.connector_id)
        table_response = hook.get_metadata(self.connector_id, "tables")
        column_response = hook.get_metadata(self.connector_id, "columns")

        for schema in schema_response["schemas"].values():
            inputs = datasets(
                config=connector_response["config"],
                service=connector_response["service"],
                table_response=table_response,
                column_response=column_response,
                schema=schema,
                connector_id=self.connector_id,
                loc="source",
            )

            outputs = datasets(
                config=destinations_response["config"],
                service=destinations_response["service"],
                table_response=table_response,
                column_response=column_response,
                schema=schema,
                connector_id=self.connector_id,
                loc="destination",
            )

        job_facets = {
            "documentation": DocumentationJobFacet(
                description=f"""
                Fivetran run for service: {connector_response['service']}\n
                Group Name: {groups_response["name"]}\n
                Connector ID: {self.connector_id}
                """
            ),
            "ownership": OwnershipJobFacet(owners=[OwnershipJobFacetOwners(name=self.owner, type=self.email)]),
        }

        run_facets = {}
        if connector_response.get("failed_at"):
            run_facets["errorMessage"] = ErrorMessageRunFacet(
                message=f"Job failed at: {connector_response['failed_at']}",
                programmingLanguage="Fivetran",
            )

        return OperatorLineage(inputs=inputs, outputs=outputs, job_facets=job_facets, run_facets=run_facets)

    def get_openlineage_facets_on_complete(self, task_instance):
        return self.get_openlineage_facets_on_start()

    def _sync(self, hook: FivetranHook):
        return hook.start_fivetran_sync(connector_id=self._connector_id)


class FivetranResyncOperator(FivetranOperator):
    def __init__(self, scope: dict | None = None, **kwargs):
        super().__init__(**kwargs)
        self.scope = scope

    def _sync(self, hook: FivetranHook):
        return hook.start_fivetran_sync(
            connector_id=self._connector_id, mode="resync", payload={"scope": self.scope} if self.scope else None
        )
