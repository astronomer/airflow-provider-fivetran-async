from __future__ import annotations

from typing import TYPE_CHECKING, Any, Dict, Optional

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator, BaseOperatorLink

if TYPE_CHECKING:
    from airflow.utils.context import Context

from fivetran_provider_async.hooks import FivetranHook
from fivetran_provider_async.triggers import FivetranTrigger
from fivetran_provider_async.utils.operator_utils import datasets


class RegistryLink(BaseOperatorLink):
    """Link to Registry"""

    name = "Astronomer Registry"

    def get_link(self, operator, dttm):
        """Get link to registry page."""

        registry_link = "https://registry.astronomer.io/providers/{provider}/modules/{operator}"
        return registry_link.format(provider="fivetran", operator="fivetranoperator")


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
    :param connector_id: ID of the Fivetran connector to sync, found on the Connector settings page.
    :param schedule_type: schedule type. Default is "manual" which takes the connector off Fivetran schedule.
    :param poll_frequency: Time in seconds that the job should wait in between each try.
    :param reschedule_wait_time: Optional, if connector is in reset state,
            number of seconds to wait before restarting the sync.
    :param deferrable: Run operator in deferrable mode. Default is True.
    """

    operator_extra_links = (RegistryLink(),)

    template_fields = ["connector_id"]

    def __init__(
        self,
        connector_id: str,
        run_name: Optional[str] = None,
        timeout_seconds: Optional[int] = None,
        fivetran_conn_id: str = "fivetran",
        fivetran_retry_limit: int = 3,
        fivetran_retry_delay: int = 1,
        poll_frequency: int = 15,
        schedule_type: str = "manual",
        reschedule_wait_time: int = 0,
        deferrable: bool = True,
        **kwargs,
    ):
        self.connector_id = connector_id
        self.fivetran_conn_id = fivetran_conn_id
        self.run_name = run_name
        self.timeout_seconds = timeout_seconds
        self.fivetran_retry_limit = fivetran_retry_limit
        self.fivetran_retry_delay = fivetran_retry_delay
        self.poll_frequency = poll_frequency
        self.schedule_type = schedule_type
        self.reschedule_wait_time = reschedule_wait_time
        self.deferrable = deferrable
        super().__init__(**kwargs)

    def execute(self, context: Context) -> None | str:
        """Start the sync using synchronous hook"""
        hook = self._get_hook()
        hook.prep_connector(self.connector_id, self.schedule_type)
        last_sync = hook.start_fivetran_sync(self.connector_id)

        if not self.deferrable:
            return last_sync
        else:
            previous_completed_at = hook.get_last_sync(self.connector_id)
            completed = hook.get_sync_status(self.connector_id, previous_completed_at)
            if not completed:
                self.defer(
                    timeout=self.execution_timeout,
                    trigger=FivetranTrigger(
                        task_id=self.task_id,
                        fivetran_conn_id=self.fivetran_conn_id,
                        connector_id=self.connector_id,
                        poke_interval=self.poll_frequency,
                        reschedule_wait_time=self.reschedule_wait_time,
                    ),
                    method_name="execute_complete",
                )

    def _get_hook(self) -> FivetranHook:
        return FivetranHook(
            self.fivetran_conn_id,
            retry_limit=self.fivetran_retry_limit,
            retry_delay=self.fivetran_retry_delay,
        )

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
        hook = self._get_hook()
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
            "ownership": OwnershipJobFacet(
                owners=[OwnershipJobFacetOwners(name=self.owner, type=self.email)]
            ),
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


class FivetranOperatorAsync(FivetranOperator):
    """This operator has been deprecated. Please use `FivetranOperator`."""

    def __init__(self, *args, **kwargs):
        import warnings

        super().__init__(*args, **kwargs)

        warnings.warn(
            "FivetranOperatorAsync has been deprecated. Please use `FivetranOperator`.",
            DeprecationWarning,
            stacklevel=2,
        )
