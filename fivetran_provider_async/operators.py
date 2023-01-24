from typing import Any, Dict, Optional

from airflow.exceptions import AirflowException
from airflow.utils.context import Context
from fivetran_provider.operators.fivetran import FivetranOperator

from fivetran_provider_async.triggers import FivetranTrigger
from fivetran_provider_async.utils.operator_utils import datasets


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
    :param poll_frequency: Time in seconds that the job should wait in
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
