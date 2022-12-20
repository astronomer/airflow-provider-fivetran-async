from typing import Any, Dict, Optional

from airflow.exceptions import AirflowException
from airflow.utils.context import Context
from fivetran_provider.operators.fivetran import FivetranOperator
from openlineage.airflow.extractors.base import BaseExtractor, OperatorLineage
from openlineage.client.run import Dataset
from openlineage.client.facet import DataSourceDatasetFacet, SchemaDatasetFacet, SchemaField

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

    def _get_fields(self, table) -> Optional[SchemaField]:
        if table.get("columns"):
            return SchemaDatasetFacet(fields=[
                SchemaField(
                    name=col["name_in_destination"],
                    type="",
                )
                for col in table["columns"].values()
            ])
        return None

    def _get_input_name(self, config, service) -> str:
        if service == "gcs":
            return f"{config['bucket']}/{config['prefix']}{config['pattern']}"
        elif service == "google_sheets":
            return config['sheet_id']
        else:
            raise ValueError(f"Service: {service} not supported by extractor.")

    def get_openlineage_facets_on_complete(self, task_instance) -> OperatorLineage:
        """
        Default extractor method that OpenLineage will call on execute completion.
        """
        # Should likely use the sync hook here to ensure that OpenLineage data is
        # returned before the timeout.
        hook = self._get_hook()
        connector_resp = hook.get_connector(self.connector_id)
        schema_resp = hook.get_connector_schemas(self.connector_id)
        config = connector_resp["config"]
        input_name = self._get_input_name(config, connector_resp["service"])
        namespace = "fivetran"
        inputs = []
        outputs = []

        for schema in schema_resp["schemas"].values():
            source = DataSourceDatasetFacet(
                name="fivetran",
                uri=BaseExtractor.get_connection_uri(hook.fivetran_conn),
            )

            # Assumes a Fivetran config will only ever have one source.
            inputs.append(
                Dataset(
                    namespace=namespace,
                    name=input_name,
                    facets={"DataSourceDatasetFacet": source}
                )
            )

            # Assumes a Fivetran sync can have multiple outputs.
            outputs.extend([
                Dataset(
                    namespace="fivetran",
                    name=table["name_in_destination"],
                    facets={
                        "SchemaDatasetFacet": self._get_fields(table),
                        "DataSourceDatasetFacet": source
                    }
                    
                )
                for table in schema["tables"].values()
            ])

        job_facets = {}
        run_facets = {}

        return OperatorLineage(
            inputs=inputs,
            outputs=outputs,
            job_facets=job_facets,
            run_facets=run_facets
        )

