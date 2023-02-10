import unittest

from fivetran_provider_async.utils.operator_utils import (
    _get_fields,
    _get_openlineage_name,
    _get_openlineage_namespace,
    _get_table_id,
    datasets,
)
from tests.common.static import (
    MOCK_FIVETRAN_CONNECTOR_RESPONSE_PAYLOAD_GCS_TO_SNOWFLAKE,
    MOCK_FIVETRAN_DESTINATIONS_RESPONSE_PAYLOAD_GCS_TO_SNOWFLAKE,
    MOCK_FIVETRAN_DESTINATIONS_RESPONSE_PAYLOAD_SHEETS,
    MOCK_FIVETRAN_METADATA_COLUMNS_RESPONSE_PAYLOAD_SHEETS,
    MOCK_FIVETRAN_METADATA_TABLES_RESPONSE_PAYLOAD_SHEETS,
    MOCK_FIVETRAN_RESPONSE_PAYLOAD_SHEETS,
    MOCK_FIVETRAN_SCHEMA_RESPONSE_PAYLOAD_SHEETS,
    MOCK_FIVETRAN_SCHEMAS_RESPONSE_PAYLOAD_GCS_TO_SNOWFLAKE,
)

CONNECTOR_ID_SHEETS = "interchangeable_revenge"
CONNECTOR_ID_GCS_TO_SNOWFLAKE = "cropping_limitation"


class TestFivetranUtils(unittest.TestCase):
    def test_utils_get_table_id(self):
        schema_dict = next(iter(MOCK_FIVETRAN_SCHEMA_RESPONSE_PAYLOAD_SHEETS["data"]["schemas"].values()))
        table_name, table = next(iter(schema_dict["tables"].items()))
        table_id = _get_table_id(
            table_name,
            table,
            MOCK_FIVETRAN_METADATA_TABLES_RESPONSE_PAYLOAD_SHEETS["data"],
            "source",
        )
        assert table_id == "NjgyMDM0OQ"

    def test_utils_get_fields_source(self):
        schema_facet = _get_fields(
            "NjgyMDM0OQ",
            MOCK_FIVETRAN_METADATA_COLUMNS_RESPONSE_PAYLOAD_SHEETS["data"],
            "source",
        )
        assert len(schema_facet.fields) == 1
        assert schema_facet.fields[0].name == "column_1"
        assert schema_facet.fields[0].type == "String"

    def test_utils_get_fields_destination(self):
        schema_facet = _get_fields(
            "NjgyMDM0OQ",
            MOCK_FIVETRAN_METADATA_COLUMNS_RESPONSE_PAYLOAD_SHEETS["data"],
            "destination",
        )

        assert len(schema_facet.fields) == 1
        assert schema_facet.fields[0].name == "column_1_dest"
        assert schema_facet.fields[0].type == "VARCHAR(256)"

    def test_utils_get_openlineage_name(self):
        schema = next(iter(MOCK_FIVETRAN_SCHEMA_RESPONSE_PAYLOAD_SHEETS["data"]["schemas"].values()))
        openlineage_name_sheets = _get_openlineage_name(
            config=MOCK_FIVETRAN_RESPONSE_PAYLOAD_SHEETS["data"]["config"],
            service=MOCK_FIVETRAN_RESPONSE_PAYLOAD_SHEETS["data"]["service"],
            schema=schema,
            table="",
        )
        assert openlineage_name_sheets == "google_sheets.fivetran_google_sheets_spotify"

        openlineage_name_gcs_upstream = _get_openlineage_name(
            config=MOCK_FIVETRAN_CONNECTOR_RESPONSE_PAYLOAD_GCS_TO_SNOWFLAKE["data"]["config"],
            service="gcs",
            schema={},
            table="",
        )
        assert openlineage_name_gcs_upstream == "fivetran-demo/subscription_periods.csv"

        openlineage_name_snowflake_downstream = _get_openlineage_name(
            config=MOCK_FIVETRAN_DESTINATIONS_RESPONSE_PAYLOAD_GCS_TO_SNOWFLAKE["data"]["config"],
            service="snowflake",
            schema=next(
                iter(MOCK_FIVETRAN_SCHEMAS_RESPONSE_PAYLOAD_GCS_TO_SNOWFLAKE["data"]["schemas"].values())
            ),
            table="subscription_periods",
        )
        assert openlineage_name_snowflake_downstream == "test.demo.subscription_periods"

    def test_utils_get_openlineage_namespace(self):
        namespace_sheets = _get_openlineage_namespace(
            config=MOCK_FIVETRAN_RESPONSE_PAYLOAD_SHEETS["data"]["config"],
            service=MOCK_FIVETRAN_RESPONSE_PAYLOAD_SHEETS["data"]["service"],
            connector_id=CONNECTOR_ID_SHEETS,
        )
        assert namespace_sheets == "sheets://"

        namespace_snowflake = _get_openlineage_namespace(
            config=MOCK_FIVETRAN_DESTINATIONS_RESPONSE_PAYLOAD_GCS_TO_SNOWFLAKE["data"]["config"],
            service=MOCK_FIVETRAN_DESTINATIONS_RESPONSE_PAYLOAD_GCS_TO_SNOWFLAKE["data"]["service"],
            connector_id=CONNECTOR_ID_GCS_TO_SNOWFLAKE,
        )
        assert namespace_snowflake == "snowflake://hq12345.us-east-1.aws"

        namespace_gcs = _get_openlineage_namespace(
            config=MOCK_FIVETRAN_CONNECTOR_RESPONSE_PAYLOAD_GCS_TO_SNOWFLAKE["data"]["config"],
            service=MOCK_FIVETRAN_CONNECTOR_RESPONSE_PAYLOAD_GCS_TO_SNOWFLAKE["data"]["service"],
            connector_id=CONNECTOR_ID_GCS_TO_SNOWFLAKE,
        )
        assert namespace_gcs == "gs://test"

    def test_utils_input_dataset(self):
        dataset = datasets(
            config=MOCK_FIVETRAN_RESPONSE_PAYLOAD_SHEETS["data"]["config"],
            service=MOCK_FIVETRAN_RESPONSE_PAYLOAD_SHEETS["data"]["service"],
            table_response=MOCK_FIVETRAN_METADATA_TABLES_RESPONSE_PAYLOAD_SHEETS["data"],
            column_response=MOCK_FIVETRAN_METADATA_COLUMNS_RESPONSE_PAYLOAD_SHEETS["data"],
            schema=next(iter(MOCK_FIVETRAN_SCHEMA_RESPONSE_PAYLOAD_SHEETS["data"]["schemas"].values())),
            connector_id=CONNECTOR_ID_SHEETS,
            loc="source",
        )[0]
        assert dataset.namespace == "sheets://"
        assert dataset.name == "google_sheets.fivetran_google_sheets_spotify"
        schema_field = dataset.facets["schema"].fields[0]
        assert schema_field.name == "column_1"
        assert schema_field.type == "String"
        assert schema_field.description is None

    def test_utils_output_dataset(self):
        dataset = datasets(
            config=MOCK_FIVETRAN_DESTINATIONS_RESPONSE_PAYLOAD_SHEETS["data"]["config"],
            service=MOCK_FIVETRAN_DESTINATIONS_RESPONSE_PAYLOAD_SHEETS["data"]["service"],
            table_response=MOCK_FIVETRAN_METADATA_TABLES_RESPONSE_PAYLOAD_SHEETS["data"],
            column_response=MOCK_FIVETRAN_METADATA_COLUMNS_RESPONSE_PAYLOAD_SHEETS["data"],
            schema=next(iter(MOCK_FIVETRAN_SCHEMA_RESPONSE_PAYLOAD_SHEETS["data"]["schemas"].values())),
            connector_id=CONNECTOR_ID_SHEETS,
            loc="destination",
        )[0]
        assert dataset.namespace == "sheets://"
        assert dataset.name == "google_sheets.fivetran_google_sheets_spotify"
        schema_field = dataset.facets["schema"].fields[0]
        assert schema_field.name == "column_1_dest"
        assert schema_field.type == "VARCHAR(256)"
        assert schema_field.description is None
