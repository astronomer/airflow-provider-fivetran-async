import unittest

from fivetran_provider_async.utils.operator_utils import (
    _get_table_id,
    _get_fields,
    _get_openlineage_name,
    _get_openlineage_namespace,
    get_dataset
)
from tests.common.static import (
    MOCK_FIVETRAN_RESPONSE_PAYLOAD,
    MOCK_FIVETRAN_METADATA_TABLES_RESPONSE_PAYLOAD,
    MOCK_FIVETRAN_METADATA_COLUMNS_RESPONSE_PAYLOAD,
    MOCK_FIVETRAN_SCHEMA_RESPONSE_PAYLOAD,
    MOCK_FIVETRAN_DESTINATIONS_RESPONSE_PAYLOAD
)


class TestFivetranOperator(unittest.TestCase):

    def test_utils_get_table_id(self):
        schema_dict = next(iter(MOCK_FIVETRAN_SCHEMA_RESPONSE_PAYLOAD["data"]["schemas"].values()))
        table_name, table = next(iter(schema_dict["tables"].items()))
        table_id = _get_table_id(
            table_name,
            table,
            MOCK_FIVETRAN_METADATA_TABLES_RESPONSE_PAYLOAD["data"],
            "source",
        )
        assert table_id == "NjgyMDM0OQ"

    def test_utils_get_fields_source(self):
        schema_facet = _get_fields(
            "NjgyMDM0OQ",
            MOCK_FIVETRAN_METADATA_COLUMNS_RESPONSE_PAYLOAD["data"],
            "source",
        )
        assert len(schema_facet.fields) == 1
        assert schema_facet.fields[0].name == "column_1"
        assert schema_facet.fields[0].type == "String"

    def test_utils_get_fields_destination(self):
        schema_facet = _get_fields(
            "NjgyMDM0OQ",
            MOCK_FIVETRAN_METADATA_COLUMNS_RESPONSE_PAYLOAD["data"],
            "destination",
        )

        assert len(schema_facet.fields) == 1 
        assert schema_facet.fields[0].name == "column_1_dest"
        assert schema_facet.fields[0].type == "VARCHAR(256)"

    # TODO: Add tests for _get_openlineage_name, _get_openlineage_namespace & get_dataset

    """ Not going to be useful tests until we have test responses for more than just Google Sheets.
    def test_utils_get_openlineage_name(self):
        openlineage_name = _get_openlineage_name(
            config=MOCK_FIVETRAN_RESPONSE_PAYLOAD["data"]["config"],
            service=
        )

    def test_utils_get_openlineage_namespace(self):
    """
