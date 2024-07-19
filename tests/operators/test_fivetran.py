import logging
import unittest
from unittest import mock

import pytest
import requests_mock
from airflow.exceptions import AirflowException, TaskDeferred

from fivetran_provider_async.hooks import FivetranHook
from fivetran_provider_async.operators import FivetranOperator
from tests.common.static import (
    MOCK_FIVETRAN_DESTINATIONS_RESPONSE_PAYLOAD_SHEETS,
    MOCK_FIVETRAN_GROUPS_RESPONSE_PAYLOAD_SHEETS,
    MOCK_FIVETRAN_METADATA_COLUMNS_RESPONSE_PAYLOAD_SHEETS,
    MOCK_FIVETRAN_METADATA_TABLES_RESPONSE_PAYLOAD_SHEETS,
    MOCK_FIVETRAN_RESPONSE_PAYLOAD_SHEETS,
    MOCK_FIVETRAN_SCHEMA_RESPONSE_PAYLOAD_SHEETS,
)

log = logging.getLogger(__name__)


MOCK_FIVETRAN_RESPONSE_PAYLOAD = {
    "code": "Success",
    "data": {
        "id": "interchangeable_revenge",
        "group_id": "rarer_gradient",
        "service": "google_sheets",
        "service_version": 1,
        "schema": "google_sheets.fivetran_google_sheets_spotify",
        "connected_by": "mournful_shalt",
        "created_at": "2021-03-05T22:58:56.238875Z",
        "succeeded_at": "2021-03-23T20:55:12.670390Z",
        "failed_at": "null",
        "paused": False,
        "sync_frequency": 360,
        "schedule_type": "manual",
        "status": {
            "setup_state": "connected",
            "sync_state": "scheduled",
            "update_state": "on_schedule",
            "is_historical_sync": False,
            "tasks": [],
            "warnings": [],
        },
        "config": {
            "latest_version": "1",
            "sheet_id": "https://docs.google.com/spreadsheets/d/.../edit#gid=...",
            "named_range": "fivetran_test_range",
            "authorization_method": "User OAuth",
            "service_version": "1",
            "last_synced_changes__utc_": "2021-03-23 20:54",
        },
    },
}

MOCK_FIVETRAN_GROUPS_RESPONSE_PAYLOAD = {
    "code": "Success",
    "data": {
        "items": [
            {
                "id": "rarer_gradient",
                "name": "GoogleSheets",
                "created_at": "2022-12-12T17:14:33.790844Z",
            },
        ]
    },
}

MOCK_FIVETRAN_CONNECTORS_RESPONSE_PAYLOAD = {
    "code": "Success",
    "data": {
        "items": [
            {
                "id": "interchangeable_revenge",
                "group_id": "rarer_gradient",
                "service": "google_sheets",
                "service_version": 1,
                "schema": "google_sheets.fivetran_google_sheets_spotify",
                "connected_by": "mournful_shalt",
                "created_at": "2021-03-05T22:58:56.238875Z",
                "succeeded_at": "2021-03-23T20:55:12.670390Z",
                "failed_at": "null",
                "sync_frequency": 360,
                "status": {
                    "setup_state": "connected",
                    "sync_state": "scheduled",
                    "update_state": "on_schedule",
                    "is_historical_sync": False,
                    "tasks": [],
                    "warnings": [],
                },
            },
            {
                "id": "wicked_impressive",
                "group_id": "rarer_gradient",
                "service": "netsuite",
                "service_version": 1,
                "schema": "netsuite",
                "connected_by": "concerning_batch",
                "created_at": "2018-07-21T22:55:21.724201Z",
                "succeeded_at": "2018-12-26T17:58:18.245Z",
                "failed_at": "2018-08-24T15:24:58.872491Z",
                "sync_frequency": 60,
                "status": {
                    "setup_state": "connected",
                    "sync_state": "paused",
                    "update_state": "delayed",
                    "is_historical_sync": False,
                    "tasks": [],
                    "warnings": [],
                },
            },
        ]
    },
}


@pytest.fixture
def context():
    """
    Creates an empty context.
    """
    context = {}
    yield context


@mock.patch.dict("os.environ", AIRFLOW_CONN_CONN_FIVETRAN="http://API_KEY:API_SECRET@")
class TestFivetranOperator(unittest.TestCase):
    @requests_mock.mock()
    def test_fivetran_op_async_execute_success(self, m):
        """Tests that task gets deferred after job submission"""
        m.get(
            "https://api.fivetran.com/v1/connectors/interchangeable_revenge",
            json=MOCK_FIVETRAN_RESPONSE_PAYLOAD_SHEETS,
        )

        m.post(
            "https://api.fivetran.com/v1/connectors/interchangeable_revenge/force",
            json=MOCK_FIVETRAN_RESPONSE_PAYLOAD_SHEETS,
        )

        task = FivetranOperator(
            task_id="fivetran_op_async",
            fivetran_conn_id="conn_fivetran",
            connector_id="interchangeable_revenge",
        )
        with pytest.raises(TaskDeferred):
            task.execute(context)

    @requests_mock.mock()
    def test_fivetran_op_async_execute_success_reschedule_wait_time_and_manual_mode(self, m):
        """Tests that task gets deferred after job submission with reschedule wait time and manual mode."""
        m.get(
            "https://api.fivetran.com/v1/connectors/interchangeable_revenge",
            json=MOCK_FIVETRAN_RESPONSE_PAYLOAD_SHEETS,
        )

        m.post(
            "https://api.fivetran.com/v1/connectors/interchangeable_revenge/force",
            json=MOCK_FIVETRAN_RESPONSE_PAYLOAD_SHEETS,
        )

        task = FivetranOperator(
            task_id="fivetran_op_async",
            fivetran_conn_id="conn_fivetran",
            connector_id="interchangeable_revenge",
            reschedule_wait_time=60,
            schedule_type="manual",
        )
        with pytest.raises(TaskDeferred):
            task.execute(context)

    def test_fivetran_op_async_execute_complete_error(self):
        """Tests that execute_complete method raises exception in case of error"""
        task = FivetranOperator(
            task_id="fivetran_op_async",
            fivetran_conn_id="conn_fivetran",
            connector_id="interchangeable_revenge",
        )
        with pytest.raises(AirflowException) as exc_info:
            task.execute_complete(context=None, event={"status": "error", "message": "test failure message"})

        assert str(exc_info.value) == "error: test failure message"

    def test_fivetran_op_async_execute_complete_success(self):
        """Tests that execute_complete method returns expected result and that it prints expected log"""
        task = FivetranOperator(
            task_id="fivetran_op_async",
            fivetran_conn_id="conn_fivetran",
            connector_id="interchangeable_revenge",
        )
        expected_return_value = MOCK_FIVETRAN_RESPONSE_PAYLOAD_SHEETS["data"]["succeeded_at"]

        with mock.patch.object(task.log, "info") as mock_log_info:
            assert (
                task.execute_complete(
                    context=context,
                    event={
                        "status": "success",
                        "message": "Fivetran sync completed",
                        "return_value": MOCK_FIVETRAN_RESPONSE_PAYLOAD_SHEETS["data"]["succeeded_at"],
                    },
                )
                == expected_return_value
            )

        mock_log_info.assert_called_with("Fivetran sync completed")

    @requests_mock.mock()
    def test_fivetran_operator_get_openlineage_facets_on_start(self, m):
        m.get(
            "https://api.fivetran.com/v1/connectors/interchangeable_revenge/schemas",
            json=MOCK_FIVETRAN_SCHEMA_RESPONSE_PAYLOAD_SHEETS,
        )
        m.get(
            "https://api.fivetran.com/v1/connectors/interchangeable_revenge",
            json=MOCK_FIVETRAN_RESPONSE_PAYLOAD_SHEETS,
        )
        m.get(
            "https://api.fivetran.com/v1/metadata/connectors/interchangeable_revenge/tables",
            json=MOCK_FIVETRAN_METADATA_TABLES_RESPONSE_PAYLOAD_SHEETS,
        )
        m.get(
            "https://api.fivetran.com/v1/metadata/connectors/interchangeable_revenge/columns",
            json=MOCK_FIVETRAN_METADATA_COLUMNS_RESPONSE_PAYLOAD_SHEETS,
        )
        m.get(
            "https://api.fivetran.com/v1/destinations/rarer_gradient",
            json=MOCK_FIVETRAN_DESTINATIONS_RESPONSE_PAYLOAD_SHEETS,
        )
        m.get(
            "https://api.fivetran.com/v1/groups/rarer_gradient",
            json=MOCK_FIVETRAN_GROUPS_RESPONSE_PAYLOAD_SHEETS,
        )

        operator = FivetranOperator(
            task_id="fivetran-task",
            fivetran_conn_id="conn_fivetran",
            connector_id="interchangeable_revenge",
        )

        facets = operator.get_openlineage_facets_on_start()
        assert facets.inputs[0].namespace == "sheets://"
        assert facets.inputs[0].name == "google_sheets.fivetran_google_sheets_spotify"
        schema_field = facets.outputs[0].facets["schema"].fields[0]
        assert schema_field.name == "column_1_dest"
        assert schema_field.type == "VARCHAR(256)"
        assert schema_field.description is None

    def test_default_conn_name(self):
        task = FivetranOperator(
            task_id="fivetran_op_async",
            connector_id="interchangeable_revenge",
            reschedule_wait_time=60,
            schedule_type="manual",
        )
        assert task.fivetran_conn_id == FivetranHook.default_conn_name

    @requests_mock.mock()
    def test_fivetran_op_without_connector_id(self, m):
        """Tests that execute_complete method returns expected result and that it prints expected log"""
        m.get(
            "https://api.fivetran.com/v1/groups/",
            json=MOCK_FIVETRAN_GROUPS_RESPONSE_PAYLOAD,
        )
        m.get(
            "https://api.fivetran.com/v1/groups/rarer_gradient/connectors/",
            json=MOCK_FIVETRAN_CONNECTORS_RESPONSE_PAYLOAD,
        )

        m.get(
            "https://api.fivetran.com/v1/connectors/interchangeable_revenge",
            json=MOCK_FIVETRAN_RESPONSE_PAYLOAD_SHEETS,
        )

        m.post(
            "https://api.fivetran.com/v1/connectors/interchangeable_revenge/force",
            json=MOCK_FIVETRAN_RESPONSE_PAYLOAD_SHEETS,
        )

        task = FivetranOperator(
            task_id="fivetran_op_async",
            fivetran_conn_id="conn_fivetran",
            connector_name="google_sheets.fivetran_google_sheets_spotify",
            destination_name="GoogleSheets",
            reschedule_wait_time=60,
            schedule_type="manual",
        )
        with pytest.raises(TaskDeferred):
            task.execute(context)

        expected_return_value = MOCK_FIVETRAN_RESPONSE_PAYLOAD_SHEETS["data"]["succeeded_at"]

        with mock.patch.object(task.log, "info") as mock_log_info:
            assert (
                task.execute_complete(
                    context=context,
                    event={
                        "status": "success",
                        "message": "Fivetran sync completed",
                        "return_value": MOCK_FIVETRAN_RESPONSE_PAYLOAD_SHEETS["data"]["succeeded_at"],
                    },
                )
                == expected_return_value
            )

        mock_log_info.assert_called_with("Fivetran sync completed")

    def test_fivetran_op_without_connector_id_error(self):
        """Tests that execute_complete method raises exception in case of error"""
        with pytest.raises(ValueError) as exc:
            FivetranOperator(
                task_id="fivetran_op_async",
                fivetran_conn_id="conn_fivetran",
            )

        assert (
            str(exc.value)
            == "No value specified for connector_id or to both connector_name and destination_name"
        )
