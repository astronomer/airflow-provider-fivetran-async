import unittest
from unittest import mock

import pytest
import requests_mock
from airflow.exceptions import AirflowException, TaskDeferred

from fivetran_provider_async.operators import FivetranOperatorAsync


@pytest.fixture
def context():
    """
    Creates an empty context.
    """
    context = {}
    yield context


LOGIN = "login"
PASSWORD = "password"
MOCK_FIVETRAN_RESPONSE_PAYLOAD = {
    "code": "Success",
    "data": {
        "id": "interchangeable_revenge",
        "paused": False,
        "group_id": "rarer_gradient",
        "service": "google_sheets",
        "service_version": 1,
        "schema": "google_sheets.fivetran_google_sheets_spotify",
        "connected_by": "mournful_shalt",
        "created_at": "2021-03-05T22:58:56.238875Z",
        "succeeded_at": "2021-03-23T20:55:12.670390Z",
        "failed_at": "2021-03-22T20:55:12.670390Z",
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


@mock.patch.dict("os.environ", AIRFLOW_CONN_CONN_FIVETRAN="http://API_KEY:API_SECRET@")
class TestFivetranHook(unittest.TestCase):
    @requests_mock.mock()
    def test_fivetran_op_async_execute_success(self, m):
        """Tests that task gets deferred after job submission"""
        m.get(
            "https://api.fivetran.com/v1/connectors/interchangeable_revenge",
            json=MOCK_FIVETRAN_RESPONSE_PAYLOAD,
        )

        m.post(
            "https://api.fivetran.com/v1/connectors/interchangeable_revenge/force",
            json=MOCK_FIVETRAN_RESPONSE_PAYLOAD,
        )

        task = FivetranOperatorAsync(
            task_id="fivetran_op_async",
            fivetran_conn_id="conn_fivetran",
            connector_id="interchangeable_revenge",
        )
        with pytest.raises(TaskDeferred):
            task.execute(context)

    def test_fivetran_op_async_execute_complete_error(self):
        """Tests that execute_complete method raises exception in case of error"""
        task = FivetranOperatorAsync(
            task_id="fivetran_op_async",
            fivetran_conn_id="conn_fivetran",
            connector_id="interchangeable_revenge",
        )
        with pytest.raises(AirflowException) as exc_info:
            task.execute_complete(context=None, event={"status": "error", "message": "test failure message"})

        assert str(exc_info.value) == "error: test failure message"

    def test_fivetran_op_async_execute_complete_success(self):
        """Tests that execute_complete method returns expected result and that it prints expected log"""
        task = FivetranOperatorAsync(
            task_id="fivetran_op_async",
            fivetran_conn_id="conn_fivetran",
            connector_id="interchangeable_revenge",
        )
        expected_return_value = MOCK_FIVETRAN_RESPONSE_PAYLOAD["data"]["succeeded_at"]

        with mock.patch.object(task.log, "info") as mock_log_info:
            assert (
                task.execute_complete(
                    context=context,
                    event={
                        "status": "success",
                        "message": "Fivetran sync completed",
                        "return_value": MOCK_FIVETRAN_RESPONSE_PAYLOAD["data"]["succeeded_at"],
                    },
                )
                == expected_return_value
            )

        mock_log_info.assert_called_with("Fivetran sync completed")
