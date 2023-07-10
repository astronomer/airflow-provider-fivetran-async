import logging
import unittest
from unittest import mock

import pytest
import requests_mock
from airflow.exceptions import AirflowException, TaskDeferred

from fivetran_provider_async.sensors import FivetranSensor, FivetranSensorAsync
from fivetran_provider_async.triggers import FivetranTrigger

TASK_ID = "fivetran_sensor_check"
POLLING_PERIOD_SECONDS = 1.0

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


@pytest.fixture
def context():
    """
    Creates an empty context.
    """
    context = {}
    yield context


class TestFivetranSensorAsync:
    def test_fivetran_sensor_async(self):
        """Asserts that a task is deferred and a FivetranTrigger will be fired
        when the FivetranSensorAsync is executed."""
        task = FivetranSensorAsync(
            task_id=TASK_ID,
            fivetran_conn_id="fivetran_default",
            connector_id="test_connector",
            poke_interval=5,
        )
        with pytest.raises(TaskDeferred) as exc:
            task.execute(context)
        assert isinstance(exc.value.trigger, FivetranTrigger), "Trigger is not a FivetranTrigger"

    def test_fivetran_sensor_async_with_response_wait_time(self):
        """Asserts that a task is deferred and a FivetranTrigger will be fired
        when the FivetranSensorAsync is executed when reschedule_wait_time is specified."""
        task = FivetranSensorAsync(
            task_id=TASK_ID,
            fivetran_conn_id="fivetran_default",
            connector_id="test_connector",
            poke_interval=5,
            reschedule_wait_time=60,
        )
        with pytest.raises(TaskDeferred) as exc:
            task.execute(context)
        assert isinstance(exc.value.trigger, FivetranTrigger), "Trigger is not a FivetranTrigger"

    def test_fivetran_sensor_async_execute_failure(self, context):
        """Tests that an AirflowException is raised in case of error event"""
        task = FivetranSensorAsync(
            task_id=TASK_ID,
            fivetran_conn_id="fivetran_default",
            connector_id="test_connector",
            poke_interval=5,
        )
        with pytest.raises(AirflowException) as exc:
            task.execute_complete(
                context=None, event={"status": "error", "message": "Fivetran connector sync failure"}
            )
        assert str(exc.value) == "error: Fivetran connector sync failure"

    def test_fivetran_sensor_async_execute_complete(self):
        """Asserts that logging occurs as expected"""
        task = FivetranSensorAsync(
            task_id=TASK_ID,
            fivetran_conn_id="fivetran_default",
            connector_id="test_connector",
            poke_interval=5,
        )
        with mock.patch.object(task.log, "info") as mock_log_info:
            task.execute_complete(
                context=None, event={"status": "success", "message": "Fivetran connector finished syncing"}
            )
        mock_log_info.assert_called_with("Fivetran connector finished syncing")


# Mock the `conn_fivetran` Airflow connection (note the `@` after `API_SECRET`)
@mock.patch.dict("os.environ", AIRFLOW_CONN_CONN_FIVETRAN="http://API_KEY:API_SECRET@")
class TestFivetranSensor(unittest.TestCase):
    """
    Test functions for Fivetran Operator.

    Mocks responses from Fivetran API.
    """

    @mock.patch.object(FivetranSensor, "poke", "returned_sync_status")
    @requests_mock.mock()
    def test_del(self, m):
        sensor = FivetranSensor(
            task_id="my_fivetran_sensor",
            fivetran_conn_id="conn_fivetran",
            connector_id="interchangeable_revenge",
        )

        log.info(sensor.poke)
        assert sensor.poke == "returned_sync_status"
