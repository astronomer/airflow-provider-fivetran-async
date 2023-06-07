from unittest import mock

import pytest
from airflow.exceptions import AirflowException, TaskDeferred

from fivetran_provider_async.sensors import FivetranSensorAsync
from fivetran_provider_async.triggers import FivetranTrigger

TASK_ID = "fivetran_sensor_check"
POLLING_PERIOD_SECONDS = 1.0


@pytest.fixture
def context():
    """
    Creates an empty context.
    """
    context = {}
    yield context


def test_fivetran_sensor_async():
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


def test_fivetran_sensor_async_with_response_wait_time():
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


def test_fivetran_sensor_async_execute_failure(context):
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


def test_fivetran_sensor_async_execute_complete():
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
