from fivetran_provider_async.triggers.fivetran import FivetranTrigger
import pytest
from unittest import mock
import pendulum
import asyncio
from airflow.triggers.base import TriggerEvent
from airflow.exceptions import AirflowException

TASK_ID = "fivetran_sync_task"
POLLING_PERIOD_SECONDS = 4
CONNECTOR_ID = 'interchangeable_revenge'
FIVETRAN_CONN_ID = 'conn_fivetran'
PREV_COMPLETED_AT = pendulum.datetime(2021, 3, 23, 21, 55)
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
        "failed_at": "2021-03-22T20:55:12.670390Z",
        "sync_frequency": 360,
        "schedule_type": "manual",
        "status": {
            "setup_state": "connected",
            "sync_state": "scheduled",
            "update_state": "on_schedule",
            "is_historical_sync": False,
            "tasks": [],
            "warnings": []
        },
        "config": {
            "latest_version": "1",
            "sheet_id": "https://docs.google.com/spreadsheets/d/.../edit#gid=...",
            "named_range": "fivetran_test_range",
            "authorization_method": "User OAuth",
            "service_version": "1",
            "last_synced_changes__utc_": "2021-03-23 20:54"
        }
    }
}


def test_fivetran_trigger_serialization():
    """
    Asserts that the FivetranTrigger correctly serializes its arguments
    and classpath.
    """
    trigger = FivetranTrigger(
        task_id=TASK_ID,
        polling_period_seconds=POLLING_PERIOD_SECONDS,
        connector_id=CONNECTOR_ID,
        fivetran_conn_id=FIVETRAN_CONN_ID,
        previous_completed_at=PREV_COMPLETED_AT
    )
    classpath, kwargs = trigger.serialize()
    assert classpath == "fivetran_provider_async.triggers.fivetran.FivetranTrigger"
    assert kwargs == {
        "connector_id": 'interchangeable_revenge',
        'fivetran_conn_id': 'conn_fivetran',
        'poll_interval': 4.0,
        "polling_period_seconds": 4,
        "previous_completed_at": PREV_COMPLETED_AT,
        "task_id": "fivetran_sync_task",
    }


@pytest.mark.asyncio
@mock.patch("fivetran_provider_async.hooks.fivetran.FivetranHookAsync.get_sync_status_async")
@mock.patch("fivetran_provider_async.hooks.fivetran.FivetranHookAsync._do_api_call_async")
async def test_fivetran_trigger_completed(mock_api_call_async_response, mock_get_sync_status_async):  #
    mock_get_sync_status_async.return_value = "success"
    mock_api_call_async_response.return_value = MOCK_FIVETRAN_RESPONSE_PAYLOAD
    trigger = FivetranTrigger(
        task_id=TASK_ID,
        polling_period_seconds=POLLING_PERIOD_SECONDS,
        connector_id=CONNECTOR_ID,
        fivetran_conn_id=FIVETRAN_CONN_ID,
        previous_completed_at=PREV_COMPLETED_AT
    )
    generator = trigger.run()
    actual_response = await generator.asend(None)
    SUCCEEDED_AT = pendulum.parse(MOCK_FIVETRAN_RESPONSE_PAYLOAD["data"]["succeeded_at"])
    assert (
            TriggerEvent({"status": "success", "message": f"Fivetran connector {CONNECTOR_ID} finished "
                                                          f"syncing at {SUCCEEDED_AT}"}
                         )
            == actual_response
    )


@pytest.mark.asyncio
@mock.patch("fivetran_provider_async.hooks.fivetran.FivetranHookAsync.get_sync_status_async")
@mock.patch("fivetran_provider_async.hooks.fivetran.FivetranHookAsync._do_api_call_async")
async def test_fivetran_trigger_pending(mock_api_call_async_response, mock_get_sync_status_async):  #
    mock_get_sync_status_async.return_value = "pending"
    mock_api_call_async_response.return_value = MOCK_FIVETRAN_RESPONSE_PAYLOAD
    trigger = FivetranTrigger(
        task_id=TASK_ID,
        polling_period_seconds=POLLING_PERIOD_SECONDS,
        connector_id=CONNECTOR_ID,
        fivetran_conn_id=FIVETRAN_CONN_ID,
        previous_completed_at=PREV_COMPLETED_AT
    )
    task = asyncio.create_task(trigger.run().__anext__())
    await asyncio.sleep(0.5)
    # TriggerEvent was not returned
    assert task.done() is False


@pytest.mark.asyncio
@mock.patch("fivetran_provider_async.hooks.fivetran.FivetranHookAsync.get_sync_status_async")
@mock.patch("fivetran_provider_async.hooks.fivetran.FivetranHookAsync._do_api_call_async")
async def test_fivetran_trigger_failed(mock_api_call_async_response, mock_get_sync_status_async):  #
    mock_get_sync_status_async.return_value = "error"
    mock_api_call_async_response.return_value = MOCK_FIVETRAN_RESPONSE_PAYLOAD
    trigger = FivetranTrigger(
        task_id=TASK_ID,
        polling_period_seconds=POLLING_PERIOD_SECONDS,
        connector_id=CONNECTOR_ID,
        fivetran_conn_id=FIVETRAN_CONN_ID,
        previous_completed_at=PREV_COMPLETED_AT
    )
    generator = trigger.run()
    actual_response = await generator.asend(None)
    assert (
            TriggerEvent({"status": "error", "message": "error"}
                         )
            == actual_response
    )


@pytest.mark.asyncio
@mock.patch("fivetran_provider_async.hooks.fivetran.FivetranHookAsync.get_sync_status_async")
@mock.patch("fivetran_provider_async.hooks.fivetran.FivetranHookAsync._do_api_call_async")
async def test_fivetran_trigger_exception(mock_api_call_async_response, mock_get_sync_status_async):  #
    mock_get_sync_status_async.side_effect = AirflowException("fivetran unavailable")
    mock_api_call_async_response.return_value = MOCK_FIVETRAN_RESPONSE_PAYLOAD
    trigger = FivetranTrigger(
        task_id=TASK_ID,
        polling_period_seconds=POLLING_PERIOD_SECONDS,
        connector_id=CONNECTOR_ID,
        fivetran_conn_id=FIVETRAN_CONN_ID,
        previous_completed_at=PREV_COMPLETED_AT
    )
    generator = trigger.run()
    actual_response = await generator.asend(None)
    assert (
            TriggerEvent({"status": "error", "message": "fivetran unavailable"}
                         )
            == actual_response
    )
