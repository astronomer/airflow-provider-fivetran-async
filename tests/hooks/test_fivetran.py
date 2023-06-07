from unittest import mock

import multidict
import pendulum
import pytest
from aiohttp import ClientResponseError, RequestInfo
from airflow.exceptions import AirflowException

from fivetran_provider_async.hooks import FivetranHookAsync
from tests.common.static import (
    LOGIN,
    MOCK_FIVETRAN_RESPONSE_PAYLOAD_SHEETS,
    MOCK_FIVETRAN_RESPONSE_PAYLOAD_SHEETS_RESCHEDULE_MODE,
    MOCK_FIVETRAN_RESPONSE_PAYLOAD_SHEETS_WITH_RESCHEDULE_FOR,
    PASSWORD,
)


@pytest.mark.asyncio
@mock.patch("fivetran_provider_async.hooks.FivetranHookAsync._do_api_call_async")
async def test_fivetran_hook_get_connector_async(mock_api_call_async_response):
    """Tests that the get_connector_async method fetches the details of a connector"""
    hook = FivetranHookAsync(fivetran_conn_id="conn_fivetran")
    mock_api_call_async_response.return_value = MOCK_FIVETRAN_RESPONSE_PAYLOAD_SHEETS
    result = await hook.get_connector_async(connector_id="interchangeable_revenge")
    assert result["status"]["setup_state"] == "connected"


@pytest.mark.asyncio
@mock.patch("fivetran_provider_async.hooks.FivetranHookAsync._do_api_call_async")
async def test_fivetran_hook_get_connector_async_error(mock_api_call_async_response):
    """Tests that the get_connector_async method raises exception when connector_id is not specified"""
    hook = FivetranHookAsync(fivetran_conn_id="conn_fivetran")
    mock_api_call_async_response.return_value = MOCK_FIVETRAN_RESPONSE_PAYLOAD_SHEETS
    with pytest.raises(ValueError) as exc:
        await hook.get_connector_async(connector_id="")
    assert str(exc.value) == "No value specified for connector_id"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_previous_completed_at, expected_result",
    [
        (
            pendulum.datetime(2021, 3, 23),  # current_completed_at > previous_completed_at
            "success",
        ),
        (
            pendulum.datetime(2021, 3, 23, 21, 55),  # current_completed_at < previous_completed_at
            "pending",
        ),
    ],
)
@mock.patch("fivetran_provider_async.hooks.FivetranHookAsync._do_api_call_async")
async def test_fivetran_hook_get_sync_status_async(
    mock_api_call_async_response, mock_previous_completed_at, expected_result
):
    """Tests that get_sync_status_async method return success or pending depending on whether
    current_completed_at > previous_completed_at"""
    hook = FivetranHookAsync(fivetran_conn_id="conn_fivetran")
    mock_api_call_async_response.return_value = MOCK_FIVETRAN_RESPONSE_PAYLOAD_SHEETS
    result = await hook.get_sync_status_async(
        connector_id="interchangeable_revenge",
        previous_completed_at=mock_previous_completed_at,
        reschedule_wait_time=60,
    )
    assert result == expected_result


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_previous_completed_at, expected_result",
    [
        (
            pendulum.datetime(2021, 3, 23),  # current_completed_at > previous_completed_at
            "success",
        ),
        (
            pendulum.datetime(2021, 3, 23, 21, 55),  # current_completed_at < previous_completed_at
            "pending",
        ),
    ],
)
@mock.patch("fivetran_provider_async.hooks.FivetranHookAsync._do_api_call_async")
async def test_fivetran_hook_get_sync_status_async_with_reschedule_mode_error_for_wait_time(
    mock_api_call_async_response, mock_previous_completed_at, expected_result
):
    """Tests that get_sync_status_async method return error with rescheduled_for in Fivetran API response
    along with schedule_type as manual and negative wait time."""
    hook = FivetranHookAsync(fivetran_conn_id="conn_fivetran")
    mock_api_call_async_response.return_value = MOCK_FIVETRAN_RESPONSE_PAYLOAD_SHEETS_RESCHEDULE_MODE
    with pytest.raises(ValueError, match="Sync connector manually."):
        await hook.get_sync_status_async(
            connector_id="interchangeable_revenge",
            previous_completed_at=mock_previous_completed_at,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_previous_completed_at, expected_result",
    [
        (
            pendulum.datetime(2021, 3, 23),  # current_completed_at > previous_completed_at
            "success",
        ),
        (
            pendulum.datetime(2021, 3, 23, 21, 55),  # current_completed_at < previous_completed_at
            "pending",
        ),
    ],
)
@mock.patch("fivetran_provider_async.hooks.FivetranHookAsync._do_api_call_async")
@mock.patch("fivetran_provider_async.hooks.FivetranHookAsync.start_fivetran_sync")
async def test_fivetran_hook_get_sync_status_async_with_reschedule_mode(
    mock_start_fivetran_sync, mock_api_call_async_response, mock_previous_completed_at, expected_result
):
    """Tests that get_sync_status_async method return success or pending depending on whether
    current_completed_at > previous_completed_at with reschedule_time specified by user and
    schedule_type as manual in API response."""
    mock_start_fivetran_sync.return_value = pendulum.datetime(2021, 3, 21, 21, 55)
    hook = FivetranHookAsync(fivetran_conn_id="conn_fivetran")
    mock_api_call_async_response.return_value = MOCK_FIVETRAN_RESPONSE_PAYLOAD_SHEETS_RESCHEDULE_MODE
    result = await hook.get_sync_status_async(
        connector_id="interchangeable_revenge",
        previous_completed_at=mock_previous_completed_at,
        reschedule_wait_time=10,
    )

    assert result == expected_result


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mock_previous_completed_at, expected_result",
    [
        (
            pendulum.datetime(2021, 3, 23),  # current_completed_at > previous_completed_at
            "success",
        ),
        (
            pendulum.datetime(2021, 3, 23, 21, 55),  # current_completed_at < previous_completed_at
            "pending",
        ),
    ],
)
@mock.patch("fivetran_provider_async.hooks.FivetranHookAsync._do_api_call_async")
@mock.patch("fivetran_provider_async.hooks.FivetranHookAsync.start_fivetran_sync")
async def test_fivetran_hook_get_sync_status_async_with_reschedule_for_and_schedule_type_manual(
    mock_start_fivetran_sync, mock_api_call_async_response, mock_previous_completed_at, expected_result
):
    """Tests that get_sync_status_async method return success or pending depending on whether
    current_completed_at > previous_completed_at with reschedule_for in Fivetran API response
    along with schedule_type as manual."""
    mock_start_fivetran_sync.return_value = pendulum.datetime(2021, 3, 21, 21, 55)
    hook = FivetranHookAsync(fivetran_conn_id="conn_fivetran")
    mock_api_call_async_response.return_value = MOCK_FIVETRAN_RESPONSE_PAYLOAD_SHEETS_WITH_RESCHEDULE_FOR
    result = await hook.get_sync_status_async(
        connector_id="interchangeable_revenge",
        previous_completed_at=mock_previous_completed_at,
    )

    assert result == expected_result


@pytest.mark.asyncio
@mock.patch("fivetran_provider_async.hooks.FivetranHookAsync._do_api_call_async")
async def test_fivetran_hook_get_sync_status_async_exception(mock_api_call_async_response):
    """Tests that get_sync_status_async method raises exception  when failed_at > previous_completed_at"""
    mock_previous_completed_at = pendulum.datetime(2021, 3, 21, 21, 55)
    hook = FivetranHookAsync(fivetran_conn_id="conn_fivetran")
    mock_api_call_async_response.return_value = MOCK_FIVETRAN_RESPONSE_PAYLOAD_SHEETS

    with pytest.raises(AirflowException) as exc:
        await hook.get_sync_status_async(
            connector_id="interchangeable_revenge", previous_completed_at=mock_previous_completed_at
        )
    assert "Fivetran sync for connector interchangeable_revenge failed" in str(exc.value)


@pytest.mark.asyncio
@mock.patch("fivetran_provider_async.hooks.FivetranHookAsync.start_fivetran_sync")
@mock.patch("fivetran_provider_async.hooks.FivetranHookAsync._do_api_call_async")
async def test_fivetran_hook_pause_and_restart(mock_api_call_async_response, mock_start_fivetran_sync):
    """Tests that pause_and_restart method for manual mode with reschedule time set."""
    hook = FivetranHookAsync(fivetran_conn_id="conn_fivetran")
    mock_start_fivetran_sync.return_value = True
    mock_api_call_async_response.return_value = MOCK_FIVETRAN_RESPONSE_PAYLOAD_SHEETS

    result = hook.pause_and_restart(
        connector_id="interchangeable_revenge", reschedule_for="manual", reschedule_wait_time=60
    )
    assert result is True


@pytest.mark.asyncio
@mock.patch("fivetran_provider_async.hooks.FivetranHookAsync._do_api_call_async")
async def test_fivetran_hook_get_last_sync_async_no_xcom(mock_api_call_async_response):
    """Tests that the get_last_sync_async method returns the last time Fivetran connector
    completed a sync"""
    hook = FivetranHookAsync(fivetran_conn_id="conn_fivetran")
    mock_api_call_async_response.return_value = MOCK_FIVETRAN_RESPONSE_PAYLOAD_SHEETS
    result = await hook.get_last_sync_async(connector_id="interchangeable_revenge")
    assert result == pendulum.parse(MOCK_FIVETRAN_RESPONSE_PAYLOAD_SHEETS["data"]["succeeded_at"])


@pytest.mark.asyncio
@mock.patch("fivetran_provider_async.hooks.FivetranHookAsync._do_api_call_async")
async def test_fivetran_hook_get_last_sync_async_with_xcom(mock_api_call_async_response):
    """Tests that the get_last_sync_async method returns the last time Fivetran connector
    completed a sync when xcom is passed"""
    XCOM = "2021-03-22T20:55:12.670390Z"
    hook = FivetranHookAsync(fivetran_conn_id="conn_fivetran")
    mock_api_call_async_response.return_value = MOCK_FIVETRAN_RESPONSE_PAYLOAD_SHEETS
    result = await hook.get_last_sync_async(connector_id="interchangeable_revenge", xcom=XCOM)
    assert result == pendulum.parse(XCOM)


@pytest.mark.asyncio
@mock.patch("fivetran_provider_async.hooks.aiohttp.ClientSession")
@mock.patch("fivetran_provider_async.hooks.FivetranHookAsync.get_connection")
async def test_do_api_call_async_get_method_with_success(mock_get_connection, mock_session):
    """Tests that _do_api_call_async method returns correct response when GET request
    is successful"""

    async def mock_fun(arg1, arg2, arg3, arg4):
        return {"status": "success"}

    mock_session.return_value.__aexit__.return_value = mock_fun
    mock_session.return_value.__aenter__.return_value.get.return_value.json.return_value = {
        "status": "success"
    }

    hook = FivetranHookAsync(fivetran_conn_id="conn_fivetran")

    hook.fivetran_conn = mock_get_connection
    hook.fivetran_conn.login = LOGIN
    hook.fivetran_conn.password = PASSWORD
    response = await hook._do_api_call_async(("GET", "v1/connectors/test"))
    assert response == {"status": "success"}


@pytest.mark.asyncio
@mock.patch("fivetran_provider_async.hooks.aiohttp.ClientSession")
@mock.patch("fivetran_provider_async.hooks.FivetranHookAsync.get_connection")
async def test_do_api_call_async_patch_method_with_success(mock_get_connection, mock_session):
    """Tests that _do_api_call_async method returns correct response when PATCH request
    is successful"""

    async def mock_fun(arg1, arg2, arg3, arg4):
        return {"status": "success"}

    mock_session.return_value.__aexit__.return_value = mock_fun
    mock_session.return_value.__aenter__.return_value.patch.return_value.json.return_value = {
        "status": "success"
    }

    hook = FivetranHookAsync(fivetran_conn_id="conn_fivetran")

    hook.fivetran_conn = mock_get_connection
    hook.fivetran_conn.login = LOGIN
    hook.fivetran_conn.password = PASSWORD
    response = await hook._do_api_call_async(("PATCH", "v1/connectors/test"))
    assert response == {"status": "success"}


@pytest.mark.asyncio
@mock.patch("fivetran_provider_async.hooks.aiohttp.ClientSession")
@mock.patch("fivetran_provider_async.hooks.FivetranHookAsync.get_connection")
async def test_do_api_call_async_post_method_with_success(mock_get_connection, mock_session):
    """Tests that _do_api_call_async method returns correct response when POST request
    is successful"""

    async def mock_fun(arg1, arg2, arg3, arg4):
        return {"status": "success"}

    mock_session.return_value.__aexit__.return_value = mock_fun
    mock_session.return_value.__aenter__.return_value.post.return_value.json.return_value = {
        "status": "success"
    }

    hook = FivetranHookAsync(fivetran_conn_id="conn_fivetran")

    hook.fivetran_conn = mock_get_connection
    hook.fivetran_conn.login = LOGIN
    hook.fivetran_conn.password = PASSWORD
    response = await hook._do_api_call_async(("POST", "v1/connectors/test"))
    assert response == {"status": "success"}


@pytest.mark.asyncio
@mock.patch("fivetran_provider_async.hooks.aiohttp.ClientSession")
@mock.patch("fivetran_provider_async.hooks.FivetranHookAsync.get_connection")
async def test_do_api_call_async_unexpected_method_error(mock_get_connection, mock_session):
    """Tests that _do_api_call_async method raises exception when a wrong request is sent"""
    hook = FivetranHookAsync(fivetran_conn_id="conn_fivetran")

    hook.fivetran_conn = mock_get_connection
    hook.fivetran_conn.login = LOGIN
    hook.fivetran_conn.password = PASSWORD
    with pytest.raises(AirflowException) as exc:
        await hook._do_api_call_async(("UNKNOWN", "v1/connectors/test"))
    assert str(exc.value) == "Unexpected HTTP Method: UNKNOWN"


@pytest.mark.asyncio
@mock.patch("fivetran_provider_async.hooks.aiohttp.ClientSession")
@mock.patch("fivetran_provider_async.hooks.FivetranHookAsync.get_connection")
async def test_do_api_call_async_with_non_retryable_client_response_error(mock_get_connection, mock_session):
    """Tests that _do_api_call_async method returns expected response for a non retryable error"""
    mock_session.return_value.__aenter__.return_value.patch.return_value.json.side_effect = (
        ClientResponseError(
            request_info=RequestInfo(url="example.com", method="PATCH", headers=multidict.CIMultiDict()),
            status=400,
            message="test message",
            history=[],
        )
    )

    hook = FivetranHookAsync(fivetran_conn_id="conn_fivetran")

    hook.fivetran_conn = mock_get_connection
    hook.fivetran_conn.login = LOGIN
    hook.fivetran_conn.password = PASSWORD

    resp = await hook._do_api_call_async(("PATCH", "v1/connectors/test"))
    assert resp == {"Response": {"test message"}, "Status Code": {400}}


@pytest.mark.asyncio
@mock.patch("fivetran_provider_async.hooks.aiohttp.ClientSession")
@mock.patch("fivetran_provider_async.hooks.FivetranHookAsync.get_connection")
async def test_do_api_call_async_with_retryable_client_response_error(mock_get_connection, mock_session):
    """Tests that _do_api_call_async method raises exception for a retryable error"""
    mock_session.return_value.__aenter__.return_value.patch.return_value.json.side_effect = (
        ClientResponseError(
            request_info=RequestInfo(url="example.com", method="PATCH", headers=multidict.CIMultiDict()),
            status=500,
            message="test message",
            history=[],
        )
    )

    hook = FivetranHookAsync(fivetran_conn_id="conn_fivetran")

    hook.fivetran_conn = mock_get_connection
    hook.fivetran_conn.login = LOGIN
    hook.fivetran_conn.password = PASSWORD

    with pytest.raises(AirflowException) as exc:
        await hook._do_api_call_async(("PATCH", "v1/connectors/test"))

    assert str(exc.value) == "API requests to Fivetran failed 3 times. Giving up."
