import unittest
from unittest import mock

import multidict
import pendulum
import pytest
import requests_mock
from aiohttp import ClientResponseError, RequestInfo
from airflow.exceptions import AirflowException

from fivetran_provider_async.hooks import FivetranHook, FivetranHookAsync
from tests.common.static import (
    LOGIN,
    MOCK_FIVETRAN_RESPONSE_PAYLOAD_SHEETS,
    MOCK_FIVETRAN_RESPONSE_PAYLOAD_SHEETS_RESCHEDULE_MODE,
    MOCK_FIVETRAN_RESPONSE_PAYLOAD_SHEETS_WITH_RESCHEDULE_FOR,
    PASSWORD,
)

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
        "pause_after_trial": False,
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

MOCK_FIVETRAN_SCHEMA_RESPONSE_PAYLOAD = {
    "code": "Success",
    "data": {
        "enable_new_by_default": True,
        "schema_change_handling": "ALLOW_ALL",
        "schemas": {
            "google_sheets.fivetran_google_sheets_spotify": {
                "name_in_destination": "google_sheets.fivetran_google_sheets_spotify",
                "enabled": True,
                "tables": {
                    "table_1": {
                        "name_in_destination": "table_1",
                        "enabled": True,
                        "sync_mode": "SOFT_DELETE",
                        "enabled_patch_settings": {"allowed": True},
                        "columns": {
                            "column_1": {
                                "name_in_destination": "column_1",
                                "enabled": True,
                                "hashed": False,
                                "enabled_patch_settings": {
                                    "allowed": False,
                                    "reason_code": "SYSTEM_COLUMN",
                                    "reason": "The column does not support exclusion as it is a Primary Key",
                                },
                            },
                        },
                    }
                },
            }
        },
    },
}

MOCK_FIVETRAN_METADATA_TABLES_RESPONSE_PAYLOAD = {
    "code": "Success",
    "data": {
        "items": [
            {
                "id": "NjgyMDM0OQ",
                "parent_id": "ZGVtbw",
                "name_in_source": "subscription_periods",
                "name_in_destination": "subscription_periods",
            }
        ]
    },
}

MOCK_FIVETRAN_METADATA_COLUMNS_RESPONSE_PAYLOAD = {
    "code": "Success",
    "data": {
        "items": [
            {
                "id": "MjE0NDM2ODE2",
                "parent_id": "NjgyMDM0OQ",
                "name_in_source": "_file",
                "name_in_destination": "_file",
                "type_in_source": "String",
                "type_in_destination": "VARCHAR(256)",
                "is_primary_key": True,
                "is_foreign_key": False,
            },
        ]
    },
}

MOCK_FIVETRAN_DESTINATIONS_RESPONSE_PAYLOAD = {
    "code": "Success",
    "data": {
        "id": "rarer_gradient",
        "group_id": "rarer_gradient",
        "service": "google_sheets",
        "region": "GCP_US_EAST4",
        "time_zone_offset": "-8",
        "setup_status": "connected",
        "config": {"schema": "google_sheets.fivetran_google_sheets_spotify"},
    },
}

MOCK_FIVETRAN_GROUPS_RESPONSE_PAYLOAD = {
    "code": "Success",
    "data": {
        "id": "rarer_gradient",
        "name": "GoogleSheets",
        "created_at": "2022-12-12T17:14:33.790844Z",
    },
}


class TestFivetranHookAsync:
    @pytest.mark.asyncio
    @mock.patch("fivetran_provider_async.hooks.FivetranHookAsync._do_api_call_async")
    async def test_fivetran_hook_get_connector_async(self, mock_api_call_async_response):
        """Tests that the get_connector_async method fetches the details of a connector"""
        hook = FivetranHookAsync(fivetran_conn_id="conn_fivetran")
        mock_api_call_async_response.return_value = MOCK_FIVETRAN_RESPONSE_PAYLOAD_SHEETS
        result = await hook.get_connector_async(connector_id="interchangeable_revenge")
        assert result["status"]["setup_state"] == "connected"

    @pytest.mark.asyncio
    @mock.patch("fivetran_provider_async.hooks.FivetranHookAsync._do_api_call_async")
    async def test_fivetran_hook_get_connector_async_error(self, mock_api_call_async_response):
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
        self, mock_api_call_async_response, mock_previous_completed_at, expected_result
    ):
        """Tests that get_sync_status_async method return success or pending depending on whether
        current_completed_at > previous_completed_at"""
        hook = FivetranHookAsync(fivetran_conn_id="conn_fivetran")
        mock_api_call_async_response.return_value = MOCK_FIVETRAN_RESPONSE_PAYLOAD_SHEETS
        result = await hook.get_sync_status_async(
            connector_id="interchangeable_revenge",
            previous_completed_at=mock_previous_completed_at,
            reschedule_wait_time=5,
        )
        assert result == expected_result

    @pytest.mark.asyncio
    @mock.patch("fivetran_provider_async.hooks.FivetranHookAsync._do_api_call_async")
    async def test_fivetran_hook_get_sync_status_async_with_reschedule_mode_error_for_wait_time(
        self, mock_api_call_async_response
    ):
        """Tests that get_sync_status_async method return error with rescheduled_for in Fivetran API response
        along with schedule_type as manual and negative wait time."""

        # current_completed_at < previous_completed_at
        mock_previous_completed_at = pendulum.datetime(2021, 3, 23, 21, 55)
        hook = FivetranHookAsync(fivetran_conn_id="conn_fivetran")
        mock_api_call_async_response.return_value = MOCK_FIVETRAN_RESPONSE_PAYLOAD_SHEETS_RESCHEDULE_MODE
        with pytest.raises(ValueError, match="Sync connector manually."):
            await hook.get_sync_status_async(
                connector_id="interchangeable_revenge",
                previous_completed_at=mock_previous_completed_at,
            )

    @pytest.mark.asyncio
    @mock.patch("fivetran_provider_async.hooks.FivetranHookAsync._do_api_call_async")
    async def test_fivetran_hook_get_sync_status_async_with_reschedule_mode_returns_success(
        self,
        mock_api_call_async_response,
    ):
        """
        Tests that get_sync_status_async method returns success when
        current_completed_at > previous_completed_at.
        (The hook returns success because data is not being blocked up to the target completed time.)
        """

        # current_completed_at > previous_completed_at
        mock_previous_completed_at = pendulum.datetime(2021, 3, 23)

        hook = FivetranHookAsync(fivetran_conn_id="conn_fivetran")
        mock_api_call_async_response.return_value = MOCK_FIVETRAN_RESPONSE_PAYLOAD_SHEETS_RESCHEDULE_MODE
        result = await hook.get_sync_status_async(
            connector_id="interchangeable_revenge",
            previous_completed_at=mock_previous_completed_at,
        )
        assert result == "success"

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
        self,
        mock_start_fivetran_sync,
        mock_api_call_async_response,
        mock_previous_completed_at,
        expected_result,
    ):
        """Tests that get_sync_status_async method return success or pending depending on whether
        current_completed_at > previous_completed_at with reschedule_wait_time specified by user and
        schedule_type as manual in API response."""
        mock_start_fivetran_sync.return_value = pendulum.datetime(2021, 3, 21, 21, 55)
        hook = FivetranHookAsync(fivetran_conn_id="conn_fivetran")
        mock_api_call_async_response.return_value = MOCK_FIVETRAN_RESPONSE_PAYLOAD_SHEETS_RESCHEDULE_MODE
        result = await hook.get_sync_status_async(
            connector_id="interchangeable_revenge",
            previous_completed_at=mock_previous_completed_at,
            reschedule_wait_time=3,
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
        self,
        mock_start_fivetran_sync,
        mock_api_call_async_response,
        mock_previous_completed_at,
        expected_result,
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
    async def test_fivetran_hook_get_sync_status_async_exception(self, mock_api_call_async_response):
        """
        Tests that get_sync_status_async method raises exception
        when failed_at > previous_completed_at > succeeded_at
        """
        mock_previous_completed_at = pendulum.datetime(2021, 3, 21, 21, 55)
        hook = FivetranHookAsync(fivetran_conn_id="conn_fivetran")

        # Set `failed_at` value so that failed_at > completed_after_time > succeeded_at
        mock_fivetran_payload_sheets_modified = MOCK_FIVETRAN_RESPONSE_PAYLOAD_SHEETS.copy()
        mock_fivetran_payload_sheets_modified["data"] = mock_fivetran_payload_sheets_modified["data"].copy()
        mock_fivetran_payload_sheets_modified["data"]["failed_at"] = "2021-03-23T20:55:12.670390Z"
        mock_fivetran_payload_sheets_modified["data"]["succeeded_at"] = "2021-03-19T20:55:12.670390Z"

        mock_api_call_async_response.return_value = mock_fivetran_payload_sheets_modified

        with pytest.raises(AirflowException) as exc:
            await hook.get_sync_status_async(
                connector_id="interchangeable_revenge", previous_completed_at=mock_previous_completed_at
            )
        assert "Fivetran sync for connector interchangeable_revenge failed" in str(exc.value)

    @pytest.mark.asyncio
    @mock.patch("fivetran_provider_async.hooks.FivetranHookAsync._do_api_call_async")
    async def test_fivetran_hook_is_synced_async_propagate_errors_forward_exception(
        self, mock_api_call_async_response
    ):
        """
        Tests that get_sync_status_async method raises exception
        when completed_after_time > failed_at > succeeded_at
        and propagate_failures_forward=True
        """
        mock_completed_after_time = pendulum.datetime(2021, 3, 21, 21, 55)
        hook = FivetranHookAsync(fivetran_conn_id="conn_fivetran")

        # Set `failed_at` value so that completed_after_time > failed_at > succeeded_at
        mock_fivetran_payload_sheets_modified = MOCK_FIVETRAN_RESPONSE_PAYLOAD_SHEETS.copy()
        mock_fivetran_payload_sheets_modified["data"] = mock_fivetran_payload_sheets_modified["data"].copy()
        mock_fivetran_payload_sheets_modified["data"]["failed_at"] = "2021-03-20T20:55:12.670390Z"
        mock_fivetran_payload_sheets_modified["data"]["succeeded_at"] = "2021-03-19T20:55:12.670390Z"

        mock_api_call_async_response.return_value = mock_fivetran_payload_sheets_modified

        with pytest.raises(AirflowException) as exc:
            await hook.is_synced_after_target_time_async(
                connector_id="interchangeable_revenge",
                completed_after_time=mock_completed_after_time,
                propagate_failures_forward=True,
            )
        assert "Fivetran sync for connector interchangeable_revenge failed" in str(exc.value)

    @pytest.mark.asyncio
    @mock.patch("fivetran_provider_async.hooks.FivetranHookAsync._do_api_call_async")
    async def test_fivetran_hook_is_synced_async_propagate_errors_forward_is_false(
        self, mock_api_call_async_response
    ):
        """
        Tests that get_sync_status_async method returns "pending"
        when completed_after_time > failed_at > succeeded_at
        and propagate_failures_forward=False
        """
        mock_completed_after_time = pendulum.datetime(2021, 3, 24, 21, 55)
        hook = FivetranHookAsync(fivetran_conn_id="conn_fivetran")

        # Set `failed_at` value so that completed_after_time > failed_at > succeeded_at
        mock_fivetran_payload_sheets_modified = MOCK_FIVETRAN_RESPONSE_PAYLOAD_SHEETS.copy()
        mock_fivetran_payload_sheets_modified["data"] = mock_fivetran_payload_sheets_modified["data"].copy()
        mock_fivetran_payload_sheets_modified["data"]["failed_at"] = "2021-03-23T20:59:12.670390Z"

        mock_api_call_async_response.return_value = mock_fivetran_payload_sheets_modified

        result = await hook.is_synced_after_target_time_async(
            connector_id="interchangeable_revenge",
            completed_after_time=mock_completed_after_time,
            propagate_failures_forward=False,
        )
        assert result == "pending"

    @pytest.mark.asyncio
    @mock.patch("fivetran_provider_async.hooks.FivetranHookAsync.start_fivetran_sync")
    @mock.patch("fivetran_provider_async.hooks.FivetranHookAsync._do_api_call_async")
    async def test_fivetran_hook_pause_and_restart(
        self, mock_api_call_async_response, mock_start_fivetran_sync
    ):
        """Tests that pause_and_restart method for manual mode with reschedule time set."""
        hook = FivetranHookAsync(fivetran_conn_id="conn_fivetran")
        mock_start_fivetran_sync.return_value = True
        mock_api_call_async_response.return_value = MOCK_FIVETRAN_RESPONSE_PAYLOAD_SHEETS

        result = hook.pause_and_restart(
            connector_id="interchangeable_revenge", reschedule_for="manual", reschedule_wait_time=5
        )
        assert result is True

    @pytest.mark.asyncio
    @mock.patch("fivetran_provider_async.hooks.FivetranHookAsync._do_api_call_async")
    async def test_fivetran_hook_get_last_sync_async_no_xcom(self, mock_api_call_async_response):
        """Tests that the get_last_sync_async method returns the last time Fivetran connector
        completed a sync"""
        hook = FivetranHookAsync(fivetran_conn_id="conn_fivetran")
        mock_api_call_async_response.return_value = MOCK_FIVETRAN_RESPONSE_PAYLOAD_SHEETS
        result = await hook.get_last_sync_async(connector_id="interchangeable_revenge")
        assert result == pendulum.parse(MOCK_FIVETRAN_RESPONSE_PAYLOAD_SHEETS["data"]["succeeded_at"])

    @pytest.mark.asyncio
    @mock.patch("fivetran_provider_async.hooks.FivetranHookAsync._do_api_call_async")
    async def test_fivetran_hook_get_last_sync_async_with_xcom(self, mock_api_call_async_response):
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
    async def test_do_api_call_async_get_method_with_success(self, mock_get_connection, mock_session):
        """Tests that _do_api_call_async method returns correct response when GET request
        is successful"""

        async def mock_fun(arg1, arg2, arg3, arg4):
            return {"status": "success"}

        mock_session.return_value.__aexit__.return_value = mock_fun
        mock_session.return_value.__aenter__.return_value.request.return_value.json.return_value = {
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
    async def test_do_api_call_async_patch_method_with_success(self, mock_get_connection, mock_session):
        """Tests that _do_api_call_async method returns correct response when PATCH request
        is successful"""

        async def mock_fun(arg1, arg2, arg3, arg4):
            return {"status": "success"}

        mock_session.return_value.__aexit__.return_value = mock_fun
        mock_session.return_value.__aenter__.return_value.request.return_value.json.return_value = {
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
    async def test_do_api_call_async_post_method_with_success(self, mock_get_connection, mock_session):
        """Tests that _do_api_call_async method returns correct response when POST request
        is successful"""

        async def mock_fun(arg1, arg2, arg3, arg4):
            return {"status": "success"}

        mock_session.return_value.__aexit__.return_value = mock_fun
        mock_session.return_value.__aenter__.return_value.request.return_value.json.return_value = {
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
    async def test_do_api_call_async_with_non_retryable_client_response_error(
        self, mock_get_connection, mock_session
    ):
        """Tests that _do_api_call_async method returns expected response for a non retryable error"""
        mock_session.return_value.__aenter__.return_value.request.return_value.json.side_effect = (
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
    async def test_do_api_call_async_with_retryable_client_response_error(
        self, mock_get_connection, mock_session
    ):
        """Tests that _do_api_call_async method raises exception for a retryable error"""
        mock_session.return_value.__aenter__.return_value.request.return_value.json.side_effect = (
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


# Mock the `conn_fivetran` Airflow connection (note the `@` after `API_SECRET`)
@mock.patch.dict("os.environ", AIRFLOW_CONN_CONN_FIVETRAN="http://API_KEY:API_SECRET@")
class TestFivetranHook(unittest.TestCase):
    """
    Test functions for Fivetran Hook.

    Mocks responses from Fivetran API.
    """

    @requests_mock.mock()
    def test_get_connector(self, m):
        m.get(
            "https://api.fivetran.com/v1/connectors/interchangeable_revenge",
            json=MOCK_FIVETRAN_RESPONSE_PAYLOAD,
        )
        hook = FivetranHook(
            fivetran_conn_id="conn_fivetran",
        )
        result = hook.get_connector(connector_id="interchangeable_revenge")
        assert result["status"]["setup_state"] == "connected"

    @requests_mock.mock()
    def test_get_connector_schemas(self, m):
        m.get(
            "https://api.fivetran.com/v1/connectors/interchangeable_revenge/schemas",
            json=MOCK_FIVETRAN_SCHEMA_RESPONSE_PAYLOAD,
        )
        hook = FivetranHook(
            fivetran_conn_id="conn_fivetran",
        )
        result = hook.get_connector_schemas(connector_id="interchangeable_revenge")
        assert result["schemas"]["google_sheets.fivetran_google_sheets_spotify"]["enabled"]

    @requests_mock.mock()
    def test_get_metadata_tables(self, m):
        m.get(
            "https://api.fivetran.com/v1/metadata/connectors/interchangeable_revenge/tables",
            json=MOCK_FIVETRAN_METADATA_TABLES_RESPONSE_PAYLOAD,
        )
        hook = FivetranHook(
            fivetran_conn_id="conn_fivetran",
        )
        result = hook.get_metadata(connector_id="interchangeable_revenge", metadata="tables")
        assert result["items"][0]["id"] == "NjgyMDM0OQ"

    @requests_mock.mock()
    def test_get_metadata_columns(self, m):
        m.get(
            "https://api.fivetran.com/v1/metadata/connectors/interchangeable_revenge/columns",
            json=MOCK_FIVETRAN_METADATA_COLUMNS_RESPONSE_PAYLOAD,
        )
        hook = FivetranHook(
            fivetran_conn_id="conn_fivetran",
        )
        result = hook.get_metadata(connector_id="interchangeable_revenge", metadata="columns")
        assert result["items"][0]["id"] == "MjE0NDM2ODE2"

    @requests_mock.mock()
    def test_get_destinations(self, m):
        m.get(
            "https://api.fivetran.com/v1/destinations/rarer_gradient",
            json=MOCK_FIVETRAN_DESTINATIONS_RESPONSE_PAYLOAD,
        )
        hook = FivetranHook(
            fivetran_conn_id="conn_fivetran",
        )
        result = hook.get_destinations(group_id="rarer_gradient")
        assert result["service"] == "google_sheets"

    @requests_mock.mock()
    def test_get_groups(self, m):
        m.get(
            "https://api.fivetran.com/v1/groups/rarer_gradient",
            json=MOCK_FIVETRAN_GROUPS_RESPONSE_PAYLOAD,
        )
        hook = FivetranHook(
            fivetran_conn_id="conn_fivetran",
        )
        result = hook.get_groups(group_id="rarer_gradient")
        assert result["id"] == "rarer_gradient"
        assert result["name"] == "GoogleSheets"

    @requests_mock.mock()
    def test_start_fivetran_sync(self, m):
        m.get(
            "https://api.fivetran.com/v1/connectors/interchangeable_revenge",
            json=MOCK_FIVETRAN_RESPONSE_PAYLOAD,
        )
        m.post(
            "https://api.fivetran.com/v1/connectors/interchangeable_revenge/force",
            json=MOCK_FIVETRAN_RESPONSE_PAYLOAD,
        )
        hook = FivetranHook(
            fivetran_conn_id="conn_fivetran",
        )
        result = hook.start_fivetran_sync(connector_id="interchangeable_revenge")
        assert result is not None
