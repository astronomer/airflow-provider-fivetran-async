from __future__ import annotations

import asyncio
import time
from collections.abc import Iterator
from datetime import datetime
from time import sleep
from typing import TYPE_CHECKING, Any, Dict

import aiohttp
import pendulum
import requests
from aiohttp import ClientResponseError
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models.connection import Connection
from airflow.utils.helpers import is_container
from asgiref.sync import sync_to_async
from requests import exceptions as requests_exceptions


class FivetranHook(BaseHook):
    """
    Fivetran API interaction hook.

    :param fivetran_conn_id: `Conn ID` of the Connection to be used to
        configure this hook.
    :type fivetran_conn_id: str
    :param timeout_seconds: The amount of time in seconds the requests library
        will wait before timing out.
    :type timeout_seconds: int
    :param retry_limit: The number of times to retry the connection in case of
        service outages.
    :type retry_limit: int
    :param retry_delay: The number of seconds to wait between retries.
    :type retry_delay: float
    """

    conn_name_attr = "fivetran_conn_id"
    default_conn_name = "fivetran_default"
    conn_type = "Fivetran"
    hook_name = "Fivetran"
    api_user_agent = "airflow_provider_fivetran/1.1.4"
    api_protocol = "https"
    api_host = "api.fivetran.com"
    api_path_connectors = "v1/connectors/"
    api_metadata_path_connectors = "v1/metadata/connectors/"
    api_path_destinations = "v1/destinations/"
    api_path_groups = "v1/groups/"

    @classmethod
    def get_ui_field_behaviour(cls) -> Dict:
        """Returns custom field behaviour"""
        return {
            "hidden_fields": ["schema", "port", "extra", "host"],
            "relabeling": {
                "login": "Fivetran API Key",
                "password": "Fivetran API Secret",
            },
            "placeholders": {
                "login": "api key",
                "password": "api secret",
            },
        }

    @staticmethod
    def _get_airflow_version() -> str:
        """
        Fetch and return the current Airflow version
        from aws provider
        https://github.com/apache/airflow/blob/ae25a52ae342c9e0bc3afdb21d613447c3687f6c/airflow/providers/amazon/aws/hooks/base_aws.py#L536
        """
        try:
            # This can be a circular import under specific configurations.
            # Importing locally to either avoid or catch it if it does happen.
            from airflow import __version__ as airflow_version

            return "-airflow_version/" + airflow_version
        except Exception:
            # Under no condition should an error here ever cause an issue for the user.
            return ""

    def __init__(
        self,
        fivetran_conn_id: str = "fivetran_default",
        fivetran_conn: Connection | None = None,
        timeout_seconds: int = 180,
        retry_limit: int = 3,
        retry_delay: float = 1.0,
    ) -> None:
        super().__init__(None)  # Passing None fixes a runtime problem in Airflow 1
        self.conn_id = fivetran_conn_id
        self.fivetran_conn = fivetran_conn
        self.timeout_seconds = timeout_seconds
        if retry_limit < 1:
            raise ValueError("Retry limit must be greater than equal to 1")
        self.retry_limit = retry_limit
        self.retry_delay = retry_delay

    def _prepare_api_call_kwargs(self, method: str, endpoint: str, **kwargs: Any) -> dict[str, Any]:
        """Add additional data to the API call."""
        if self.fivetran_conn is None:
            self.fivetran_conn = self.get_connection(self.conn_id)

        auth = (self.fivetran_conn.login, self.fivetran_conn.password)

        kwargs["auth"] = auth
        kwargs.setdefault("headers", {})

        kwargs["headers"].setdefault("User-Agent", self.api_user_agent + self._get_airflow_version())

        if method in ("POST", "PATCH"):
            kwargs["headers"].setdefault("Content-Type", "application/json;version=2")

        return kwargs

    def _do_api_call(
        self, method: str, endpoint: str = None, **kwargs: Any  # type: ignore[assignment]
    ) -> dict[str, Any]:
        """
        Utility function to perform an API call with retries

        :param method: Method for the API call
        :param endpoint: Endpoint of the Fivetran API to be hit
        :param kwargs: kwargs to be passed to requests.request()
        :return: If the api call returns a OK status code,
            this function returns the response as a dict. Otherwise,
            we throw an AirflowException.
        """
        if is_container(method) and len(method) == 2:
            import warnings

            warnings.warn(
                "The API for _do_api_call() has changed to closer match the"
                " requests.request() API. Please update your code accordingly.",
                DeprecationWarning,
                stacklevel=2,
            )
            method, endpoint = method  # type: ignore[misc]
        elif endpoint is None:
            raise TypeError(
                f"{self.__class__.__name__}._do_api_call() missing 1 required" f" positional argument: 'endpoint'"
            )

        if TYPE_CHECKING:
            assert endpoint is not None

        if self.fivetran_conn is None:
            self.fivetran_conn = self.get_connection(self.conn_id)

        url = f"{self.api_protocol}://{self.api_host}/{endpoint}"

        kwargs = self._prepare_api_call_kwargs(method, endpoint, **kwargs)

        attempt_num = 1
        while True:
            try:
                response = requests.request(method, url, **kwargs)
                response.raise_for_status()
                return response.json()
            except requests_exceptions.RequestException as e:
                if not _retryable_error(e):
                    # In this case, the user probably made a mistake.
                    # Don't retry.
                    assert e.response is not None
                    raise AirflowException(
                        f"Response: {e.response.content.decode()}, " f"Status Code: {e.response.status_code}"
                    ) from e

                self._log_request_error(attempt_num, str(e))

            if attempt_num == self.retry_limit:
                raise AirflowException(f"API request to Fivetran failed {self.retry_limit} times." " Giving up.")

            attempt_num += 1
            sleep(self.retry_delay)

    def _log_request_error(self, attempt_num: int, error: str) -> None:
        self.log.error(
            "Attempt %s API Request to Fivetran failed with reason: %s",
            attempt_num,
            error,
        )

    def _connector_ui_url(self, service_name: str, schema_name: str) -> str:
        return f"https://fivetran.com/dashboard/connectors/{service_name}/{schema_name}"

    def _connector_ui_url_logs(self, service_name: str, schema_name: str) -> str:
        return self._connector_ui_url(service_name, schema_name) + "/logs"

    def _connector_ui_url_setup(self, service_name: str, schema_name: str) -> str:
        return self._connector_ui_url(service_name, schema_name) + "/setup"

    def get_connector(self, connector_id: str) -> dict[str, Any]:
        """
        Fetches the detail of a connector.

        :param connector_id: Fivetran connector_id, found in connector settings
            page in the Fivetran user interface.
        :return: connector details
        """
        if connector_id == "":
            raise ValueError("No value specified for connector_id")
        endpoint = self.api_path_connectors + connector_id
        resp = self._do_api_call("GET", endpoint)
        return resp["data"]

    def get_connector_schemas(self, connector_id: str) -> dict[str, Any]:
        """
        Fetches schema information of the connector.

        :param connector_id: Fivetran connector_id, found in connector settings
            page in the Fivetran user interface.
        :return: schema details
        """
        if connector_id == "":
            raise ValueError("No value specified for connector_id")
        endpoint = self.api_path_connectors + connector_id + "/schemas"
        resp = self._do_api_call("GET", endpoint)
        return resp["data"]

    def get_metadata(self, connector_id: str, metadata: str) -> dict[str, Any]:
        """
        Fetches metadata for a given metadata string and connector.

        The Fivetran metadata API is currently in beta and available to
        all Fivetran users on the enterprise plan and above.

        :param connector_id: Fivetran connector_id, found in connector settings
            page in the Fivetran user interface.
        :param metadata: The string to return the type of metadata from the API
        :return: table or column metadata details
        """
        metadata_values = ("tables", "columns")
        if connector_id == "":
            raise ValueError("No value specified for connector_id")
        if metadata not in metadata_values:
            raise ValueError(f"Got {metadata} for param 'metadata', expected one" f" of: {metadata_values}")
        endpoint = self.api_metadata_path_connectors + connector_id + "/" + metadata
        resp = self._do_api_call("GET", endpoint)
        return resp["data"]

    def get_destinations(self, group_id: str) -> dict:
        """
        Fetches destination information for the given group.
        :param group_id: The Fivetran group ID, returned by a connector API call.
        :return: destination details
        :rtype: Dict
        """
        if group_id == "":
            raise ValueError("No value specified for group_id")
        endpoint = self.api_path_destinations + group_id
        resp = self._do_api_call("GET", endpoint)
        return resp["data"]

    def get_groups(self, group_id: str = "") -> Iterator[dict] | dict:
        """
        Fetches information about groups.
        :param group_id: The Fivetran group ID, if provided will restrict to that group.
        :return: group details
        """
        if group_id == "":
            return iter(self._get_groups())
        endpoint = self.api_path_groups + group_id
        resp = self._do_api_call("GET", endpoint)
        return resp["data"]

    def _get_groups(self) -> Iterator[dict]:
        endpoint = self.api_path_groups
        cursor = True
        while cursor:
            resp = self._do_api_call("GET", endpoint, params={"cursor": cursor} if isinstance(cursor, str) else None)
            cursor = resp["data"].get("next_cursor")
            for group in resp["data"]["items"]:
                yield group

    def get_connectors(self, group_id: str) -> Iterator[dict]:
        """
        Fetches connector information for the given group, and returns a generator that iterates through
        each connector.
        :param group_id: The Fivetran group ID, returned by a connector API call.
        :yields: connector details
        """
        endpoint = f"{self.api_path_groups}{group_id}/connectors/"
        cursor = True
        while cursor:
            resp = self._do_api_call("GET", endpoint, params={"cursor": cursor} if isinstance(cursor, str) else None)
            cursor = resp["data"].get("next_cursor")
            for connector in resp["data"]["items"]:
                yield connector

    def get_connector_id(self, connector_name: str, destination_name: str) -> str:
        all_groups = self._get_groups()
        group = next((group for group in all_groups if group.get("name") == destination_name), None)
        if not group:
            raise ValueError(f"Destination '{destination_name}' not found.")

        all_connectors = self.get_connectors(group_id=group.get("id", ""))
        connector = next((connector for connector in all_connectors if connector.get("schema") == connector_name), None)
        if not connector:
            raise ValueError(f"Connector '{connector_name}' not found in Destination '{destination_name}'.")

        return connector.get("id", "")

    def check_connector(self, connector_id: str) -> dict[str, Any]:
        """
        Ensures connector configuration has been completed successfully and is in
            a functional state.

        :param connector_id: Fivetran connector_id, found in connector settings
            page in the Fivetran user interface.
        :return: API call response
        """
        connector_details = self.get_connector(connector_id)
        service_name = connector_details["service"]
        schema_name = connector_details["schema"]
        setup_state = connector_details["status"]["setup_state"]
        if setup_state != "connected":
            raise AirflowException(
                f'Fivetran connector "{connector_id}" not correctly configured, '
                f"status: {setup_state}\nPlease see: "
                f"{self._connector_ui_url_setup(service_name, schema_name)}"
            )
        self.log.info("Connector type: %s, connector schema: %s", service_name, schema_name)
        self.log.info("Connectors logs at %s", self._connector_ui_url_logs(service_name, schema_name))
        return connector_details

    def set_schedule_type(self, connector_id: str, schedule_type: str) -> dict[str, Any]:
        """
        Set connector sync mode to switch sync control between API and UI.

        :param connector_id: Fivetran connector_id, found in connector settings
            page in the Fivetran user interface.
        :param schedule_type: "manual" (schedule controlled via Airlow) or
            "auto" (schedule controlled via Fivetran)
        :return: API call response
        """
        endpoint = self.api_path_connectors + connector_id
        return self._do_api_call("PATCH", endpoint, json={"schedule_type": schedule_type})

    def prep_connector(self, connector_id: str, schedule_type: str) -> None:
        """
        Prepare the connector to run in Airflow by checking that it exists and is a good state,
            then update connector sync schedule type if changed.

        :param connector_id: Fivetran connector_id, found in connector settings
            page in the Fivetran user interface.
        :param schedule_type: Fivetran connector schedule type
        """
        connector_details = self.check_connector(connector_id)
        if schedule_type not in {"manual", "auto"}:
            raise ValueError('schedule_type must be either "manual" or "auto"')
        if connector_details["schedule_type"] != schedule_type:
            self.set_schedule_type(connector_id, schedule_type)
        else:
            self.log.debug("Schedule type for %s was already %s", connector_id, schedule_type)

    def start_fivetran_sync(self, connector_id: str, mode="sync", payload: dict | None = None) -> str:
        """
        :param connector_id: Fivetran connector_id, found in connector settings
            page in the Fivetran user interface.
        :return: Timestamp of previously completed sync
        """
        if payload and mode != "resync":
            raise ValueError("payload should only be provided when doing a resync")

        connector_details = self.get_connector(connector_id)
        succeeded_at = connector_details["succeeded_at"]
        failed_at = connector_details["failed_at"]
        endpoint = self.api_path_connectors + connector_id

        if connector_details["paused"] is True:
            self._do_api_call("PATCH", endpoint, json={"paused": False})
            if succeeded_at is None and failed_at is None:
                succeeded_at = str(pendulum.now())

        api_call_args: dict[str, Any] = {
            "method": "POST",
            "endpoint": f"{endpoint}/{'resync' if mode == 'resync' else 'force'}",
        }
        if payload:
            api_call_args["json"] = payload
        self._do_api_call(**api_call_args)

        failed_at_time = None
        try:
            failed_at_time = self._parse_timestamp(failed_at)
        except Exception:
            self.log.error("Pendulum.parsing.exception occured")

        last_sync = (
            succeeded_at
            if failed_at_time is None or self._parse_timestamp(succeeded_at) > self._parse_timestamp(failed_at)
            else failed_at
        )
        return last_sync

    def get_last_sync(self, connector_id: str, xcom: str = "") -> pendulum.DateTime:
        """
        Get the last time Fivetran connector completed a sync.
            Used with FivetranSensor to monitor sync completion status.

        :param connector_id: Fivetran connector_id, found in connector settings
            page in the Fivetran user interface.
        :param xcom: Deprecated. Timestamp as string pull from FivetranOperator via XCOM
        :return: Timestamp of last completed sync
        :rtype: Pendulum.DateTime
        """
        if xcom:
            import warnings

            warnings.warn(
                "Param `xcom` is deprecated. Timestamps should be rendered in the Operator, Sensor,"
                " or Triggerer instead",
                stacklevel=2,
            )

            last_sync = self._parse_timestamp(xcom)
        else:
            connector_details = self.get_connector(connector_id)
            succeeded_at = self._parse_timestamp(connector_details["succeeded_at"])
            failed_at = self._parse_timestamp(connector_details["failed_at"])
            last_sync = succeeded_at if succeeded_at > failed_at else failed_at
        return last_sync

    def get_sync_status(
        self, connector_id: str, previous_completed_at: pendulum.DateTime, reschedule_time: int | None = None
    ) -> bool:
        import warnings

        warnings.warn(
            "`get_sync_status()` is deprecated. Please use `is_synced_after_target_time()` instead.",
            stacklevel=2,
        )

        return self.is_synced_after_target_time(
            connector_id=connector_id,
            completed_after_time=previous_completed_at,
            reschedule_wait_time=reschedule_time,
            always_wait_when_syncing=False,
            propagate_failures_forward=False,
        )

    def is_synced_after_target_time(
        self,
        connector_id: str,
        completed_after_time: datetime,
        reschedule_wait_time: int | None = None,
        always_wait_when_syncing: bool = False,
        propagate_failures_forward: bool = True,
    ) -> bool:
        """
        For sensor, return True if connector's 'succeeded_at' field has updated.

        :param connector_id: Fivetran connector_id, found in connector settings
            page in the Fivetran user interface.
        :param completed_after_time: The time we are comparing the Fivetran
            `succeeded_at` and `failed_at` timestamps to. The method returns
            True when both the last `succeeded_at` exceeds the
            `completed_after_time` and other conditions (determined by other
            params to this method) are met.
        :param reschedule_wait_time: Optional. If connector is in a
            rescheduled state, this will be the number of seconds to wait
            before restarting. If None, then Fivetran's suggestion is used
            instead.
        :param always_wait_when_syncing: If True, then this method will
            always return False when the connector's sync_state is "syncing",
            no matter what.
        :param propagate_failures_forward: If True, then this method will always
            raise an AirflowException when the most recent connector status is
            a failure and there are no successes dated after the target time.
            Specifically, this makes it so that
            `completed_after_time > failed_at > succeeded_at` is considered a
            fail condition.
        """
        # @todo Need logic here to tell if the sync is not running at all and not
        # likely to run in the near future.
        connector_details = self.get_connector(connector_id)
        return self._determine_if_synced_from_connector_details(
            connector_id=connector_id,
            connector_details=connector_details,
            completed_after_time=completed_after_time,
            reschedule_wait_time=reschedule_wait_time,
            always_wait_when_syncing=always_wait_when_syncing,
            propagate_failures_forward=propagate_failures_forward,
        )

    def _determine_if_synced_from_connector_details(
        self,
        connector_id: str,
        connector_details: dict[str, Any],
        completed_after_time: datetime,
        reschedule_wait_time: int | None = None,
        always_wait_when_syncing: bool = False,
        propagate_failures_forward: bool = True,
    ) -> bool:
        succeeded_at = self._parse_timestamp(connector_details["succeeded_at"])
        failed_at = self._parse_timestamp(connector_details["failed_at"])

        sync_state = connector_details["status"]["sync_state"]
        self.log.info("Connector %s: sync_state = %s", connector_id, sync_state)

        if always_wait_when_syncing and sync_state == "syncing":
            return False

        # The only way to tell if a sync failed is to check if its latest
        # failed_at value is greater than then last known "sync completed at" value.
        if failed_at > completed_after_time > succeeded_at or (
            completed_after_time > failed_at > succeeded_at and propagate_failures_forward
        ):
            service_name = connector_details["service"]
            schema_name = connector_details["schema"]
            raise AirflowException(
                f"Fivetran sync for connector {connector_id} failed; "
                f"please see logs at "
                f"{self._connector_ui_url_logs(service_name, schema_name)}"
            )

        # Check if sync started by FivetranOperator has finished
        # indicated by new 'succeeded_at' timestamp
        if succeeded_at > completed_after_time:
            self.log.info("Connector %s: succeeded_at: %s", connector_id, succeeded_at.to_iso8601_string())
            return True

        # if sync in rescheduled start, wait for time recommended by Fivetran
        # or manually specified, then restart sync
        if sync_state == "rescheduled" and connector_details["schedule_type"] == "manual":
            self.log.info('Connector is in "rescheduled" state and needs to be manually restarted')
            self.pause_and_restart(
                connector_id,
                connector_details["status"]["rescheduled_for"],
                reschedule_wait_time=reschedule_wait_time,
            )
            return False

        return False

    def pause_and_restart(
        self,
        connector_id: str,
        reschedule_for: str,
        reschedule_wait_time: int | None = None,
        *,
        reschedule_time: int | None = None,  # deprecated!
    ) -> str:
        """
        While a connector is syncing, if it falls into a reschedule state,
        wait for a time either specified by the user of recommended by Fivetran,
        Then restart a sync

        :param connector_id: Fivetran connector_id, found in connector settings
            page in the Fivetran user interface.
        :param reschedule_for: From connector details, if schedule_type is manual,
            then the connector expects triggering the event at the designated UTC time
        :param reschedule_wait_time: Optional, if connector is in reset state
            number of seconds to wait before restarting, else Fivetran suggestion used
        :param reschedule_time: Deprecated
        """
        if reschedule_time is not None:
            import warnings

            warnings.warn(
                "`reschedule_time` arg is deprecated. Please use `reschedule_wait_time` instead.",
                stacklevel=2,
            )
            if reschedule_wait_time is None:
                reschedule_wait_time = reschedule_time

        if reschedule_wait_time is not None:
            self.log.info("Starting connector again in %s seconds", reschedule_wait_time)
            time.sleep(reschedule_wait_time)
        else:
            wait_time = (self._parse_timestamp(reschedule_for).add(minutes=1) - pendulum.now(tz="UTC")).seconds
            if wait_time < 0:
                raise ValueError(
                    f"Reschedule time {wait_time} configured in "
                    f"Fivetran connector has elapsed. Sync connector manually."
                )
            self.log.info("Starting connector again in %s seconds", wait_time)
            time.sleep(wait_time)

        self.log.info("Restarting connector now")
        return self.start_fivetran_sync(connector_id)

    def _parse_timestamp(self, api_time: datetime | str | None) -> pendulum.DateTime:
        """
        Returns either the pendulum-parsed actual timestamp or a very out-of-date timestamp if not set.

        :param api_time: timestamp format as returned by the Fivetran API.
        """
        if isinstance(api_time, datetime):
            return pendulum.instance(api_time)
        elif api_time is None:
            return pendulum.from_timestamp(-1)
        else:
            return pendulum.parse(api_time)  # type: ignore[return-value]

    def test_connection(self):
        """
        Ensures Airflow can reach Fivetran API
        """
        try:
            resp = self._do_api_call(("GET", "v1/users"))
            if resp["code"] == "Success":
                return True, "Fivetran connection test passed"
            else:
                return False, resp
        except Exception as e:
            return False, str(e)


class FivetranHookAsync(FivetranHook):
    """
    Fivetran API interaction hook extending FivetranHook for asynchronous fuctionality.

    :param fivetran_conn_id: `Conn ID` of the Connection to be used to
        configure this hook.
    :type fivetran_conn_id: str
    :param timeout_seconds: The amount of time in seconds the requests library
        will wait before timing out.
    :type timeout_seconds: int
    :param retry_limit: The number of times to retry the connection in case of
        service outages.
    :type retry_limit: int
    :param retry_delay: The number of seconds to wait between retries.
    :type retry_delay: float
    """

    api_user_agent = "airflow_provider_fivetran_async/1.0.0"

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    def _prepare_api_call_kwargs_async(self, method: str, endpoint: str, **kwargs: Any) -> dict[str, Any]:
        kwargs = self._prepare_api_call_kwargs(method, endpoint, **kwargs)
        auth = kwargs.get("auth")
        if auth is not None and is_container(auth) and 2 <= len(auth) <= 3:
            kwargs["auth"] = aiohttp.BasicAuth(*auth)
        return kwargs

    async def _do_api_call_async(
        self, method: str, endpoint: str = None, **kwargs: Any  # type: ignore[assignment]
    ) -> dict[str, Any]:
        # Check for previous implementation:
        if is_container(method) and len(method) == 2:
            import warnings

            warnings.warn(
                "The API for _do_api_call_async() has changed to closer match the"
                " aiohttp.ClientSession.request() API. Please update your code accordingly.",
                DeprecationWarning,
                stacklevel=2,
            )
            method, endpoint = method  # type: ignore[misc]
        elif endpoint is None:
            raise TypeError(
                f"{self.__class__.__name__}._do_api_call_async() missing 1 required" f" positional argument: 'endpoint'"
            )

        if self.fivetran_conn is None:
            self.fivetran_conn = await sync_to_async(self.get_connection)(self.conn_id)

        url = f"{self.api_protocol}://{self.api_host}/{endpoint}"

        kwargs = self._prepare_api_call_kwargs_async(method, endpoint, **kwargs)

        async with aiohttp.ClientSession() as session:
            attempt_num = 1
            while True:
                try:
                    response = await session.request(method, url, **kwargs)
                    response.raise_for_status()
                    return await response.json()
                except ClientResponseError as e:
                    if not _retryable_error_async(e):
                        # In this case, the user probably made a mistake.
                        # Don't retry.
                        return {"Response": {e.message}, "Status Code": {e.status}}
                    self._log_request_error(attempt_num, str(e))

                if attempt_num == self.retry_limit:
                    raise AirflowException(f"API requests to Fivetran failed {self.retry_limit} times." " Giving up.")

                attempt_num += 1
                await asyncio.sleep(self.retry_delay)

    async def get_connector_async(self, connector_id):
        """
        Fetches the detail of a connector asynchronously.

        :param connector_id: Fivetran connector_id, found in connector settings
            page in the Fivetran user interface.
        :type connector_id: str
        :rtype: Dict
        """
        if connector_id == "":
            raise ValueError("No value specified for connector_id")
        endpoint = self.api_path_connectors + connector_id
        resp = await self._do_api_call_async("GET", endpoint, headers={"Accept": "application/json;version=2"})
        return resp["data"]

    async def get_sync_status_async(
        self,
        connector_id: str,
        previous_completed_at: pendulum.DateTime,
        reschedule_wait_time: int | None = None,
    ) -> str:
        import warnings

        warnings.warn(
            "`get_sync_status_async()` is deprecated." " Please use `is_synced_after_target_time_async()` instead.",
            stacklevel=2,
        )

        return await self.is_synced_after_target_time_async(
            connector_id=connector_id,
            completed_after_time=previous_completed_at,
            reschedule_wait_time=reschedule_wait_time,
            always_wait_when_syncing=False,
            propagate_failures_forward=False,
        )

    async def is_synced_after_target_time_async(
        self,
        connector_id: str,
        completed_after_time: datetime,
        reschedule_wait_time: int | None = None,
        always_wait_when_syncing: bool = False,
        propagate_failures_forward: bool = True,
    ) -> str:
        """
        For sensor, return True if connector's 'succeeded_at' field has updated.

        :param connector_id: Fivetran connector_id, found in connector settings
            page in the Fivetran user interface.
        :param completed_after_time: The time we are comparing the Fivetran
            `succeeded_at` and `failed_at` timestamps to. The method returns
            True when both the last `succeeded_at` exceeds the
            `completed_after_time` and other conditions (determined by other
            params to this method) are met.
        :param reschedule_wait_time: Optional. If connector is in a
            rescheduled state, this will be the number of seconds to wait
            before restarting. If None, then Fivetran's suggestion is used
            instead.
        :param always_wait_when_syncing: If True, then this method will
            always return False when the connector's sync_state is "syncing",
            no matter what.
        :param propagate_failures_forward: If True, then this method will always
            raise an AirflowException when the most recent connector status is
            a failure and there are no successes dated after the target time.
            Specifically, this makes it so that
            `completed_after_time > failed_at > succeeded_at` is considered a
            fail condition.
        """
        connector_details = await self.get_connector_async(connector_id)
        is_completed = self._determine_if_synced_from_connector_details(
            connector_id=connector_id,
            connector_details=connector_details,
            completed_after_time=completed_after_time,
            reschedule_wait_time=reschedule_wait_time,
            always_wait_when_syncing=always_wait_when_syncing,
            propagate_failures_forward=propagate_failures_forward,
        )
        if is_completed:
            return "success"
        return "pending"

    async def get_last_sync_async(self, connector_id: str, xcom: str = "") -> pendulum.DateTime:
        """
        Get the last time Fivetran connector completed a sync.
        Used with FivetranSensor to monitor sync completion status.

        :param connector_id: Fivetran connector_id, found in connector settings
            page in the Fivetran user interface.
        :type connector_id: str
        :param xcom: Timestamp as string pull from FivetranOperator via XCOM
        :type xcom: str
        :return: Timestamp of last completed sync
        :rtype: Pendulum.DateTime
        """
        if xcom:
            last_sync = self._parse_timestamp(xcom)

        else:
            connector_details = await self.get_connector_async(connector_id)
            succeeded_at = self._parse_timestamp(connector_details["succeeded_at"])
            failed_at = self._parse_timestamp(connector_details["failed_at"])
            last_sync = succeeded_at if succeeded_at > failed_at else failed_at
        return last_sync


def _retryable_error_async(exception: ClientResponseError) -> bool:
    return exception.status >= 500


def _retryable_error(exception: Exception) -> bool:
    return isinstance(
        exception,
        (requests_exceptions.ConnectionError, requests_exceptions.Timeout),
    ) or (
        getattr(exception, "response", None) is not None
        and getattr(exception, "response").status_code >= 500  # noqa: B009
    )
