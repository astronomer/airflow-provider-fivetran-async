import asyncio
import time
from typing import Any, Dict, cast

import aiohttp
import pendulum
from aiohttp import ClientResponseError
from airflow.exceptions import AirflowException
from asgiref.sync import sync_to_async
from fivetran_provider.hooks.fivetran import FivetranHook


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

    async def _do_api_call_async(self, endpoint_info, json=None):
        method, endpoint = endpoint_info

        if not self.fivetran_conn:
            self.fivetran_conn = await sync_to_async(self.get_connection)(self.conn_id)
        auth = (self.fivetran_conn.login, self.fivetran_conn.password)
        url = f"{self.api_protocol}://{self.api_host}/{endpoint}"
        headers = {"User-Agent": self.api_user_agent}

        async with aiohttp.ClientSession() as session:
            if method == "GET":
                request_func = session.get
            elif method == "POST":
                request_func = session.post
                headers.update({"Content-Type": "application/json;version=2"})
            elif method == "PATCH":
                request_func = session.patch
                headers.update({"Content-Type": "application/json;version=2"})
            else:
                raise AirflowException("Unexpected HTTP Method: " + method)

            attempt_num = 1
            while True:
                try:
                    response = await request_func(
                        url,
                        data=json if method in ("POST", "PATCH") else None,
                        params=json if method == "GET" else None,
                        auth=aiohttp.BasicAuth(login=auth[0], password=auth[1]),
                        headers=headers,
                    )
                    response.raise_for_status()
                    return cast(Dict[str, Any], await response.json())
                except ClientResponseError as e:
                    if not _retryable_error_async(e):
                        # In this case, the user probably made a mistake.
                        # Don't retry.
                        return {"Response": {e.message}, "Status Code": {e.status}}
                    self._log_request_error(attempt_num, str(e))

                if attempt_num == self.retry_limit:
                    raise AirflowException(
                        f"API requests to Fivetran failed {self.retry_limit} times." " Giving up."
                    )

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
        resp = await self._do_api_call_async(("GET", endpoint))
        return resp["data"]

    async def get_sync_status_async(self, connector_id, previous_completed_at, reschedule_wait_time=0):
        """
        For sensor, return True if connector's 'succeeded_at' field has updated.

        :param connector_id: Fivetran connector_id, found in connector settings
            page in the Fivetran user interface.
        :type connector_id: str
        :param previous_completed_at: The last time the connector ran, collected on Sensor
            initialization.
        :type previous_completed_at: pendulum.datetime.DateTime
        :param reschedule_wait_time: Optional, if connector is in reset state,
            number of seconds to wait before restarting the sync.
        :type reschedule_wait_time: int
        """
        connector_details = await self.get_connector_async(connector_id)
        succeeded_at = self._parse_timestamp(connector_details["succeeded_at"])
        failed_at = self._parse_timestamp(connector_details["failed_at"])
        current_completed_at = succeeded_at if succeeded_at > failed_at else failed_at

        # The only way to tell if a sync failed is to check if its latest
        # failed_at value is greater than then last known "sync completed at" value.
        if failed_at > previous_completed_at:
            service_name = connector_details["service"]
            schema_name = connector_details["schema"]
            raise AirflowException(
                f"Fivetran sync for connector {connector_id} failed; "
                f"please see logs at "
                f"{self._connector_ui_url_logs(service_name, schema_name)}"
            )

        sync_state = connector_details["status"]["sync_state"]
        self.log.info('Connector "%s": sync_state = "%s"', connector_id, sync_state)

        # if sync in rescheduled start, wait for time recommended by Fivetran
        # or manually specified, then restart sync
        if sync_state == "rescheduled" and connector_details["schedule_type"] == "manual":
            self.log.info('Connector is in "rescheduled" state and needs to be manually restarted')
            self.pause_and_restart(
                connector_id=connector_id,
                reschedule_for=connector_details["status"]["rescheduled_for"],
                reschedule_wait_time=reschedule_wait_time,
            )

        # Check if sync started by airflow has finished
        # indicated by new 'succeeded_at' timestamp
        if current_completed_at > previous_completed_at:
            self.log.info(
                'Connector "%s": succeeded_at = "%s"', connector_id, succeeded_at.to_iso8601_string()
            )
            job_status = "success"
            return job_status
        else:
            job_status = "pending"
            return job_status

    def pause_and_restart(self, connector_id, reschedule_for, reschedule_wait_time):
        """
        While a connector is syncing, if it falls into a reschedule state,
        wait for a time either specified by the user of recommended by Fivetran,
        Then restart a sync

        :param connector_id: Fivetran connector_id, found in connector settings
            page in the Fivetran user interface.
        :type connector_id: str
        :param reschedule_for: From Fivetran API response, if schedule_type is manual,
            then the connector expects triggering the event at the designated UTC time.
        :type reschedule_for: str
        :param reschedule_wait_time: Optional, if connector is in reset state,
            number of seconds to wait before restarting the sync.
        :type reschedule_wait_time: int
        """
        if reschedule_wait_time:
            log_statement = f'Starting connector again in "{reschedule_wait_time}" seconds'
            self.log.info(log_statement)
            time.sleep(reschedule_wait_time)
        else:
            wait_time = (
                self._parse_timestamp(reschedule_for).add(minutes=1) - pendulum.now(tz="UTC")
            ).seconds
            if wait_time < 0:
                raise ValueError(
                    f"Reschedule time {wait_time} configured in "
                    f"Fivetran connector has elapsed. Sync connector manually."
                )
            log_statement = f'Starting connector again in "{wait_time}" seconds'
            self.log.info(log_statement)
            time.sleep(wait_time)

        self.log.info("Restarting connector now")
        return self.start_fivetran_sync(connector_id)

    def _parse_timestamp(self, api_time):
        """
        Returns either the pendulum-parsed actual timestamp or a very out-of-date timestamp if not set.

        :param api_time: timestamp format as returned by the Fivetran API.
        :type api_time: str
        :rtype: Pendulum.DateTime
        """
        return pendulum.parse(api_time) if api_time is not None else pendulum.from_timestamp(-1)

    async def get_last_sync_async(self, connector_id, xcom=""):
        """
        Get the last time Fivetran connector completed a sync.
        Used with FivetranSensorAsync to monitor sync completion status.

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
