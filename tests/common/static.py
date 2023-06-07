import pendulum

LOGIN = "login"
PASSWORD = "password"

MOCK_FIVETRAN_RESPONSE_PAYLOAD_SHEETS = {
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

MOCK_FIVETRAN_RESPONSE_PAYLOAD_SHEETS_RESCHEDULE_MODE = {
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
            "sync_state": "rescheduled",
            "update_state": "on_schedule",
            "is_historical_sync": False,
            "tasks": [],
            "warnings": [],
            "rescheduled_for": "2021-03-05T22:59:56.238875Z",
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

MOCK_FIVETRAN_RESPONSE_PAYLOAD_SHEETS_WITH_RESCHEDULE_FOR = {
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
            "sync_state": "rescheduled",
            "update_state": "on_schedule",
            "is_historical_sync": False,
            "tasks": [],
            "warnings": [],
            "rescheduled_for": str(pendulum.now(tz="UTC").add(minutes=1)),
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

MOCK_FIVETRAN_SCHEMA_RESPONSE_PAYLOAD_SHEETS = {
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
                    }
                },
            }
        },
    },
}

MOCK_FIVETRAN_METADATA_TABLES_RESPONSE_PAYLOAD_SHEETS = {
    "code": "Success",
    "data": {
        "items": [
            {
                "id": "NjgyMDM0OQ",
                "parent_id": "ZGVtbw",
                "name_in_source": "table_1",
                "name_in_destination": "table_1",
            }
        ]
    },
}

MOCK_FIVETRAN_METADATA_COLUMNS_RESPONSE_PAYLOAD_SHEETS = {
    "code": "Success",
    "data": {
        "items": [
            {
                "id": "MjE0NDM2ODE2",
                "parent_id": "NjgyMDM0OQ",
                "name_in_source": "column_1",
                "name_in_destination": "column_1_dest",
                "type_in_source": "String",
                "type_in_destination": "VARCHAR(256)",
                "is_primary_key": True,
                "is_foreign_key": False,
            },
        ]
    },
}

MOCK_FIVETRAN_DESTINATIONS_RESPONSE_PAYLOAD_SHEETS = {
    "code": "Success",
    "data": {
        "id": "rarer_gradient",
        "group_id": "rarer_gradient",
        "service": "google_sheets",
        "region": "GCP_US_EAST4",
        "time_zone_offset": "-8",
        "setup_status": "connected",
        "config": {"sheet_id": "google_sheets.fivetran_google_sheets_spotify"},
    },
}

MOCK_FIVETRAN_GROUPS_RESPONSE_PAYLOAD_SHEETS = {
    "code": "Success",
    "data": {"id": "rarer_gradient", "name": "GoogleSheets", "created_at": "2022-12-12T17:14:33.790844Z"},
}

"""
GCS --> Snowflake Mock API Responses
"""

MOCK_FIVETRAN_CONNECTOR_RESPONSE_PAYLOAD_GCS_TO_SNOWFLAKE = {
    "code": "Success",
    "data": {
        "id": "cropping_limitation",
        "group_id": "stowing_sway",
        "service": "gcs",
        "service_version": 1,
        "schema": "demo.subscription_periods",
        "connected_by": "pummel_versa",
        "created_at": "2022-12-13T21:27:47.327348Z",
        "succeeded_at": "2023-01-05T18:12:49.562Z",
        "failed_at": None,
        "paused": False,
        "pause_after_trial": False,
        "sync_frequency": 1440,
        "daily_sync_time": "23:00",
        "schedule_type": "manual",
        "status": {
            "setup_state": "connected",
            "schema_status": "ready",
            "sync_state": "scheduled",
            "update_state": "on_schedule",
            "is_historical_sync": False,
            "tasks": [],
            "warnings": [],
        },
        "config": {
            "skip_before": 0,
            "skip_after": 0,
            "prefix": "fivetran-demo/",
            "pattern": "subscription_periods\\.csv",
            "file_type": "infer",
            "on_error": "fail",
            "compression": "infer",
            "append_file_option": "upsert_file",
            "empty_header": False,
            "bucket": "test",
            "auth_type": "FIVETRAN_SERVICE_ACCOUNT",
        },
    },
}

MOCK_FIVETRAN_SCHEMAS_RESPONSE_PAYLOAD_GCS_TO_SNOWFLAKE = {
    "code": "Success",
    "data": {
        "enable_new_by_default": True,
        "schemas": {
            "demo": {
                "name_in_destination": "demo",
                "enabled": True,
                "tables": {
                    "subscription_periods": {
                        "name_in_destination": "subscription_periods",
                        "enabled": True,
                        "enabled_patch_settings": {"allowed": True},
                    }
                },
            }
        },
        "schema_change_handling": "ALLOW_ALL",
    },
}

MOCK_FIVETRAN_DESTINATIONS_RESPONSE_PAYLOAD_GCS_TO_SNOWFLAKE = {
    "code": "Success",
    "data": {
        "id": "stowing_sway",
        "group_id": "stowing_sway",
        "service": "snowflake",
        "region": "GCP_US_EAST4",
        "time_zone_offset": "-8",
        "setup_status": "connected",
        "config": {
            "snowflake_region": "us_east_1",
            "database": "TEST",
            "password": "******",
            "role": "TEST_USER",
            "connection_type": "Directly",
            "port": "443",
            "auth": "PASSWORD",
            "snowflake_cloud": "AWS",
            "host": "hq12345.us-east-1.snowflakecomputing.com",
            "user": "TEST_USER",
        },
    },
}
