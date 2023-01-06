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
                "name_in_source": "table_1",
                "name_in_destination": "table_1",
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

MOCK_FIVETRAN_DESTINATIONS_RESPONSE_PAYLOAD = {
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

MOCK_FIVETRAN_GROUPS_RESPONSE_PAYLOAD = {
    "code": "Success",
    "data": {"id": "rarer_gradient", "name": "GoogleSheets", "created_at": "2022-12-12T17:14:33.790844Z"},
}
