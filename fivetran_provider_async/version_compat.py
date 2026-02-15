from __future__ import annotations


def get_base_airflow_version_tuple() -> tuple[int, int, int]:
    from packaging.version import Version

    from airflow import __version__

    airflow_version = Version(__version__)
    return airflow_version.major, airflow_version.minor, airflow_version.micro


AIRFLOW_V_3_0_PLUS = get_base_airflow_version_tuple() >= (3, 0, 0)
AIRFLOW_V_3_1_PLUS = get_base_airflow_version_tuple() >= (3, 1, 0)


__all__ = [
    "AIRFLOW_V_3_0_PLUS",
    "AIRFLOW_V_3_1_PLUS",
]
