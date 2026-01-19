"""Airflow version compatibility utilities for Fivetran provider."""

from __future__ import annotations

from airflow import __version__ as airflow_version
from packaging.version import Version

AIRFLOW_VERSION = Version(airflow_version)
AIRFLOW_3_0_PLUS = AIRFLOW_VERSION >= Version("3.0.0")
AIRFLOW_3_1_PLUS = AIRFLOW_VERSION >= Version("3.1.0")
