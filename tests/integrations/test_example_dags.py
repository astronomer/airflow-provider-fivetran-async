from __future__ import annotations

import logging
import os
from functools import cache
from pathlib import Path

import airflow
import pytest
from airflow.models.dagbag import DagBag
from airflow.models.variable import Variable
from airflow.utils.db import create_default_connections
from airflow.utils.session import NEW_SESSION, provide_session
from airflow.utils.state import DagRunState
from packaging.version import Version
from sqlalchemy.orm.session import Session

log = logging.getLogger(__name__)

EXAMPLE_DAGS_DIR = Path(__file__).parent.parent.parent / "dev/dags"
AIRFLOW_IGNORE_FILE = EXAMPLE_DAGS_DIR / ".airflowignore"
AIRFLOW_VERSION = Version(airflow.__version__)
IGNORED_DAG_FILES = [
    # Disabled due to infrastructure and credential challenges.
    # This DAG fetches data from LinkedIn and X (formerly Twitter),
    # and also depends on the SSH operator.
    "example_fivetran_dbt.py",
    "example_fivetran_bqml.py",
    # Depends on an operator from the Google provider, which was removed in recent Airflow versions.
    "example_fivetran_bigquery.py",
    # Disabled due to slow performance
    "example_fivetran_resync.py",
]

MIN_VER_DAG_FILE_VER: dict[str, list[str]] = {}


@provide_session
def get_session(session=None):
    create_default_connections(session)
    return session


@pytest.fixture()
def session():
    return get_session()


@provide_session
def setup_variables(session: Session = NEW_SESSION):
    required_vars = ["connector_id", "connector_name", "destination_name"]
    for var_name in required_vars:
        var_key = var_name
        var_val = os.getenv(f"CI_{var_name.upper()}")

        if var_val is None:
            raise Exception("Environment variable %s not set.", f"CI_{var_name.upper()}")

        existing_var = session.query(Variable).filter_by(key=var_key).first()
        if not existing_var:
            session.add(Variable(key=var_key, val=var_val))
            session.commit()
            log.info("Variable %s created.", var_key)
        else:
            log.info("Variable %s already exists.", var_key)


@cache
def get_dag_bag() -> DagBag:
    """Create a DagBag by adding the files that are not supported to .airflowignore"""

    with open(AIRFLOW_IGNORE_FILE, "w+") as file:
        for min_version, files in MIN_VER_DAG_FILE_VER.items():
            if AIRFLOW_VERSION < Version(min_version):
                print(f"Adding {files} to .airflowignore")
                file.writelines([f"{file}\n" for file in files])

        for dagfile in IGNORED_DAG_FILES:
            print(f"Adding {dagfile} to .airflowignore")
            file.writelines([f"{dagfile}\n"])

    # Print the contents of the .airflowignore file, and build the DagBag
    print(".airflowignore contents: ")
    print(AIRFLOW_IGNORE_FILE.read_text())
    db = DagBag(EXAMPLE_DAGS_DIR, include_examples=False)

    assert db.dags
    assert not db.import_errors
    return db


def get_dag_ids() -> list[str]:
    dag_bag = get_dag_bag()
    return dag_bag.dag_ids


@pytest.mark.integration
@pytest.mark.parametrize("dag_id", get_dag_ids())
def test_example_dag(session, dag_id: str):
    setup_variables(session)
    dag_bag = get_dag_bag()
    dag = dag_bag.get_dag(dag_id)

    dag_run = dag.test()
    if dag_run is not None:
        assert dag_run.state == DagRunState.SUCCESS
