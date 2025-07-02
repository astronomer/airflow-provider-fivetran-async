from __future__ import annotations

import logging
import os
import sys
from datetime import datetime
from typing import Any
import pytest

from airflow.configuration import secrets_backend_list
from airflow.exceptions import AirflowSkipException
from airflow.models.dag import DAG
from airflow.models.dagrun import DagRun
from airflow.models.taskinstance import TaskInstance
from airflow.secrets.local_filesystem import LocalFilesystemBackend
from airflow.utils import timezone
from airflow.utils.session import provide_session
from airflow.utils.state import DagRunState, State
from airflow.utils.types import DagRunType
from sqlalchemy.orm.session import Session
from airflow.utils.session import NEW_SESSION
from airflow.models.connection import Connection
from airflow.models.variable import Variable


log = logging.getLogger(__name__)

try:
    from airflow.version import version as AIRFLOW_VERSION
except ImportError:
    from airflow import __version__ as AIRFLOW_VERSION


def run_dag(dag: DAG, conn_file_path: str | None = None) -> DagRun:
    return test_dag(dag=dag, conn_file_path=conn_file_path)


# DAG.test() was added in Airflow version 2.5.0. And to test on older Airflow versions, we need to copy the
# implementation here.
@provide_session
def test_dag(
    dag,
    execution_date: datetime | None = None,
    run_conf: dict[str, Any] | None = None,
    conn_file_path: str | None = None,
    variable_file_path: str | None = None,
    session: Session = NEW_SESSION,
) -> DagRun:
    """
    Execute one single DagRun for a given DAG and execution date.

    :param execution_date: execution date for the DAG run
    :param run_conf: configuration to pass to newly created dagrun
    :param conn_file_path: file path to a connection file in either yaml or json
    :param variable_file_path: file path to a variable file in either yaml or json
    :param session: database connection (optional)
    """

    if conn_file_path or variable_file_path:
        local_secrets = LocalFilesystemBackend(
            variables_file_path=variable_file_path, connections_file_path=conn_file_path
        )
        secrets_backend_list.insert(0, local_secrets)

    execution_date = execution_date or timezone.utcnow()

    dag.log.debug("Clearing existing task instances for execution date %s", execution_date)
    dag.clear(
        start_date=execution_date,
        end_date=execution_date,
        dag_run_state=False,
        session=session,
    )
    dag.log.debug("Getting dagrun for dag %s", dag.dag_id)
    dr: DagRun = _get_or_create_dagrun(
        dag=dag,
        start_date=execution_date,
        execution_date=execution_date,
        run_id=DagRun.generate_run_id(DagRunType.MANUAL, execution_date),
        session=session,
        conf=run_conf,
    )

    tasks = dag.task_dict
    dag.log.debug("starting dagrun")
    # Instead of starting a scheduler, we run the minimal loop possible to check
    # for task readiness and dependency management. This is notably faster
    # than creating a BackfillJob and allows us to surface logs to the user
    while dr.state == State.RUNNING:
        schedulable_tis, _ = dr.update_state(session=session)
        for ti in schedulable_tis:
            add_logger_if_needed(dag, ti)
            ti.task = tasks[ti.task_id]
            _run_task(ti, session=session)
    if conn_file_path or variable_file_path:
        # Remove the local variables we have added to the secrets_backend_list
        secrets_backend_list.pop(0)

    print("conn_file_path", conn_file_path)

    return dr


def add_logger_if_needed(dag: DAG, ti: TaskInstance):
    """
    Add a formatted logger to the taskinstance so all logs are surfaced to the command line instead
    of into a task file. Since this is a local test run, it is much better for the user to see logs
    in the command line, rather than needing to search for a log file.
    Args:
        ti: The taskinstance that will receive a logger

    """
    logging_format = logging.Formatter("[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s")
    handler = logging.StreamHandler(sys.stdout)
    handler.level = logging.INFO
    handler.setFormatter(logging_format)
    # only add log handler once
    if not any(isinstance(h, logging.StreamHandler) for h in ti.log.handlers):
        dag.log.debug("Adding Streamhandler to taskinstance %s", ti.task_id)
        ti.log.addHandler(handler)


def _run_task(ti: TaskInstance, session):
    """
    Run a single task instance, and push result to Xcom for downstream tasks. Bypasses a lot of
    extra steps used in `task.run` to keep our local running as fast as possible
    This function is only meant for the `dag.test` function as a helper function.

    Args:
        ti: TaskInstance to run
    """
    log.info("*****************************************************")
    if hasattr(ti, "map_index") and ti.map_index > 0:
        log.info("Running task %s index %d", ti.task_id, ti.map_index)
    else:
        log.info("Running task %s", ti.task_id)
    try:
        ti._run_raw_task(session=session)
        session.flush()
        log.info("%s ran successfully!", ti.task_id)
    except AirflowSkipException:
        log.info("Task Skipped, continuing")
    log.info("*****************************************************")


def _get_or_create_dagrun(
    dag: DAG,
    conf: dict[Any, Any] | None,
    start_date: datetime,
    execution_date: datetime,
    run_id: str,
    session: Session,
) -> DagRun:
    """
    Create a DAGRun, but only after clearing the previous instance of said dagrun to prevent collisions.
    This function is only meant for the `dag.test` function as a helper function.
    :param dag: Dag to be used to find dagrun
    :param conf: configuration to pass to newly created dagrun
    :param start_date: start date of new dagrun, defaults to execution_date
    :param execution_date: execution_date for finding the dagrun
    :param run_id: run_id to pass to new dagrun
    :param session: sqlalchemy session
    :return:
    """
    log.info("dagrun id: %s", dag.dag_id)
    dr: DagRun = (
        session.query(DagRun).filter(DagRun.dag_id == dag.dag_id, DagRun.execution_date == execution_date).first()
    )
    if dr:
        session.delete(dr)
        session.commit()
    dr = dag.create_dagrun(
        state=DagRunState.RUNNING,
        execution_date=execution_date,
        run_id=run_id,
        start_date=start_date or execution_date,
        session=session,
        conf=conf,
    )
    log.info("created dagrun %s", str(dr))
    return dr

@pytest.fixture
def setup_connection(session: Session,):
    conn_id = "fivetran_default"
    existing_conn = session.query(Connection).filter_by(conn_id=conn_id).first()
    if not existing_conn:
        new_conn = Connection(
        conn_id=conn_id,
        conn_type="fivetran",
        login=os.getenv("CI_FIVETRAN_KEY"),
        password=os.getenv("CI_FIVETRAN_SECRET"),
    )
        session.add(new_conn)
        session.commit()
        log.info(f"Connection '{conn_id}' created.")
    else:
        log.info(f"Connection '{conn_id}' already exists.")

@pytest.fixture
def setup_variables(session: Session):
    required_vars = ["connector_id", "connector_name", "destination_name"]
    for var_name in required_vars:
        var_key = var_name
        var_val = os.getenv(f"CI_{var_name.upper()}")

        if var_val is None:
            log.warning(f"Environment variable CI_{var_name.upper()} not set.")
            continue

        existing_var = session.query(Variable).filter_by(key=var_key).first()
        if not existing_var:
            session.add(Variable(key=var_key, val=var_val))
            log.info(f"Variable '{var_key}' created.")
        else:
            log.info(f"Variable '{var_key}' already exists.")

    session.commit()



