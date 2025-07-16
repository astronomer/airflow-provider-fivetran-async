# Fivetran Async Provider for Apache Airflow

[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit&logoColor=white)](https://github.com/pre-commit/pre-commit)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

This package provides an async operator, sensor and hook that integrates [Fivetran](https://fivetran.com) into Apache Airflow.
`FivetranSensor` allows you to monitor a Fivetran sync job for completion before running downstream processes.
`FivetranOperator` submits a Fivetran sync job and polls for its status on the triggerer.
Since an async sensor or operator frees up worker slot while polling is happening on the triggerer,
they consume less resources when compared to traditional "sync" sensors and operators.

[Fivetran automates your data pipeline, and Airflow automates your data processing.](https://www.youtube.com/watch?v=siSx6L2ckSw&ab_channel=Fivetran)

## Installation

Prerequisites: An environment running `apache-airflow`.

```
pip install airflow-provider-fivetran-async
```

## Configuration

In the Airflow user interface, configure a Connection for Fivetran. Most of the Connection config fields will be left blank. Configure the following fields:

* `Conn Id`: `fivetran`
* `Conn Type`: `Fivetran`
* `Login`: Fivetran API Key
* `Password`: Fivetran API Secret

Find the Fivetran API Key and Secret in the [Fivetran Account Settings](https://fivetran.com/account/settings), under the **API Config** section. See our documentation for more information on [Fivetran API Authentication](https://fivetran.com/docs/rest-api/getting-started#authentication).

The sensor assumes the `Conn Id` is set to `fivetran`, however if you are managing multiple Fivetran accounts, you can set this to anything you like. See the DAG in examples to see how to specify a custom `Conn Id`.

## Modules

### [Fivetran Operator Async](https://github.com/astronomer/airflow-provider-fivetran-async/tree/main/fivetran_provider_async/operators.py)

```python
from fivetran_provider_async.operators import FivetranOperator
```

`FivetranOperator` submits a Fivetran sync job and monitors it on trigger for completion.

`FivetranOperator` requires that you specify the `connector_id` of the Fivetran connector you wish to trigger. You can find `connector_id` in the Settings page of the connector you configured in the [Fivetran dashboard](https://fivetran.com/dashboard/connectors).

The `FivetranOperator` will wait for the sync to complete so long as `wait_for_completion=True` (this is the default). It is recommended that
you run in deferrable mode (this is also the default). If `wait_for_completion=False`, the operator will return the timestamp for the last sync.

Import into your DAG via:

### [Fivetran Sensor Async](https://github.com/astronomer/airflow-provider-fivetran-async/tree/main/fivetran_provider_async/sensors.py)

```python
from fivetran_provider_async.sensors import FivetranSensor
```

`FivetranSensor` monitors a Fivetran sync job for completion.
Monitoring with `FivetranSensor` allows you to trigger downstream processes only when the Fivetran sync jobs have completed, ensuring data consistency.

`FivetranSensor` requires that you specify the `connector_id` of the Fivetran connector you want to wait for. You can find `connector_id` in the Settings page of the connector you configured in the [Fivetran dashboard](https://fivetran.com/dashboard/connectors).

You can use multiple instances of `FivetranSensor` to monitor multiple Fivetran connectors.

`FivetranSensor` is most commonly useful in two scenarios:

1. Fivetran is using a separate scheduler than the Airflow scheduler.
2. You set `wait_for_completion=False` in the `FivetranOperator`, and you need to await the `FivetranOperator` task later. (You may want to do this if you want to arrange your DAG such that some tasks are dependent on _starting_ a sync and other tasks are dependent on _completing_ a sync).

If you are doing the 1st pattern, you may find it useful to set the `completed_after_time` to `data_interval_end`, or `data_interval_end` with some buffer:

```python
fivetran_sensor = FivetranSensor(
    task_id="wait_for_fivetran_externally_scheduled_sync",
    connector_id="bronzing_largely",
    poke_interval=5,
    completed_after_time="{{ data_interval_end + macros.timedelta(minutes=1) }}",
)
```

If you are doing the 2nd pattern, you can use XComs to pass the target completed time to the sensor:

```python
fivetran_op = FivetranOperator(
    task_id="fivetran_sync_my_db",
    connector_id="bronzing_largely",
    wait_for_completion=False,
)

fivetran_sensor = FivetranSensor(
    task_id="wait_for_fivetran_db_sync",
    connector_id="bronzing_largely",
    poke_interval=5,
    completed_after_time="{{ task_instance.xcom_pull('fivetran_sync_op', key='return_value') }}",
)

fivetran_op >> fivetran_sensor
```

You may also specify the `FivetranSensor` without a `completed_after_time`.
In this case, the sensor will make note of when the last completed time was, and will wait for a new completed time.

## Examples

See the [**examples**](https://github.com/astronomer/airflow-provider-fivetran-async/tree/main/fivetran_provider_async/dev/dags) directory for an example DAG.

## Issues

Please submit [issues](https://github.com/astronomer/airflow-provider-fivetran-async/issues) and [pull requests](https://github.com/astronomer/airflow-provider-fivetran-async/pulls) in our official repo:
[https://github.com/astronomer/airflow-provider-fivetran-async](https://github.com/astronomer/airflow-provider-fivetran-async)

We are happy to hear from you. Please email any feedback to the authors at [humans@astronomer.io](mailto:humans@astronomer.io).
