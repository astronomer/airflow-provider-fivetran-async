# Fivetran Async Provider for Apache Airflow

This package provides an async operator, sensor and hook that integrates [Fivetran](https://fivetran.com) into Apache Airflow.
`FivetranSensorAsync` allows you to monitor a Fivetran sync job for completion before running downstream processes.
`FivetranOperatorAsync` submits a Fivetran sync job and polls for its status on the triggerer.
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

`FivetranOperatorAsync` submits a Fivetran sync job and monitors it on trigger for completion.
It requires that you specify the `connector_id` of the sync job to start. You can find `connector_id` in the Settings page of the connector you configured in the [Fivetran dashboard](https://fivetran.com/dashboard/connectors).

Import into your DAG via:
```python
from fivetran_provider_async.operators import FivetranOperatorAsync
```

### [Fivetran Sensor Async](https://github.com/astronomer/airflow-provider-fivetran-async/tree/main/fivetran_provider_async/sensors.py)

`FivetranSensorAsync` monitors a Fivetran sync job for completion.
Monitoring with `FivetranSensorAsync` allows you to trigger downstream processes only when the Fivetran sync jobs have completed, ensuring data consistency.



You can use multiple instances of `FivetranSensorAsync` to monitor multiple Fivetran connectors.

If used in this way,

`FivetranSensorAsync` requires that you specify the `connector_id` of the sync job to start. You can find `connector_id` in the Settings page of the connector you configured in the [Fivetran dashboard](https://fivetran.com/dashboard/connectors).

Import into your DAG via:
```python
from fivetran_provider_async.sensors import FivetranSensorAsync
```

## Examples

See the [**examples**](https://github.com/astronomer/airflow-provider-fivetran-async/tree/main/fivetran_provider_async/example_dags) directory for an example DAG.

## Issues

Please submit [issues](https://github.com/astronomer/airflow-provider-fivetran-async/issues) and [pull requests](https://github.com/astronomer/airflow-provider-fivetran-async/pulls) in our official repo:
[https://github.com/astronomer/airflow-provider-fivetran-async](https://github.com/astronomer/airflow-provider-fivetran-async)

We are happy to hear from you. Please email any feedback to the authors at [humans@astronomer.io](mailto:humans@astronomer.io).
