# Changelog

## 1.1.2 (2023-08-10)

### Bug fixes
- Remove uses of fivetran_provider [#46](https://github.com/astronomer/airflow-provider-fivetran-async/pull/46)

## 1.1.1 (2023-08-07)

### Feature
- Add Python3.11 support [#37](https://github.com/astronomer/airflow-provider-fivetran-async/pull/37)
- Deprecate `FivetranOperatorAsync` in favour of `FivetranOperator` and `FivetranSensorAsync` in favour of `FivetranSensor` [#40](https://github.com/astronomer/airflow-provider-fivetran-async/pull/40)
- Add `deferrable` param in `FivetranOperator` and `FivetranSensor` with default value True [#42](https://github.com/astronomer/airflow-provider-fivetran-async/pull/42)

### Bug Fixes
- Fix typo in trigger docstring [#42](https://github.com/astronomer/airflow-provider-fivetran-async/pull/42)
