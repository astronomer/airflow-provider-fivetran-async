# Changelog

## 2.0.2 (2024-01-17)

### Feature
- Add methods for getting group and connector info by @johnhoran in https://github.com/astronomer/airflow-provider-fivetran-async/pull/80

## 2.0.1 (2024-01-08)

### Bug Fixes
- Fix connection form rendering by @pankajastro in https://github.com/astronomer/airflow-provider-fivetran-async/pull/72
- Remove redundant text from README by @pankajastro in https://github.com/astronomer/airflow-provider-fivetran-async/pull/73

## 2.0.0 (2023-11-07)

### Breaking changes
- FivetranHook.check_connector now returns the connector_details dict object by @dwreeves in https://github.com/astronomer/airflow-provider-fivetran-async/pull/54
- FivetranHook.prep_connector now returns None instead of Literal[True] by @dwreeves in https://github.com/astronomer/airflow-provider-fivetran-async/pull/54
- For users using the sync hook directly to call either get_sync_status or call_and_restart, they will experience breakage if they are using reschedule_time=
and for users who manually set =0, they will also experience breakage by @dwreeves in https://github.com/astronomer/airflow-provider-fivetran-async/pull/57
- Remove deprecated `hook-class-names` from `get_provider_info` return value dict by @pankajastro in https://github.com/astronomer/airflow-provider-fivetran-async/pull/62
- Remove deprecated `FivetranOperatorAsync` and `FivetranSensorAsync` by @pankajastro in https://github.com/astronomer/airflow-provider-fivetran-async/pull/68

### Feature
- API Changes to FivetranSensor for supporting backfills by @dwreeves in https://github.com/astronomer/airflow-provider-fivetran-async/pull/58

### Bug fixes
- Use dynamic operator value in link by @pankajastro in https://github.com/astronomer/airflow-provider-fivetran-async/pull/56
- Update the registry URL for FivetranOperator by @utkarsharma2 in https://github.com/astronomer/airflow-provider-fivetran-async/pull/52
- Updated quotation placement to fix `TypeError` by @cSaller in https://github.com/astronomer/airflow-provider-fivetran-async/pull/65

### Misc
- Add mypy + some other misc. changes by @dwreeves in https://github.com/astronomer/airflow-provider-fivetran-async/pull/54
- Consolidate api for `pause_and_restart` and `_parse_timestamp` methods by @dwreeves in https://github.com/astronomer/airflow-provider-fivetran-async/pull/57

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
