# Changelog

## 2.2.0a1 (2025-05-07)

### Feature

- Add `ResyncOperator` by @johnhoran in https://github.com/astronomer/airflow-provider-fivetran-async/pull/140

### Others

- Pre-commit hook updates in https://github.com/astronomer/airflow-provider-fivetran-async/pull/138, https://github.com/astronomer/airflow-provider-fivetran-async/pull/141 and https://github.com/astronomer/airflow-provider-fivetran-async/pull/142

## 2.1.3 (2025-04-17)

### Bug Fixes

- fix: pass last_sync when deferring by @m1racoli in https://github.com/astronomer/airflow-provider-fivetran-async/pull/131
- Add missing provider entry point by @pankajastro in https://github.com/astronomer/airflow-provider-fivetran-async/pull/136

### Others

- ci: replace flake8 with ruff by @m1racoli in https://github.com/astronomer/airflow-provider-fivetran-async/pull/133
- Pre-commit hook updates in https://github.com/astronomer/airflow-provider-fivetran-async/pull/129, https://github.com/astronomer/airflow-provider-fivetran-async/pull/132 and https://github.com/astronomer/airflow-provider-fivetran-async/pull/135

## 2.1.2 (2025-03-28)

### Bug Fixes

- Allow ``FivetranSensor`` poke_interval to work as expected by @stubs in https://github.com/astronomer/airflow-provider-fivetran-async/pull/125

### Others

- Add contributing guidelines by @pankajastro in https://github.com/astronomer/airflow-provider-fivetran-async/pull/127
- Use single packaging tool pyproject.toml by @pankajastro in https://github.com/astronomer/airflow-provider-fivetran-async/pull/126
- Pre-commit hook updates in https://github.com/astronomer/airflow-provider-fivetran-async/pull/118, https://github.com/astronomer/airflow-provider-fivetran-async/pull/120, https://github.com/astronomer/airflow-provider-fivetran-async/pull/121, https://github.com/astronomer/airflow-provider-fivetran-async/pull/122 and https://github.com/astronomer/airflow-provider-fivetran-async/pull/123

## 2.1.1 (2024-12-30)

### Bug Fixes

- Ensure proper preparation of API call ``kwargs`` for synchronous methods in ``FivetranHookAsync`` by @JeremyDOwens in https://github.com/astronomer/airflow-provider-fivetran-async/pull/115

### Others

- Pre-commit hook updates in https://github.com/astronomer/airflow-provider-fivetran-async/pull/104, https://github.com/astronomer/airflow-provider-fivetran-async/pull/105,  https://github.com/astronomer/airflow-provider-fivetran-async/pull/112, https://github.com/astronomer/airflow-provider-fivetran-async/pull/113, https://github.com/astronomer/airflow-provider-fivetran-async/pull/114 and https://github.com/astronomer/airflow-provider-fivetran-async/pull/116

## 2.1.0 (2024-07-24)

### Feature
- Add methods to get the connector_id by @natanlaverde in https://github.com/astronomer/airflow-provider-fivetran-async/pull/101

### Bug Fixes
- Set correct default_conn_name in class by @pankajastro in https://github.com/astronomer/airflow-provider-fivetran-async/pull/95
- Add return statement after yield in trigger to avoid infinite loop by @pankajastro in https://github.com/astronomer/airflow-provider-fivetran-async/pull/94

### Misc
- Enable Python3.12 test by @pankajastro in https://github.com/astronomer/airflow-provider-fivetran-async/pull/91
- Remove old Async operator name from docstring by @pankajastro in https://github.com/astronomer/airflow-provider-fivetran-async/pull/96
- Clean example DAG by @pankajastro in https://github.com/astronomer/airflow-provider-fivetran-async/pull/97

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
