#!/bin/bash

set -x
set -e


pip freeze | grep airflow
echo $AIRFLOW_HOME
ls $AIRFLOW_HOME

airflow db check

pytest -vv \
    --cov=fivetran_provider_async \
    --cov-report=term-missing \
    --cov-report=xml \
    -m integration
