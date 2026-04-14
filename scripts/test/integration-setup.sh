#!/bin/bash

set -v
set -x
set -e

if [ -L "dags" ]; then
    echo "Symbolic link 'dags' already exists."
elif [ -e "dags" ]; then
    echo "'dags' exists but is not a symbolic link. Please resolve this manually."
else
    ln -s dev/dags dags
    echo "Symbolic link 'dags' created successfully."
fi

rm -rf airflow.*
pip freeze | grep airflow
airflow db reset -y

AIRFLOW_MAJOR_VERSION=$(airflow version 2>/dev/null | tail -1 | cut -d. -f1)
if [ "$AIRFLOW_MAJOR_VERSION" -ge 3 ]; then
  echo "Detected Airflow 3.x. Running 'airflow db migrate'..."
  airflow db migrate
else
  echo "Detected Airflow 2.x. Running 'airflow db init'..."
  airflow db init
fi
