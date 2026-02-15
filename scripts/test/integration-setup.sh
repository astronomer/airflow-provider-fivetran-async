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

# Airflow 3.0+ replaced 'airflow db init' with 'airflow db migrate'
AIRFLOW_VERSION=$(airflow version)
AIRFLOW_MAJOR_VERSION=$(echo "$AIRFLOW_VERSION" | cut -d. -f1)
if [ "$AIRFLOW_MAJOR_VERSION" -ge 3 ]; then
    echo "Detected Airflow $AIRFLOW_VERSION. Running 'airflow db migrate'..."
    airflow db migrate
else
    echo "Detected Airflow $AIRFLOW_VERSION. Running 'airflow db init'..."
    airflow db init
fi
