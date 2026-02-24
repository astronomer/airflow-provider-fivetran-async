#!/bin/bash

set -v
set -x
set -e

AIRFLOW_VERSION="$1"
PYTHON_VERSION="$2"

# Use this to set the appropriate Python environment in Github Actions,
# while also not assuming --system when running locally.
if [ "$GITHUB_ACTIONS" = "true" ] && [ -z "${VIRTUAL_ENV}" ]; then
  py_path=$(which python)
  virtual_env_dir=$(dirname "$(dirname "$py_path")")
  export VIRTUAL_ENV="$virtual_env_dir"
fi

echo "${VIRTUAL_ENV}"

# Set constraint URL based on Airflow version
if [ "$AIRFLOW_VERSION" = "3.0" ]; then
  CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-$AIRFLOW_VERSION.2/constraints-$PYTHON_VERSION.txt"
elif [ "$AIRFLOW_VERSION" = "3.1" ]; then
  CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-$AIRFLOW_VERSION.0/constraints-$PYTHON_VERSION.txt"
else
  CONSTRAINT_URL="https://raw.githubusercontent.com/apache/airflow/constraints-$AIRFLOW_VERSION.0/constraints-$PYTHON_VERSION.txt"
fi

curl -sSL "$CONSTRAINT_URL" -o /tmp/constraint.txt
# Workaround to remove PyYAML constraint that will work on both Linux and MacOS
sed '/PyYAML==/d' /tmp/constraint.txt > /tmp/constraint.txt.tmp
mv /tmp/constraint.txt.tmp /tmp/constraint.txt

# Install Airflow with constraints
pip install uv
uv pip install pip --upgrade

if [ "$AIRFLOW_VERSION" = "3.0" ]; then
  # Airflow 3.0.x installation - use exact version to match constraints-3.0.2
  uv pip install "apache-airflow==3.0.2" --constraint /tmp/constraint.txt
  uv pip install "apache-airflow-providers-cncf-kubernetes" --constraint /tmp/constraint.txt
elif [ "$AIRFLOW_VERSION" = "3.1" ]; then
  # Airflow 3.1.x installation
  uv pip install "apache-airflow==$AIRFLOW_VERSION" --constraint /tmp/constraint.txt
  uv pip install "apache-airflow-providers-cncf-kubernetes" --constraint /tmp/constraint.txt
else
  # Airflow 2.x installation
  uv pip install "apache-airflow==$AIRFLOW_VERSION" --constraint /tmp/constraint.txt
  pip install "apache-airflow-providers-cncf-kubernetes" --constraint /tmp/constraint.txt
fi

rm /tmp/constraint.txt

# Verify installed version matches expected version
actual_version=$(airflow version | cut -d. -f1,2)
desired_version=$(echo "$AIRFLOW_VERSION" | cut -d. -f1,2)

if [ "$actual_version" = "$desired_version" ]; then
    echo "Version is as expected: $desired_version"
else
    echo "Version does not match. Expected: $desired_version, but got: $actual_version"
    exit 1
fi
