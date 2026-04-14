#!/bin/bash
#!/usr/bin/env bash

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

# Find the Airflow version to install
# Pin 3.0 to 3.0.0 due to dag.test() regression in later 3.0.x task-sdk releases
if [ "$AIRFLOW_VERSION" = "3.0" ]; then
  INSTALL_AIRFLOW_VERSION="3.0.0"
else
  INSTALL_AIRFLOW_VERSION=$(curl -s "https://pypi.org/pypi/apache-airflow/json" | \
    python3 -c "import sys,json,re; d=json.load(sys.stdin); vs=[v for v in d['releases'] if v.startswith('${AIRFLOW_VERSION}.') and re.fullmatch(r'\d+\.\d+\.\d+', v) and not any(f.get('yanked') for f in d['releases'][v])]; vs.sort(key=lambda v:[int(x) for x in v.split('.')]); print(vs[-1])")
fi
echo "Installing Airflow: ${INSTALL_AIRFLOW_VERSION}"

# Install Airflow
pip install uv
uv pip install "apache-airflow==${INSTALL_AIRFLOW_VERSION}"

# Install OpenLineage provider, pinning Airflow to avoid upgrades
if [[ "$AIRFLOW_VERSION" == 2.* ]]; then
  uv pip install "openlineage-airflow>=0.19.2" "apache-airflow==${INSTALL_AIRFLOW_VERSION}"
else
  uv pip install apache-airflow-providers-openlineage "apache-airflow==${INSTALL_AIRFLOW_VERSION}"
fi

actual_airflow_version=$(airflow version 2>/dev/null | tail -1 | cut -d. -f1,2)
desired_airflow_version=$(echo $AIRFLOW_VERSION | cut -d. -f1,2)

if [ "$actual_airflow_version" = "$desired_airflow_version" ]; then
    echo "Version is as expected: $desired_airflow_version"
else
    echo "ERROR: Expected Airflow $desired_airflow_version but got $actual_airflow_version"
    exit 1
fi
