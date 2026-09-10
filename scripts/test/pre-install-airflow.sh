#!/bin/sh
# pyproject.toml invokes this via `sh`, which is dash on ubuntu-latest -- keep it POSIX (no `[[ ]]`).
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

# Find the Airflow version to install
# Pin 3.0 to 3.0.0 due to dag.test() regression in later 3.0.x task-sdk releases
if [ "$AIRFLOW_VERSION" = "3.0" ]; then
  INSTALL_AIRFLOW_VERSION="3.0.0"
else
  INSTALL_AIRFLOW_VERSION=$(curl -s "https://pypi.org/pypi/apache-airflow/json" | \
    python3 -c "import sys,json,re; d=json.load(sys.stdin); vs=[v for v in d['releases'] if v.startswith('${AIRFLOW_VERSION}.') and re.fullmatch(r'\d+\.\d+\.\d+', v) and not any(f.get('yanked') for f in d['releases'][v])]; vs.sort(key=lambda v:[int(x) for x in v.split('.')]); print(vs[-1])")
fi
echo "Installing Airflow: ${INSTALL_AIRFLOW_VERSION}"

# Pin transitive deps (e.g. cadwyn) to what this Airflow release was actually tested against,
# instead of whatever's newest on install day -- an unrelated upstream major bump can otherwise
# silently break dag.test() on a previously-green Airflow version.
CONSTRAINTS_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${INSTALL_AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

# Install Airflow
pip install uv
# Some constraints files don't match what the release actually supports for a given Python
# version (e.g. constraints-3.0.0/constraints-3.13.txt requires termcolor<3.0, which conflicts
# with apache-airflow-core==3.0.0 itself -- Airflow 3.0.0 never shipped Python 3.13 support).
# Fall back to unconstrained rather than fail outright, but warn loudly: this drops every pin
# --constraint exists for, for any failure reason, not just this one known case.
if ! uv pip install --constraint "${CONSTRAINTS_URL}" "apache-airflow==${INSTALL_AIRFLOW_VERSION}"; then
  echo "::warning::Constrained install failed for apache-airflow==${INSTALL_AIRFLOW_VERSION} / Python ${PYTHON_VERSION} (constraints file may not match this Python version, or may not exist yet); falling back to an unconstrained install, which reintroduces unpinned transitive deps for this lane"
  uv pip install "apache-airflow==${INSTALL_AIRFLOW_VERSION}"
fi

# No --constraint here: this provider declares its own openlineage-airflow range in pyproject.toml,
# and Airflow's constraints would drag that stack down to whatever it happened to pin. The cadwyn
# pin above still holds -- uv won't touch an already-satisfied dependency.
case "$AIRFLOW_VERSION" in
  2.*)
    uv pip install "openlineage-airflow>=0.19.2" "apache-airflow==${INSTALL_AIRFLOW_VERSION}"
    ;;
  *)
    uv pip install apache-airflow-providers-openlineage "apache-airflow==${INSTALL_AIRFLOW_VERSION}"
    ;;
esac

actual_airflow_version=$(airflow version 2>/dev/null | tail -1 | cut -d. -f1,2)
desired_airflow_version=$(echo $AIRFLOW_VERSION | cut -d. -f1,2)

if [ "$actual_airflow_version" = "$desired_airflow_version" ]; then
    echo "Version is as expected: $desired_airflow_version"
else
    echo "ERROR: Expected Airflow $desired_airflow_version but got $actual_airflow_version"
    exit 1
fi
