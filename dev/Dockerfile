ARG IMAGE_NAME
FROM ${IMAGE_NAME}

USER root
RUN apt-get update -y && apt-get install -y git
RUN apt-get install -y --no-install-recommends \
        build-essential \
        libsasl2-2 \
        libsasl2-dev \
        libsasl2-modules

COPY setup.cfg ${AIRFLOW_HOME}/airflow_provider_fivetran_async/setup.cfg
COPY pyproject.toml ${AIRFLOW_HOME}/airflow_provider_fivetran_async/pyproject.toml

COPY fivetran_provider_async/__init__.py ${AIRFLOW_HOME}/airflow_provider_fivetran_async/fivetran_provider_async/__init__.py

RUN pip install -e "${AIRFLOW_HOME}/airflow_provider_fivetran_async[all,tests]"
USER astro
