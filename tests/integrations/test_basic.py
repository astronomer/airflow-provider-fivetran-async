import pytest

from airflow.providers_manager import ProvidersManager

@pytest.mark.integration
def test_fivetran_conn():

    manager = ProvidersManager()
    assert  "airflow-provider-fivetran-async" in manager.providers.keys()
