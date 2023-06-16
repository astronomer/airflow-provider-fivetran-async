# flake8: noqa F401
__version__ = "1.1.0"

import logging

log = logging.getLogger(__name__)


try:
    from openlineage.airflow.extractors.base import OperatorLineage
    from openlineage.client.facet import (
        DataSourceDatasetFacet,
        DocumentationJobFacet,
        ErrorMessageRunFacet,
        OwnershipJobFacet,
        OwnershipJobFacetOwners,
        SchemaDatasetFacet,
        SchemaField,
    )
    from openlineage.client.run import Dataset
except ImportError:
    logging.debug("openlineage-airflow python dependency is missing")


def get_provider_info():
    return {
        "package-name": "airflow-provider-fivetran-async",
        "name": "Fivetran Async Provider",
        "description": "A Fivetran Async provider for Apache Airflow.",
        "hook-class-names": ["fivetran_provider_async.hooks.FivetranHookAsync"],
        "extra-links": ["fivetran_provider.operators.fivetran.RegistryLink"],
        "versions": [__version__],
    }
