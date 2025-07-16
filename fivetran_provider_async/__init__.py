# ruff: noqa F401
__version__ = "2.2.0a2"

import logging

log = logging.getLogger(__name__)

try:
    from openlineage.airflow.extractors.base import (  # type: ignore[import]
        OperatorLineage,
    )
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
        "connection-types": [
            {
                "hook-class-name": "fivetran_provider_async.hooks.FivetranHook",
                "connection-type": "Fivetran",
            },
        ],
        "extra-links": ["fivetran_provider_async.operators.RegistryLink"],
        "versions": [__version__],
    }
