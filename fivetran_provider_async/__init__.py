# ruff: noqa F401
__version__ = "2.2.1a1"

import logging

log = logging.getLogger(__name__)

# OpenLineage support (Airflow 2.9+): requires apache-airflow-providers-openlineage.
# Compat and openlineage.client facets; see airflow.providers.openlineage docs.
try:
    from airflow.providers.openlineage.extractors import OperatorLineage  # type: ignore[import]
    from airflow.providers.common.compat.openlineage.facet import (  # type: ignore[import]
        Dataset,
        ErrorMessageRunFacet,
        SchemaDatasetFacet,
        SchemaDatasetFacetFields,
    )
    from openlineage.client.facet import (  # type: ignore[import]
        DataSourceDatasetFacet,
        DocumentationJobFacet,
        OwnershipJobFacet,
        OwnershipJobFacetOwners,
    )
    SchemaField = SchemaDatasetFacetFields  # legacy name for operator_utils
except ImportError:
    log.debug("apache-airflow-providers-openlineage not installed; OpenLineage facets unavailable")


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
