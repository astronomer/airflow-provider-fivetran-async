# ruff: noqa F401
__version__ = "2.2.1a1"

import logging

log = logging.getLogger(__name__)

try:
    # Airflow 2.7+ / 3.0+ preferred: use the OpenLineage provider package
    from airflow.providers.openlineage.extractors.base import (  # type: ignore[import]
        OperatorLineage,
    )
except (ImportError, ModuleNotFoundError):
    try:
        # Fallback: standalone openlineage-airflow package
        from openlineage.airflow.extractors.base import (  # type: ignore[import]
            OperatorLineage,
        )
    except (ImportError, ModuleNotFoundError):
        OperatorLineage = None  # type: ignore[misc, assignment]
        logging.debug("OpenLineage not available.")

try:
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
    DataSourceDatasetFacet = None  # type: ignore[misc, assignment]
    DocumentationJobFacet = None  # type: ignore[misc, assignment]
    ErrorMessageRunFacet = None  # type: ignore[misc, assignment]
    OwnershipJobFacet = None  # type: ignore[misc, assignment]
    OwnershipJobFacetOwners = None  # type: ignore[misc, assignment]
    SchemaDatasetFacet = None  # type: ignore[misc, assignment]
    SchemaField = None  # type: ignore[misc, assignment]
    Dataset = None  # type: ignore[misc, assignment]
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
