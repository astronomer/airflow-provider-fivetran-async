def get_provider_info():
    return {
        "package-name": "airflow-provider-fivetran-async",
        "name": "Fivetran Async Provider",
        "description": "A Fivetran Async provider for Apache Airflow.",
        "hook-class-names": ["fivetran_provider_async.hooks.fivetran.FivetranHookAsync"],
        "extra-links": ["fivetran_provider.operators.fivetran.RegistryLink"],
        "versions": ["1.0.1"]
    }
