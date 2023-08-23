def _get_table_id(schemas_api_table_name, schemas_api_table, tables_api_tables, loc) -> str:
    """
    Finds a table ID for the given table names
    :param schemas_api_table_name: The source name of the table from the schemas API response
    :type schemas_api_table_name: str
    :param schemas_api_table: The table information from the schemas API response
    :type schemas_api_table: dict
    :param tables_api_tables: The tables returned from the metadata tables API response
    :type tables_api_tables: List[dict]
    :param loc: Short for location, either source or destination
    :type loc: str

    The table returned by the schemas call does not contain the table ID,
    and the columns only contain the table ID as parent ID, so the tables
    values from the tables API call is needed to translate table name to
    table ID to pull only the relevant columns from the unnamed list
    returned by the columns API.
    """
    if loc == "source":
        schema_api_table_name = schemas_api_table.get("name_in_source", schemas_api_table_name)
    else:
        schema_api_table_name = schemas_api_table["name_in_destination"]
    for table_api_table in tables_api_tables["items"]:
        if table_api_table[f"name_in_{loc}"] == schema_api_table_name:
            return table_api_table["id"]
    else:
        raise ValueError(f"{schema_api_table_name} not found in tables_api_tables['items']")


def _get_fields(table_id, columns, loc):
    """
    Creates the SchemaDatasetFacet and necessary SchemaField objects.

    :param table_id: The ID of the table returned from the tables API call.
    :type table_id: str
    :param columns: The columns returned from the columns API call.
    :type: List[dict]
    :param loc: Either the source or location destination.
    :type loc: str
    """
    from fivetran_provider_async import SchemaDatasetFacet, SchemaField

    return SchemaDatasetFacet(
        fields=[
            SchemaField(
                name=col[f"name_in_{loc}"],
                type=col[f"type_in_{loc}"],
            )
            for col in columns["items"]
            if col["parent_id"] == table_id
        ]
    )


def _get_openlineage_name(config, service, schema, table) -> str:
    """
    Creates an OpenLineage-compliant name from the relevant Fivetran API
    data. For more information, see the spec:
    https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md

    :param config: The config dict returned from the connector API call.
    :type config: dict
    :param service: The service type returned from the connector or destinations
                    API call.
    :type service: str
    :param schema: The schema returned from the schema API call.
    :type schema: dict
    :param table: The table name. Not always used.
    :type table: str
    """
    if service == "gcs":
        pattern = config["pattern"].replace("\\", "")
        return f"{config['prefix']}{pattern}"
    elif service == "google_sheets":
        return schema["name_in_destination"]
    elif service == "snowflake" or service == "redshift":
        return f"{config['database']}.{schema['name_in_destination']}.{table}".lower()
    # TODO: find where dataset and table are returned in Fivetran API response for BigQuery
    # elif service == "bigquery":
    #    return f"{config['project_id']}.{}.{}}"
    else:
        # defaulting to just returning the schema name in destination for now.
        return schema["name_in_destination"]


def _get_openlineage_namespace(config, service, connector_id) -> str:
    """
    Creates an OpenLineage-compliant namespace from the relevant
    Fivetran API data by combining the service and authority. For more information,
    see the spec:
    https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md

    :param config: The config dict returned from the connector API call.
    :type config: dict
    :param service: The service type returned from the connector or destinations
                    API call.
    :type service: str
    """
    if service == "snowflake":
        name_split = config["host"].split(".")
        return f"snowflake://{name_split[0]}.{name_split[1]}.{config['snowflake_cloud']}".lower()
    elif service == "gcs":
        return f"gs://{config['bucket']}"
    elif service == "redshift":
        name_split = config["host"].split(".")
        return f"redshift://{name_split[1]}.{config['cluster_region']}:{config['port']}"
    elif service == "bigquery":
        return "bigquery:"
    elif service == "google_sheets":
        return "sheets://"
    else:
        # Default namespace for now
        return f"fivetran://{connector_id}"


def datasets(
    config,
    service,
    table_response,
    column_response,
    schema,
    connector_id,
    loc,
):
    from fivetran_provider_async import Dataset, DataSourceDatasetFacet

    datasets = []

    for table_name, table in schema["tables"].items():
        name = _get_openlineage_name(config, service, schema, table["name_in_destination"])
        namespace = _get_openlineage_namespace(config, service, connector_id)
        uri = f"{namespace}/{name}"

        datasets.extend(
            [
                Dataset(
                    namespace=namespace,
                    name=name,
                    facets={
                        "schema": _get_fields(
                            table_id=_get_table_id(table_name, table, table_response, loc),
                            columns=column_response,
                            loc=loc,
                        ),
                        "datasource": DataSourceDatasetFacet(name=name, uri=uri),
                    },
                )
            ]
        )
    return datasets
