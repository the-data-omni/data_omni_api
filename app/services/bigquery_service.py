"""
Module providing functions and queries to interact with a BigQuery database.
"""

import logging
import re
import os
from flask import json, jsonify, session
from google.cloud import bigquery
from ..utils.logging_config import logger
from google.cloud import bigquery
from google.oauth2 import service_account



def get_bigquery_client_from_session():
    """
    Retrieve a BigQuery client from:
      1) The user's session-based service account JSON, if present.
      2) Otherwise, fall back to a local service_account.json file in app/ folder.
      3) If neither is available, raise an exception.
    """
    sa_json = session.get("service_account_json")

    if not sa_json:
        # Fallback: check if there's a local service_account.json in app/ directory
        service_account_path = os.path.join(
            os.path.dirname(__file__),  # current directory: app/services
            "..",                       # go up one level to "app"
            "service_account.json"
        )

        if os.path.exists(service_account_path):
            with open(service_account_path, "r", encoding="utf-8") as f:
                sa_json = json.load(f)
        else:
            raise Exception(
                "No service account available in session and no local service_account.json found in app/. "
                "Please upload one via /upload_service_account."
            )

    # Convert the JSON into credentials and build a BigQuery client
    creds = service_account.Credentials.from_service_account_info(sa_json)
    return bigquery.Client(credentials=creds, project=creds.project_id)


def get_queries(client: bigquery.Client, time_interval="10 day") -> dict:
    """
    Call a query to get historical queries from the past `time_interval`
    (defaults to 90 days). Returns a dictionary with query text as keys
    and the number of times each query appeared as values.

    :param client: A BigQuery client (user-specific or default).
    :type client: bigquery.Client
    :param time_interval: The time interval for querying historical data.
    :type time_interval: str
    :return: Dictionary of queries and their frequencies.
    :rtype: dict
    """
    query = (
        f"""
        SELECT query, creation_time
        FROM region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT
        WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {time_interval})
          AND state = 'DONE'
          AND job_type = 'QUERY'
          AND NOT REGEXP_CONTAINS(LOWER(query), r'\\binformation_schema\\b')
        """
    )
    client = get_bigquery_client_from_session()
    query_job = client.query(query)
    query_counts = {}

    for row in query_job.result():
        query_text = row.query
        query_counts[query_text] = query_counts.get(query_text, 0) + 1

    return query_counts


def get_bigquery_info(client: bigquery.Client):
    """
    Retrieve BigQuery metadata, including project ID, datasets, tables,
    and each table's field information (including constraints if available).

    :param client: A BigQuery client (user-specific or default).
    :type client: bigquery.Client
    :return: A Flask response (JSON) with project info, or an error response.
    :rtype: Tuple[flask.Response, int]
    """
    try:
        project_id = client.project
        logger.info("BigQuery Client Project: %s", project_id)

        project_info = {
            "project_id": project_id,
            "datasets": []
        }

        datasets = list(client.list_datasets())
        if not datasets:
            logger.info("No datasets found in project: %s", project_id)
            return jsonify(project_info), 200

        # Pre-fetch constraints for each dataset
        constraints_data = {}
        for dataset in datasets:
            dataset_id = dataset.dataset_id
            logger.info("Fetching constraints for dataset: %s", dataset_id)
            constraints = get_constraints(client, dataset_id)
            constraints_data[dataset_id] = constraints

        # Build project_info
        shard_pattern = re.compile(r'^(.*)_\d{8}$')

        for dataset in datasets:
            dataset_id = dataset.dataset_id
            logger.info("Processing dataset: %s", dataset_id)
            dataset_info = {
                "dataset_id": dataset_id,
                "tables": []
            }

            tables = client.list_tables(dataset_id)
            if not tables:
                logger.info("No tables found in dataset: %s", dataset_id)
                project_info["datasets"].append(dataset_info)
                continue

            # Track which normalized table has been added
            seen_normalized = set()

            for table in tables:
                original_table_id = table.table_id
                logger.info(
                    "Processing table: %s in dataset: %s",
                    original_table_id,
                    dataset_id
                )
                # Check if it matches the shard pattern (e.g. table_YYYYMMDD)
                match = shard_pattern.match(original_table_id)
                if match:
                    normalized_table_id = f"{match.group(1)}_*"
                else:
                    normalized_table_id = original_table_id

                # If we've already handled this normalized table, skip
                if normalized_table_id in seen_normalized:
                    logger.info(
                        "Skipping table %s because we already have %s",
                        original_table_id,
                        normalized_table_id
                    )
                    continue

                # Mark as seen
                seen_normalized.add(normalized_table_id)

                # Retrieve schema and constraints for this table
                table_ref = client.dataset(dataset_id).table(original_table_id)
                table_obj = client.get_table(table_ref)
                table_field_info = get_field_info(table_obj.schema)

                dataset_constraints = constraints_data.get(dataset_id, {})
                enriched_fields = []
                for field in table_field_info:
                    enriched_field = enrich_field_with_constraints(
                        field, original_table_id, dataset_constraints
                    )
                    enriched_fields.append(enriched_field)

                dataset_info["tables"].append({
                    "table_id": normalized_table_id,
                    "fields": enriched_fields
                })

            project_info["datasets"].append(dataset_info)

        return jsonify(project_info), 200

    except Exception as exc:  # pylint: disable=broad-exception-caught
        logger.error("Error processing BigQuery info: %s", str(exc))
        error_response = jsonify({"error": f"An error occurred: {exc}"})
        return error_response, 500


def get_constraints(client: bigquery.Client, dataset_id: str) -> dict:
    """
    Fetch primary key and foreign key constraints for a given dataset
    from INFORMATION_SCHEMA.KEY_COLUMN_USAGE. Returns a dictionary with
    primary keys and foreign keys.

    :param client: A BigQuery client (user-specific or default).
    :type client: bigquery.Client
    :param dataset_id: The dataset ID to fetch constraints for.
    :type dataset_id: str
    :return: Dictionary with 'primary_keys' and 'foreign_keys'.
    :rtype: dict
    """
    key_columns_query = f"""
        SELECT
            TABLE_NAME,
            COLUMN_NAME,
            CONSTRAINT_NAME,
            ORDINAL_POSITION
        FROM
            `{client.project}.{dataset_id}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE`
        WHERE
            CONSTRAINT_NAME IS NOT NULL
    """

    try:
        key_columns_results = client.query(key_columns_query).result()
        constraints = {
            "primary_keys": {},
            "foreign_keys": {},
        }

        for row in key_columns_results:
            table_name = row.TABLE_NAME
            column_name = row.COLUMN_NAME
            constraint_name = row.CONSTRAINT_NAME
            ordinal_position = row.ORDINAL_POSITION

            logging.info(
                "Key Found - Table: %s, Column: %s, Constraint: %s, Position: %s",
                table_name, column_name, constraint_name, ordinal_position
            )

            # Classify primary keys vs foreign keys based on constraint name
            if "PK" in constraint_name.upper():
                if table_name not in constraints["primary_keys"]:
                    constraints["primary_keys"][table_name] = []
                constraints["primary_keys"][table_name].append(column_name)
                logging.info(
                    "Primary Key Identified - Table: %s, Column: %s",
                    table_name, column_name
                )
            elif "FK" in constraint_name.upper():
                if table_name not in constraints["foreign_keys"]:
                    constraints["foreign_keys"][table_name] = []
                constraints["foreign_keys"][table_name].append(column_name)
                logging.info(
                    "Foreign Key Identified - Table: %s, Column: %s",
                    table_name, column_name
                )
            else:
                logging.warning(
                    "Unclassified Key - Constraint: %s, Table: %s, Column: %s",
                    constraint_name, table_name, column_name
                )

        logging.info("Constraints: %s", constraints)

    except Exception as exc:  # pylint: disable=broad-exception-caught
        logging.error("Error fetching constraints: %s", str(exc))
        constraints = {
            "primary_keys": {},
            "foreign_keys": {},
        }

    return constraints


def enrich_field_with_constraints(field: dict, table_id: str, constraints: dict) -> dict:
    """
    Enrich a field dictionary with primary key and foreign key information.

    :param field: The field dictionary containing schema information.
    :type field: dict
    :param table_id: The table name used to lookup constraints.
    :type table_id: str
    :param constraints: Dictionary with primary and foreign keys for the dataset.
    :type constraints: dict
    :return: The updated field dictionary with constraint info.
    :rtype: dict
    """
    column_name = field.get("field_path")
    is_pk = False
    is_fk = False
    referenced_table = None
    referenced_column = None

    # Check if this column is a primary key
    if column_name in constraints.get("primary_keys", {}).get(table_id, []):
        is_pk = True

    # Check if this column is a foreign key
    if column_name in constraints.get("foreign_keys", {}).get(table_id, []):
        is_fk = True
        # Optionally set referenced_table/referenced_column if desired

    field["is_primary_key"] = is_pk
    field["is_foreign_key"] = is_fk
    field["referenced_table"] = referenced_table
    field["referenced_column"] = referenced_column

    return field


def get_field_info(fields, parent_field_name="") -> list:
    """
    Recursively extract field information from a BigQuery table schema,
    handling nested RECORD (STRUCT) types.

    :param fields: The schema fields to process.
    :type fields: Sequence[bigquery.SchemaField]
    :param parent_field_name: The dot-delimited parent field name if nested.
    :type parent_field_name: str
    :return: A list of dictionaries containing field metadata.
    :rtype: list
    """
    field_info_list = []

    for field in fields:
        full_field_name = (
            f"{parent_field_name}.{field.name}" if parent_field_name else field.name
        )

        if field.field_type == "RECORD":
            nested_field_info = get_field_info(field.fields, full_field_name)
            field_info_list.extend(nested_field_info)
        else:
            field_info_list.append({
                "field_path": full_field_name,
                "data_type": field.field_type,
                "description": field.description or None,
                "collation_name": None,  # BigQuery does not support collation
                "rounding_mode": None    # BigQuery does not support rounding
            })

    return field_info_list


def flatten_bq_schema(project_info: dict) -> list:
    """
    Convert a nested 'project_info' dict (containing datasets and tables)
    into a flat list of schema objects.

    Each object in the returned list has this structure:
    {
      "table_catalog": str,
      "table_schema": str,
      "table_name": str,
      "column_name": str,
      "field_path": str,
      "data_type": str,
      "description": str or None,
      "collation_name": "NULL",
      "rounding_mode": None,
      "primary_key": bool,
      "foreign_key": bool
    }

    :param project_info: The project info dictionary with datasets, tables, etc.
    :type project_info: dict
    :return: A flattened list of schema objects.
    :rtype: list
    """
    flattened = []
    top_level_project = project_info.get("project_id", "unknown_project")

    for dataset in project_info.get("datasets", []):
        ds_project_id = dataset.get("project_id", top_level_project)
        ds_id = dataset.get("dataset_id", "")

        for table in dataset.get("tables", []):
            tbl_id = table.get("table_id", "")

            for field in table.get("fields", []):
                col_name = field.get("field_path", "")
                field_path = field.get("field_path", "")

                flattened.append({
                    "table_catalog": ds_project_id,
                    "table_schema": ds_id,
                    "table_name": tbl_id,
                    "column_name": col_name,
                    "field_path": field_path,
                    "data_type": field.get("data_type", ""),
                    "description": field.get("description"),
                    "collation_name": "NULL",  # BigQuery doesn't support collation
                    "rounding_mode": None,
                    "primary_key": field.get("is_primary_key", False),
                    "foreign_key": field.get("is_foreign_key", False),
                })

    return flattened


def human_readable_size(num_bytes: float) -> str:
    """
    Converts bytes into a human-readable string with B, KB, MB, GB, etc.
    Example: 1234 bytes -> '1.21 KB'
    """
    for unit in ["B", "KB", "MB", "GB", "TB", "PB"]:
        if num_bytes < 1024.0:
            return f"{num_bytes:0.2f} {unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:0.2f} PB"  # fallback if huge


def dry_run_query(client: bigquery.Client, sql_query: str) -> dict:
    """
    Executes a dry run of the provided SQL query using the provided
    BigQuery client and returns the total bytes processed (raw + formatted).

    :param client: A BigQuery client (user-specific or default).
    :type client: bigquery.Client
    :param sql_query: The SQL query string to dry-run.
    :type sql_query: str
    :return: Dictionary with total_bytes_processed and formatted_bytes_processed.
    :rtype: dict
    :raises: Exception if any error occurs during the query.
    """
    try:
        job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
        query_job = client.query(sql_query, job_config=job_config)

        # Get the raw bytes processed
        raw_bytes = query_job.total_bytes_processed
        formatted = human_readable_size(raw_bytes)
        logger.info("Dry-run query processed ~%s bytes (%s).", raw_bytes, formatted)

        return {
            "total_bytes_processed": raw_bytes,
            "formatted_bytes_processed": formatted
        }
    except Exception as exc:
        logger.error("Error during dry-run: %s", str(exc))
        raise
