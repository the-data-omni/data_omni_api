import logging
import re
import os
from flask import jsonify
from google.cloud import bigquery
from ..utils.logging_config import logger

def get_bigquery_client():
    return bigquery.Client()

def get_queries(time_interval="90 day"):
    client = get_bigquery_client()
    query = f"""
        SELECT query, creation_time
        FROM region-us.INFORMATION_SCHEMA.JOBS_BY_PROJECT
        WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {time_interval})
        AND state = 'DONE'
        AND job_type = 'QUERY'
        AND NOT REGEXP_CONTAINS(LOWER(query), r'\\binformation_schema\\b') 
    """
    query_job = client.query(query)
    query_counts = {}
    for row in query_job.result():
        query_text = row.query
        query_counts[query_text] = query_counts.get(query_text, 0) + 1
    return query_counts

def get_bigquery_info():
    try:
        client = get_bigquery_client()
        project_id = client.project
        logger.info(f"BigQuery Client Project: {project_id}")

        project_info = {
            "project_id": project_id,
            "datasets": []
        }

        datasets = list(client.list_datasets())
        if not datasets:
            logger.info(f"No datasets found in project {project_id}.")
            return jsonify(project_info), 200

        constraints_data = {}
        for dataset in datasets:
            dataset_id = dataset.dataset_id
            logger.info(f"Fetching constraints for dataset: {dataset_id}")
            constraints = get_constraints(client, dataset_id)
            constraints_data[dataset_id] = constraints

        # Build project_info
        # Add a compiled regex to detect shard patterns like table_YYYYMMDD
        shard_pattern = re.compile(r'^(.*)_\d{8}$')

        for dataset in datasets:
            dataset_id = dataset.dataset_id
            logger.info(f"Processing dataset: {dataset_id}")
            dataset_info = {
                "dataset_id": dataset_id,
                "tables": []
            }

            tables = client.list_tables(dataset_id)
            if not tables:
                logger.info(f"No tables found in dataset {dataset_id}.")
                project_info["datasets"].append(dataset_info)
                continue

            # We'll keep a dictionary (or set) to track which normalized table we’ve added
            seen_normalized = set()

            for table in tables:
                original_table_id = table.table_id
                logger.info(f"Processing table: {original_table_id} in dataset: {dataset_id}")

                # Check if it matches the shard pattern (e.g. ga_sessions_20170701)
                match = shard_pattern.match(original_table_id)
                if match:
                    # Replace with "ga_sessions_*"
                    normalized_table_id = match.group(1) + '_*'
                else:
                    # If not sharded, keep as is
                    normalized_table_id = original_table_id

                # If we've already handled this normalized table, skip
                if normalized_table_id in seen_normalized:
                    logger.info(f"Skipping table {original_table_id} because we already have {normalized_table_id}")
                    continue

                # Otherwise, mark it as seen
                seen_normalized.add(normalized_table_id)

                # Now retrieve the schema and constraints for this table
                table_ref = client.dataset(dataset_id).table(original_table_id)
                table_obj = client.get_table(table_ref)

                table_field_info = get_field_info(table_obj.schema)

                constraints = constraints_data.get(dataset_id, {})
                enriched_fields = []
                for field in table_field_info:
                    enriched_field = enrich_field_with_constraints(field, original_table_id, constraints)
                    enriched_fields.append(enriched_field)

                dataset_info["tables"].append({
                    "table_id": normalized_table_id,
                    "fields": enriched_fields
                })

            project_info["datasets"].append(dataset_info)

        return jsonify(project_info), 200

    except Exception as e:
        logger.error(f"Error processing BigQuery info: {str(e)}")
        error_response = jsonify({"error": f"An error occurred: {str(e)}"})
        return error_response, 500
# Helper functions for the /bigquery_info route
def get_constraints(client, dataset_id: str):
    """
    Fetch primary key and foreign key constraints for a given dataset from INFORMATION_SCHEMA.KEY_COLUMN_USAGE.
    Returns a dictionary with primary keys and foreign keys.
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
        # Execute the query
        key_columns_results = client.query(key_columns_query).result()

        # Dictionary to store constraints
        constraints = {
            "primary_keys": {},  # Format: {table_name: [column_names]}
            "foreign_keys": {},  # Format: {table_name: [column_names]}
        }

        # Process each row in the results
        for row in key_columns_results:
            table_name = row.TABLE_NAME
            column_name = row.COLUMN_NAME
            constraint_name = row.CONSTRAINT_NAME
            ordinal_position = row.ORDINAL_POSITION

            logging.info(f"Key Found - Table: {table_name}, Column: {column_name}, Constraint: {constraint_name}, Position: {ordinal_position}")

            # Classify primary keys and foreign keys based on the constraint name pattern
            if "PK" in constraint_name.upper():  # Example condition for primary keys
                if table_name not in constraints["primary_keys"]:
                    constraints["primary_keys"][table_name] = []
                constraints["primary_keys"][table_name].append(column_name)
                logging.info(f"Primary Key Identified - Table: {table_name}, Column: {column_name}")
            elif "FK" in constraint_name.upper():  # Example condition for foreign keys
                if table_name not in constraints["foreign_keys"]:
                    constraints["foreign_keys"][table_name] = []
                constraints["foreign_keys"][table_name].append(column_name)
                logging.info(f"Foreign Key Identified - Table: {table_name}, Column: {column_name}")
            else:
                logging.warning(f"Unclassified Key - Constraint: {constraint_name}, Table: {table_name}, Column: {column_name}")

        # Output constraints dictionary for further processing
        logging.info(f"Constraints: {constraints}")

    except Exception as e:
        logging.error(f"An error occurred while fetching constraints: {e}")
        constraints = {
            "primary_keys": {},
            "foreign_keys": {},
        }

    return constraints

def enrich_field_with_constraints(field: dict, table_id: str, constraints: dict) -> dict:
    """
    Enrich a field dictionary with primary key and foreign key information.
    """
    column_name = field.get("field_path")
    is_pk = False
    is_fk = False
    referenced_table = None
    referenced_column = None

    # Check for Primary Key
    if column_name in constraints.get("primary_keys", {}).get(table_id, []):
        is_pk = True

    # Check for Foreign Key
    if column_name in constraints.get("foreign_keys", {}).get(table_id, []):
        is_fk = True
        # Optionally, you can set referenced_table and referenced_column if available

    # Add constraint information to the field
    field["is_primary_key"] = is_pk
    field["is_foreign_key"] = is_fk
    field["referenced_table"] = referenced_table
    field["referenced_column"] = referenced_column

    return field

def get_field_info(fields, parent_field_name=""):
    """
    Recursively extract field information from BigQuery schema, handling nested RECORD types.
    """
    field_info_list = []

    for field in fields:
        # Generate the full field path
        full_field_name = f"{parent_field_name}.{field.name}" if parent_field_name else field.name

        if field.field_type == "RECORD":  # Handle nested fields (STRUCT/RECORD)
            # Recursively process nested fields
            nested_field_info = get_field_info(field.fields, full_field_name)
            field_info_list.extend(nested_field_info)
        else:
            # If it's not a RECORD, capture the field details
            field_info = {
                'field_path': full_field_name,
                'data_type': field.field_type,
                'description': field.description or None,
                'collation_name': None,  # BigQuery does not support collation
                'rounding_mode': None    # BigQuery does not support rounding_mode
            }
            field_info_list.append(field_info)

    return field_info_list

def flatten_bq_schema(project_info: dict) -> list:
    """
    Convert a nested 'project_info' dict into a flat list of schema objects.
    Each object looks like:
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
    """
    flattened = []
    
    # The top-level project in "project_info" 
    # (assuming "project_id" is at the top-level)
    top_level_project = project_info.get("project_id", "unknown_project")

    # Safely iterate over datasets
    for dataset in project_info.get("datasets", []):
        # If each dataset has its own "project_id" instead, you can read it here:
        ds_project_id = dataset.get("project_id", top_level_project)
        ds_id = dataset.get("dataset_id", "")

        for table in dataset.get("tables", []):
            tbl_id = table.get("table_id", "")
            
            # Now flatten each field
            for field in table.get("fields", []):
                # "column_name" can be separate or the same as "field_path"
                col_name = field.get("field_path", "")
                field_path = field.get("field_path", "")

                flattened.append({
                    "table_catalog": ds_project_id,  # or top_level_project
                    "table_schema": ds_id,
                    "table_name": tbl_id,
                    "column_name": col_name,         # If you want a separate name, do so here
                    "field_path": field_path,
                    "data_type": field.get("data_type", ""),
                    "description": field.get("description"),
                    "collation_name": "NULL",        # Hard-coded or adjust if needed
                    "rounding_mode": None,           # Hard-coded or adjust if needed
                    "primary_key": field.get("is_primary_key", False),
                    "foreign_key": field.get("is_foreign_key", False),
                })
    return flattened