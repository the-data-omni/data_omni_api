from flask import Flask, jsonify, request, make_response
from google.cloud import bigquery
import openai
import os
import re
import logging

app = Flask(__name__)

# Set your OpenAI API key
openai.api_key = os.getenv("OPENAI_API_KEY")

# Configure logging
logging.basicConfig(level=logging.INFO)


# Function to get queries from BigQuery
def get_queries(time_interval="90 day"):
    client = bigquery.Client()
    query = f"""
        SELECT ANY_VALUE(query) AS query_text, query_hash, COUNT(*) AS query_count
        FROM `region-us`.INFORMATION_SCHEMA.JOBS_BY_PROJECT
        WHERE creation_time > TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {time_interval})
        AND state = 'DONE'  -- Only finished jobs
        AND job_type = 'QUERY'  -- Only query jobs
        AND NOT REGEXP_CONTAINS(LOWER(query), r'\\binformation_schema\\b')  -- Exclude INFORMATION_SCHEMA queries
        GROUP BY query_hash
        ORDER BY query_count DESC
    """
    query_job = client.query(query)
    queries_list = []
    for row in query_job.result():
        query_text = row['query_text']
        query_count = row['query_count']
        queries_list.append({
            "query": query_text,
            "count": query_count
        })
    return queries_list

# Function to generate natural language question from SQL query
def generate_natural_language_question(sql_query):
    prompt = f"Convert the following SQL query into a natural language question:\n\nSQL Query:\n{sql_query}\n\nQuestion:"
    try:
        response = openai.Completion.create(
            engine="text-davinci-003",  # Use the appropriate model
            prompt=prompt,
            max_tokens=50,
            n=1,
            stop=None,
            temperature=0.5,
        )
        question = response.choices[0].text.strip()
    except Exception as e:
        question = "Error generating question."
        logging.error(f"An error occurred with OpenAI API: {e}")
    return question

# Function to fetch primary key and foreign key constraints
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

# Route to get queries and counts
@app.route('/queries', methods=['GET'])
def queries():
    time_interval = request.args.get('time_interval', '90 day')
    queries_list = get_queries(time_interval)
    return jsonify(queries_list)

# Route to get natural language questions
@app.route('/questions', methods=['GET'])
def questions():
    time_interval = request.args.get('time_interval', '90 day')
    queries_list = get_queries(time_interval)
    questions_list = []
    for item in queries_list:
        sql_query = item['query']
        count = item['count']
        question = generate_natural_language_question(sql_query)
        questions_list.append({
            "question": question,
            "query": sql_query,
            "count": count
        })
    return jsonify(questions_list)

# Route to get both queries and their natural language questions
@app.route('/queries_and_questions', methods=['GET'])
def queries_and_questions():
    time_interval = request.args.get('time_interval', '90 day')
    queries_list = get_queries(time_interval)
    results = []
    for item in queries_list:
        sql_query = item['query']
        count = item['count']
        question = generate_natural_language_question(sql_query)
        results.append({
            "query": sql_query,
            "question": question,
            "count": count
        })
    return jsonify(results)

# Route to fetch BigQuery schema and constraints
@app.route('/bigquery_info', methods=['GET', 'OPTIONS'])
def bigquery_info():
    if request.method == "OPTIONS":
        # Handle CORS preflight request
        headers = {
            "Access-Control-Allow-Origin": "*",  # Adjust as needed
            "Access-Control-Allow-Methods": "GET, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, Authorization",
            "Access-Control-Max-Age": "3600",
        }
        return ('', 204, headers)

    # Set CORS headers for the main request
    headers = {
        "Access-Control-Allow-Origin": "*",  # Adjust as needed
        "Access-Control-Allow-Headers": "Content-Type, Authorization"
    }

    try:
        # Initialize BigQuery client using default credentials
        client = bigquery.Client()
        logging.info(f"BigQuery Client Project: {client.project}")

        # Get project_id from the client
        project_id = client.project

        # Initialize the result structure
        project_info = {
            "project_id": project_id,
            "datasets": []
        }

        # Retrieve all datasets in the project
        datasets = list(client.list_datasets())
        if not datasets:
            logging.info(f"No datasets found in project {project_id}.")
            return jsonify(project_info), 200, headers

        # Fetch constraints for all datasets
        constraints_data = {}
        for dataset in datasets:
            dataset_id = dataset.dataset_id
            logging.info(f"Fetching constraints for dataset: {dataset_id}")
            constraints = get_constraints(client, dataset_id)
            constraints_data[dataset_id] = constraints

        # Iterate through all datasets to build project_info
        for dataset in datasets:
            dataset_id = dataset.dataset_id
            logging.info(f"Processing dataset: {dataset_id}")
            dataset_info = {
                "dataset_id": dataset_id,
                "tables": []
            }

            # Retrieve tables in the dataset
            tables = client.list_tables(dataset_id)
            if not tables:
                logging.info(f"No tables found in dataset {dataset_id}.")
                project_info["datasets"].append(dataset_info)
                continue

            for table in tables:
                original_table_id = table.table_id
                logging.info(f"Processing table: {original_table_id} in dataset: {dataset_id}")
                table_ref = client.dataset(dataset_id).table(original_table_id)
                table_obj = client.get_table(table_ref)

                # Get the field info (schema) for the table
                table_field_info = get_field_info(table_obj.schema)

                # Enrich each field with constraint information
                constraints = constraints_data.get(dataset_id, {})
                enriched_fields = []
                for field in table_field_info:
                    enriched_field = enrich_field_with_constraints(field, original_table_id, constraints)
                    enriched_fields.append(enriched_field)

                # Add the table information to the dataset
                dataset_info["tables"].append({
                    "table_id": original_table_id,
                    "fields": enriched_fields
                })

            # Add the dataset information to the project
            project_info["datasets"].append(dataset_info)

        # Return the JSON response with the hierarchical structure including constraints
        response = jsonify(project_info)
        response.headers.update(headers)
        return response, 200

    except Exception as e:
        logging.error(f"Error processing project {client.project}: {str(e)}")
        error_response = jsonify({"error": f"An error occurred: {str(e)}"})
        error_response.headers.update(headers)
        return error_response, 500

if __name__ == '__main__':
    app.run(debug=True)
