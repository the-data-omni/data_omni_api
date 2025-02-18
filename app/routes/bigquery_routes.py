
"""
Blueprint module providing endpoints for BigQuery metadata and schema.
"""

from flask import Blueprint, jsonify, request, session
from flask_cors import cross_origin
from google.api_core.exceptions import BadRequest, GoogleAPICallError


# Import your existing helper functions from bigquery_service.py
# BUT you'll modify them to accept a 'client' param rather than
# creating their own client from foreign-connect.json
from ..services.bigquery_service import (
    get_bigquery_info, 
    flatten_bq_schema,
    dry_run_query,
    get_bigquery_client_from_session
)

bigquery_bp = Blueprint("bigquery_bp", __name__)



@bigquery_bp.route("/bigquery_info", methods=["GET"])
def bigquery_info():
    """
    Handle requests for BigQuery metadata and schema, using the
    user's session-based credentials.
    """

    try:
        current_sa_json = session.get("service_account_json")
        # print("DEBUG: session service_account_json:", current_sa_json)
        # Build a client from the user's session-based service account
        client = get_bigquery_client_from_session()

        # We updated get_bigquery_info(...) so it takes a client param
        response, status_code = get_bigquery_info(client)

        # If 200, flatten the JSON
        if status_code == 200:
            data_dict = response.get_json()
            flattened_schema = flatten_bq_schema(data_dict)
            return jsonify({"schema": flattened_schema}), 200

        return response, status_code
    except Exception as exc:
        print("DEBUG: bigquery_info error:", exc)
        return jsonify({"error": str(exc)}), 500


@bigquery_bp.route("/dry_run", methods=["POST", "OPTIONS"])
def dry_run_route():
    """
    Handle requests for performing a BigQuery dry run using the user's
    session-based service account credentials.
    Expects JSON body with a 'query' key:
      { "query": "SELECT * FROM `project.dataset.table`" }
    """
    if request.method == "OPTIONS":
        # Handle CORS preflight request if necessary
        headers = {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "POST, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, Authorization",
            "Access-Control-Max-Age": "3600",
        }
        return "", 204, headers

    data = request.get_json()
    if not data or "query" not in data:
        return jsonify({"error": "Missing 'query' field in JSON"}), 400

    try:
        client = get_bigquery_client_from_session()
        result = dry_run_query(client, data["query"])
        return jsonify({
            "message": f"Query will process approximately {result['formatted_bytes_processed']}.",
            "raw_bytes_processed": result["total_bytes_processed"],
            "formatted_bytes_processed": result["formatted_bytes_processed"]
        }), 200

    except BadRequest as exc:
        # BigQuery specifically complained about the query syntax or other client error
        # Usually exc.message or str(exc) holds detail like "Unrecognized name: query at [1:8]"
        error_message = str(exc)  # or exc.message
        # Optionally parse out just the part you want:
        # e.g. "Unrecognized name: query at [1:8]"
        return jsonify({"error": "BadRequest", "message": error_message}), 400

    except GoogleAPICallError as exc:
        # This catches other broad issues from the BigQuery client
        return jsonify({"error": "GoogleAPICallError", "message": str(exc)}), 500

    except Exception as exc:
        # Generic fallback
        return jsonify({"error": str(exc)}), 500
