# bigquery_bp.py
from flask import Blueprint, jsonify, request
from ..services.bigquery_service import get_bigquery_info
from ..services.bigquery_service import flatten_bq_schema  

bigquery_bp = Blueprint('bigquery_bp', __name__)

@bigquery_bp.route('/bigquery_info', methods=['GET', 'OPTIONS'])
def bigquery_info():
    if request.method == "OPTIONS":
        # Handle CORS preflight request
        headers = {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, Authorization",
            "Access-Control-Max-Age": "3600",
        }
        return ('', 204, headers)

    # Main request
    response, status_code = get_bigquery_info()

    # If 200, flatten the JSON
    if status_code == 200:
        # Convert 'response' from a Flask Response to a dict
        data_dict = response.get_json()
        flattened_schema = flatten_bq_schema(data_dict)
        return jsonify({"schema": flattened_schema}), 200
    else:
        return response, status_code

