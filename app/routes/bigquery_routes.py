from flask import Blueprint, jsonify, request
from ..services.bigquery_service import get_bigquery_info

bigquery_bp = Blueprint('bigquery_bp', __name__)

@bigquery_bp.route('/bigquery_info', methods=['GET', 'OPTIONS'])
def bigquery_info():
    # Handle CORS preflight request
    if request.method == "OPTIONS":
        headers = {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, Authorization",
            "Access-Control-Max-Age": "3600",
        }
        return ('', 204, headers)

    # Main request handling
    response, status_code = get_bigquery_info()
    return response, status_code
