import json
from flask import Blueprint, jsonify, request
from flask_cors import CORS
from ..services.scoring_service import score_gen_ai

score_schema_bp = Blueprint('score_schema_bp', __name__)

# Enable CORS for this blueprint
CORS(score_schema_bp)

@score_schema_bp.route('/score_schema', methods=['POST', 'OPTIONS'])
def score_schema():
    # Handle preflight CORS request
    if request.method == "OPTIONS":
        headers = {
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
            "Access-Control-Allow-Headers": "Content-Type, Authorization",
            "Access-Control-Max-Age": "3600",
        }
        return ('', 204, headers)
    
    try:
        data = request.get_json()

        if not data or 'schema' not in data:
            return jsonify({
                'error': 'Bad Request',
                'message': 'No schema provided in the request body'
            }), 400

        schema = data['schema']

        if not isinstance(schema, list):
            return jsonify({
                'error': 'Invalid Format',
                'message': 'The "schema" field must be a list'
            }), 422

        # Debugging - print schema if needed
        print(f"Received schema: {json.dumps(schema, indent=2)}")
        
        # Call the scoring service
        scores = score_gen_ai(schema)

        return jsonify(scores), 200

    except Exception as e:
        return jsonify({
            'error': 'Internal Server Error',
            'message': str(e)
        }), 500
