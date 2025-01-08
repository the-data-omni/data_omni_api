
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

        # Build a dict of only the parameters that were actually provided
        score_params = {}
        if 'similarity_threshold' in data:
            score_params['similarity_threshold'] = data['similarity_threshold']
        if 'doc_similarity_meaningful_min' in data:
            score_params['doc_similarity_meaningful_min'] = data['doc_similarity_meaningful_min']
        if 'doc_similarity_placeholder_max' in data:
            score_params['doc_similarity_placeholder_max'] = data['doc_similarity_placeholder_max']
        if 'weights_override' in data:
            score_params['weights_override'] = data['weights_override']

        print("Received schema with length:", len(schema))
        print("Optional parameters (only those passed):", score_params)

        # Now call score_gen_ai with schema plus *only* the present optional parameters:
        scores = score_gen_ai(schema, **score_params)

        return jsonify(scores), 200

    except Exception as e:
        return jsonify({
            'error': 'Internal Server Error',
            'message': str(e)
        }), 500


    except Exception as e:
        return jsonify({
            'error': 'Internal Server Error',
            'message': str(e)
        }), 500
