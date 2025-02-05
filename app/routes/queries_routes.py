from flask import Blueprint, jsonify, request
from flask_cors import cross_origin
from ..services.bigquery_service import get_queries, get_bigquery_client_from_session
from ..services.openai_service import generate_natural_language_question



queries_bp = Blueprint('queries_bp', __name__)

@queries_bp.route('/queries', methods=['GET'])
@cross_origin(supports_credentials=True, origins=['http://localhost:4200'])
def queries():
    """Route to get queries"""
    time_interval = request.args.get('time_interval', '90 day')
    query_counts = get_queries(time_interval)
    
    queries_list = []
    for q, c in query_counts.items():
        queries_list.append({
            "query": q,
            "count": c,
        })

    return jsonify(queries_list)

@queries_bp.route('/questions', methods=['GET'])
def questions():
    """route to get query corresponding questions"""
    time_interval = request.args.get('time_interval', '90 day')
    query_counts = get_queries(time_interval)
    questions_list = []
    for query_text, count in query_counts.items():
        question = generate_natural_language_question(query_text)
        questions_list.append({
            "question": question,
            "query": query_text,
            "count": count
        })
    return jsonify(questions_list)

@queries_bp.route('/queries_and_questions', methods=['GET'])
def queries_and_questions():
    """Route to get question query pairs"""
    time_interval = request.args.get('time_interval', '90 day')
    query_counts = get_queries(time_interval)
    results = []
    for query_text, count in query_counts.items():
        question = generate_natural_language_question(query_text)
        results.append({
            "query": query_text,
            "question": question,
            "count": count
        })
    return jsonify(results)
