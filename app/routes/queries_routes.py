from flask import Blueprint, jsonify, request
from ..services.bigquery_service import get_queries, parse_fields_from_query
from ..services.openai_service import generate_natural_language_question


queries_bp = Blueprint('queries_bp', __name__)

# @queries_bp.route('/queries', methods=['GET'])
# def queries():
#     time_interval = request.args.get('time_interval', '90 day')
#     query_counts = get_queries(time_interval)
#     queries_list = [{"query": q, "count": c} for q, c in query_counts.items()]
#     return jsonify(queries_list)

@queries_bp.route('/queries', methods=['GET'])
def queries():
    time_interval = request.args.get('time_interval', '90 day')
    query_counts = get_queries(time_interval)
    
    queries_list = []
    for q, c in query_counts.items():
        fields = parse_fields_from_query(q)
        queries_list.append({
            "query": q,
            "count": c,
            "fields": fields  # include parsed fields
        })

    return jsonify(queries_list)

@queries_bp.route('/questions', methods=['GET'])
def questions():
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
