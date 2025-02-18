from flask import Blueprint, jsonify, request
from flask_cors import cross_origin

from ..services.openai_sql_service import generate_sql_from_question
from ..services.bigquery_service import get_queries, get_bigquery_client_from_session
from ..services.openai_service import generate_natural_language_question


queries_bp = Blueprint('queries_bp', __name__)

@queries_bp.route('/queries', methods=['GET'])
def queries():
    """Route to get queries"""
    time_interval = request.args.get('time_interval', '90 day')
    query_counts = get_queries(time_interval)
    
    queries_list = []
    for q, data in query_counts.items():
        queries_list.append({
            "query": q,
            "count": data["count"],
            "job_type": data["job_type"],
            "statement_type": data["statement_type"],
            "creation_time": data["creation_time"],
            "avg_total_bytes_processed": data["avg_total_bytes_processed"],
            "avg_execution_time": data["avg_execution_time"]
        })

    return jsonify(queries_list)

@queries_bp.route('/questions', methods=['GET'])
def questions():
    """Route to get query corresponding questions"""
    time_interval = request.args.get('time_interval', '90 day')
    query_counts = get_queries(time_interval)
    
    questions_list = []
    for query_text, data in query_counts.items():
        question = generate_natural_language_question(query_text)
        questions_list.append({
            "question": question,
            "query": query_text,
            "count": data["count"],
            "creation_time": data["creation_time"],
            "avg_total_bytes_processed": data["avg_total_bytes_processed"],
            "avg_execution_time": data["avg_execution_time"]
            # add more fields here if you'd like
        })
    return jsonify(questions_list)

@queries_bp.route('/queries_and_questions', methods=['GET'])
def queries_and_questions():
    """Route to get question-query pairs"""
    time_interval = request.args.get('time_interval', '90 day')
    query_counts = get_queries(time_interval)
    
    results = []
    for query_text, data in query_counts.items():
        question = generate_natural_language_question(query_text)
        results.append({
            "query": query_text,
            "question": question,
            "count": data["count"],
            "statement_type": data["statement_type"],
            "creation_time": data["creation_time"],
            "avg_total_bytes_processed": data["avg_total_bytes_processed"],
            "avg_execution_time": data["avg_execution_time"]
        })
    return jsonify(results)


@queries_bp.route('/generate_sql', methods=['POST'])
# @cross_origin()
def generate_sql_endpoint():
    """
    POST endpoint:
    Expects JSON like:
      {
        "userQuestion": "Show me all columns where name is Bob",
        "existingSQL": "SELECT * FROM `project.dataset.table`"
      }
    Returns JSON:
      {
        "sql": "SELECT * FROM `project.dataset.table` WHERE name = 'Bob';"
      }
    """
    data = request.get_json()
    if not data:
        return jsonify({"error": "No JSON body found"}), 400

    user_question = data.get("userQuestion", "")
    existing_sql = data.get("existingSql", "")

    # Generate the new or refined SQL
    sql_result = generate_sql_from_question(user_question, existing_sql)
    return jsonify({"sql": sql_result})