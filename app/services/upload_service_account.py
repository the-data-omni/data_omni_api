from flask import Blueprint, request, session, jsonify
from google.oauth2 import service_account

import app

upload_bp = Blueprint("upload_bp", __name__)

@upload_bp.route("/upload_service_account", methods=["POST", "OPTIONS"])
def upload_service_account():
    if request.method == "OPTIONS":
        # Let Flask-CORS handle this normally, but you can also do:
        return '', 204

    try:
        service_account_json = request.get_json(force=True)
        # Validate
        creds = service_account.Credentials.from_service_account_info(service_account_json)
        # Store in session
        session["service_account_json"] = service_account_json
        print(creds)
        return jsonify({"message": "Service account uploaded successfully."}), 200
 
    except Exception as e:
        app.logger.error("Exception occurred", exc_info=True)
        return jsonify({"error": "An internal error has occurred."}), 500
