# from flask import Flask
# from .routes.bigquery_routes import bigquery_bp
# from .routes.queries_routes import queries_bp
# from .utils.logging_config import configure_logging

# def create_app():
#     app = Flask(__name__)
#     app.config.from_object('config.Config')

#     # Configure logging
#     configure_logging(app)

#     # Register Blueprints
#     app.register_blueprint(bigquery_bp)
#     app.register_blueprint(queries_bp)

#     return app

from flask import Flask
from flask_cors import CORS
from .routes.bigquery_routes import bigquery_bp
from .routes.queries_routes import queries_bp
from .routes.gen_ai_scoring_routes import score_schema_bp
from .utils.logging_config import configure_logging

def create_app():
    app = Flask(__name__)
    app.config.from_object('config.Config')

    # Configure logging
    configure_logging(app)

    # Apply CORS globally with specific configuration
    CORS(app, resources={r"/*": {"origins": ["http://localhost:3000", "http://localhost:4200"]}},
         expose_headers=['Content-Length', 'X-Custom-Header'])

    # Register Blueprints
    app.register_blueprint(bigquery_bp)
    app.register_blueprint(queries_bp)
    app.register_blueprint(score_schema_bp)

    return app

