from flask import Flask
from .routes.bigquery_routes import bigquery_bp
from .routes.queries_routes import queries_bp
from .utils.logging_config import configure_logging

def create_app():
    app = Flask(__name__)
    app.config.from_object('config.Config')

    # Configure logging
    configure_logging(app)

    # Register Blueprints
    app.register_blueprint(bigquery_bp)
    app.register_blueprint(queries_bp)

    return app
