"""logger service"""
import logging

def configure_logging(app):
    """function configuring logginf"""
    # Basic logging configuration
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)
    app.logger.handlers = logger.handlers
    app.logger.setLevel(logger.level)

logger = logging.getLogger(__name__)
