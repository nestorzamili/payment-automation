import logging

from flask import Flask, request

from src.core import get_logger, load_settings, setup_logger
from src.routes import register_routes, PUBLIC_ENDPOINTS
from src.utils import jsend_fail

setup_logger()
logger = get_logger(__name__)

settings = load_settings()
flask_config = settings['flask']

app = Flask(__name__)
app.json.sort_keys = False

logging.getLogger('werkzeug').setLevel(logging.ERROR)

register_routes(app)


@app.before_request
def check_api_key():
    if request.endpoint in PUBLIC_ENDPOINTS:
        return
    api_key = request.headers.get('X-API-Key')
    if not api_key or api_key != flask_config.get('api_key'):
        return jsend_fail('Invalid or missing API key', 401)


@app.after_request
def log_response(response):
    log_msg = f"{request.remote_addr} - {request.method} {request.path} {response.status_code}"
    
    if response.status_code >= 500:
        logger.error(log_msg)
    elif response.status_code >= 400:
        logger.warning(log_msg)
    else:
        logger.info(log_msg)
    
    return response


if __name__ == '__main__':
    logger.info("Starting server")
    logger.info(f"http://{flask_config['host']}:{flask_config['port']}")
    
    app.run(
        host=flask_config['host'],
        port=flask_config['port'],
        debug=flask_config['debug'],
        use_reloader=False
    )
