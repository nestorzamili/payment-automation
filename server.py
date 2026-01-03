import atexit
import logging
import signal
import sys
from typing import Any

from flask import Flask, request

from src.core import get_logger, load_settings, setup_logger
from src.routes import PUBLIC_ENDPOINTS, register_routes
from src.scrapers import cleanup_all_browsers
from src.utils import jsend_fail, get_ssh_tunnel

setup_logger()
logger = get_logger(__name__)

settings = load_settings()
flask_config = settings['flask']

app = Flask(__name__)
app.json.sort_keys = False  # type: ignore[assignment]

logging.getLogger('werkzeug').setLevel(logging.ERROR)

register_routes(app)


def get_client_ip() -> str:
    if request.headers.get('CF-Connecting-IP'):
        return request.headers.get('CF-Connecting-IP', '')
    if request.headers.get('X-Forwarded-For'):
        return request.headers.get('X-Forwarded-For', '').split(',')[0].strip()
    if request.headers.get('X-Real-IP'):
        return request.headers.get('X-Real-IP', '')
    return request.remote_addr or 'unknown'


@app.before_request
def check_api_key():
    if request.endpoint in PUBLIC_ENDPOINTS:
        return
    api_key = request.headers.get('X-API-Key')
    if not api_key or api_key != flask_config.get('api_key'):
        return jsend_fail('Invalid or missing API key', 401)


@app.after_request
def log_response(response):
    client_ip = get_client_ip()
    log_msg = f"{client_ip} - {request.method} {request.path} {response.status_code}"

    if response.status_code >= 500:
        logger.error(log_msg)
    elif response.status_code >= 400:
        logger.warning(log_msg)
    else:
        logger.info(log_msg)

    return response


ssh_tunnel = get_ssh_tunnel()


def signal_handler(signum: int, frame: Any) -> None:
    cleanup_all_browsers()
    ssh_tunnel.stop()
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)

atexit.register(ssh_tunnel.stop)


if __name__ == '__main__':
    logger.info("Starting server")
    logger.info(f"http://{flask_config['host']}:{flask_config['port']}")

    ssh_tunnel.start()

    try:
        app.run(
            host=flask_config['host'],
            port=flask_config['port'],
            debug=flask_config['debug'],
            use_reloader=False
        )
    finally:
        ssh_tunnel.stop()
