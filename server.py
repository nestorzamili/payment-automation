import atexit
import logging
import os
import signal
import subprocess
import sys
from pathlib import Path
from typing import Any

from flask import Flask, request

from src.core import get_logger, load_settings, setup_logger
from src.routes import PUBLIC_ENDPOINTS, register_routes
from src.utils import jsend_fail

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


class SSHTunnel:
    def __init__(self):
        self._process: Any = None
        self._config: dict = settings.get('ssh_tunnel', {})

    @property
    def is_enabled(self) -> bool:
        return self._config.get('enabled', False)

    @property
    def is_running(self) -> bool:
        return self._process is not None and self._process.poll() is None

    def _get_ssh_key_path(self) -> Path:
        ssh_key = self._config.get('ssh_key')
        if ssh_key:
            return Path(ssh_key).expanduser()
        return Path.home() / '.ssh' / 'id_rsa'

    def _validate_config(self) -> tuple[bool, str]:
        if not self._config.get('remote_host'):
            return False, "remote_host not configured"

        ssh_key_path = self._get_ssh_key_path()
        if not ssh_key_path.exists():
            return False, f"SSH key not found: {ssh_key_path}"

        if os.name != 'nt':
            key_stat = ssh_key_path.stat()
            if key_stat.st_mode & 0o077:
                logger.warning(f"SSH key {ssh_key_path} has insecure permissions")

        return True, ""

    def _build_command(self) -> list[str]:
        ssh_key_path = self._get_ssh_key_path()
        remote_host = self._config['remote_host']
        remote_port = self._config.get('remote_port', 9000)
        local_port = flask_config.get('port', 5000)

        return [
            'ssh',
            '-i', str(ssh_key_path),
            '-N',
            '-T',
            '-R', f'127.0.0.1:{remote_port}:127.0.0.1:{local_port}',
            '-o', 'ExitOnForwardFailure=yes',
            '-o', 'ServerAliveInterval=30',
            '-o', 'ServerAliveCountMax=3',
            '-o', 'StrictHostKeyChecking=accept-new',
            '-o', 'BatchMode=yes',
            '-o', 'ConnectTimeout=10',
            remote_host
        ]

    def start(self) -> bool:
        if not self.is_enabled:
            logger.info("SSH tunnel is disabled")
            return False

        if self.is_running:
            logger.warning("SSH tunnel is already running")
            return True

        is_valid, error_msg = self._validate_config()
        if not is_valid:
            logger.error(f"SSH tunnel config error: {error_msg}")
            return False

        remote_host = self._config['remote_host']
        remote_port = self._config.get('remote_port', 9000)
        local_port = flask_config.get('port', 5000)

        try:
            logger.info(f"Starting SSH tunnel")

            if os.name == 'nt':
                self._process = subprocess.Popen(
                    self._build_command(),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    stdin=subprocess.DEVNULL,
                    creationflags=subprocess.CREATE_NO_WINDOW
                )
            else:
                self._process = subprocess.Popen(
                    self._build_command(),
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    stdin=subprocess.DEVNULL,
                    start_new_session=True
                )

            try:
                self._process.wait(timeout=3)
                stderr = self._process.stderr
                if stderr:
                    error_output = stderr.read().decode().strip()
                    logger.error(f"SSH tunnel failed: {error_output or 'Unknown error'}")
                else:
                    logger.error("SSH tunnel failed: Unknown error")
                self._process = None
                return False
            except subprocess.TimeoutExpired:
                logger.info("SSH tunnel established successfully")
                return True

        except FileNotFoundError:
            logger.error("SSH client not found. Please install OpenSSH.")
            return False
        except Exception as e:
            logger.error(f"Failed to start SSH tunnel: {e}")
            self._process = None
            return False

    def stop(self) -> None:
        if not self.is_running:
            return

        if self._process is None:
            return

        try:
            self._process.terminate()
            self._process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            logger.warning("SSH tunnel didn't terminate gracefully, forcing...")
            if self._process:
                self._process.kill()
                self._process.wait()
        except Exception as e:
            logger.error(f"Error stopping SSH tunnel: {e}")
        finally:
            self._process = None
            logger.info("SSH tunnel stopped")


ssh_tunnel = SSHTunnel()


def signal_handler(signum: int, frame: Any) -> None:
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
