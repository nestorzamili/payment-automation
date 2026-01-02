from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Any, Optional

from src.core import get_logger, load_settings

logger = get_logger(__name__)


class SSHTunnel:
    def __init__(self, settings: Optional[dict] = None):
        if settings is None:
            settings = load_settings()
        self._settings = settings
        self._process: Any = None
        self._config: dict = settings.get('ssh_tunnel', {})
        self._flask_config: dict = settings.get('flask', {})

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

        if os.name == 'nt':
            self._fix_key_permissions_windows(ssh_key_path)
        else:
            self._fix_key_permissions_unix(ssh_key_path)

        return True, ""

    def _fix_key_permissions_unix(self, key_path: Path) -> None:
        try:
            current_mode = key_path.stat().st_mode
            if current_mode & 0o077:
                key_path.chmod(0o600)
                logger.info(f"Fixed SSH key permissions: {key_path}")
        except Exception as e:
            logger.warning(f"Failed to fix SSH key permissions: {e}")

    def _fix_key_permissions_windows(self, key_path: Path) -> None:
        try:
            key_path_str = str(key_path.resolve())
            username = os.environ.get('USERNAME', '')
            if not username:
                return

            subprocess.run(
                ['icacls', key_path_str, '/reset'],
                capture_output=True,
                creationflags=subprocess.CREATE_NO_WINDOW
            )
            subprocess.run(
                ['icacls', key_path_str, '/inheritance:r'],
                capture_output=True,
                creationflags=subprocess.CREATE_NO_WINDOW
            )
            subprocess.run(
                ['icacls', key_path_str, '/remove:g', 'Authenticated Users'],
                capture_output=True,
                creationflags=subprocess.CREATE_NO_WINDOW
            )
            subprocess.run(
                ['icacls', key_path_str, '/remove:g', 'Users'],
                capture_output=True,
                creationflags=subprocess.CREATE_NO_WINDOW
            )
            subprocess.run(
                ['icacls', key_path_str, '/grant:r', f'{username}:(R)'],
                capture_output=True,
                creationflags=subprocess.CREATE_NO_WINDOW
            )
        except Exception as e:
            logger.warning(f"Failed to fix SSH key permissions: {e}")

    def _build_command(self) -> list[str]:
        ssh_key_path = self._get_ssh_key_path()
        remote_host = self._config['remote_host']
        remote_port = self._config.get('remote_port', 9000)
        local_port = self._flask_config.get('port', 5000)

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

        try:
            logger.info("Starting SSH tunnel")

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


_ssh_tunnel_instance: Optional[SSHTunnel] = None


def get_ssh_tunnel() -> SSHTunnel:
    global _ssh_tunnel_instance
    if _ssh_tunnel_instance is None:
        _ssh_tunnel_instance = SSHTunnel()
    return _ssh_tunnel_instance
