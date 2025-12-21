from pathlib import Path
from typing import Optional
from src.core.logger import get_logger

logger = get_logger(__name__)


class SessionManager:
    
    @staticmethod
    def session_exists(session_path: Path) -> bool:
        exists = session_path.exists() and session_path.is_file()
        
        if exists:
            logger.info(f"Session found: {session_path}")
        else:
            logger.info(f"No session found: {session_path}")
        
        return exists
    
    @staticmethod
    def delete_session(session_path: Path):
        try:
            if session_path.exists():
                session_path.unlink()
                logger.info(f"Session deleted: {session_path}")
            else:
                logger.warning(f"Session file not found for deletion: {session_path}")
        except Exception as e:
            logger.error(f"Failed to delete session {session_path}: {e}")
            raise
    
    @staticmethod
    def get_session_info(session_path: Path) -> Optional[dict]:
        if not session_path.exists():
            return None
        
        try:
            stat = session_path.stat()
            return {
                'path': str(session_path),
                'size_bytes': stat.st_size,
                'modified_time': stat.st_mtime,
                'exists': True
            }
        except Exception as e:
            logger.error(f"Failed to get session info for {session_path}: {e}")
            return None
