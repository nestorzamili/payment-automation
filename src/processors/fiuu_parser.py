from pathlib import Path
from typing import List

from src.core.logger import get_logger

logger = get_logger(__name__)


class FiuuParser:
    
    def parse_file(self, file_path: Path, account_label: str) -> List[dict]:
        logger.info(f"FiuuParser: parse_file not implemented yet for {file_path}")
        return []
    
    def save_transactions(self, transactions: List[dict]) -> int:
        return 0
    
    def process_directory(self, directory: Path, account_label: str) -> dict:
        logger.info(f"FiuuParser: process_directory not implemented yet for {directory}")
        return {
            'account_label': account_label,
            'files_processed': 0,
            'total_transactions': 0
        }
