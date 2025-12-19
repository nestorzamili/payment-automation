from flask import Blueprint

from src.core import load_accounts
from src.core.loader import PROJECT_ROOT
from src.core.logger import get_logger
from src.processors.m1_parser import M1Parser
from src.utils import jsend_success, jsend_fail, jsend_error

bp = Blueprint('parse', __name__)
logger = get_logger(__name__)


@bp.route('/parse/<label>', methods=['POST'])
def parse_m1_files(label: str):
    accounts = load_accounts()
    account = next((a for a in accounts if a['label'] == label), None)
    
    if not account:
        return jsend_fail(f'Account not found: {label}', 404)
    
    if account['platform'] != 'm1':
        return jsend_fail(f'Account {label} is not an M1 account', 400)
    
    data_dir = PROJECT_ROOT / 'data' / label
    if not data_dir.exists():
        return jsend_fail(f'Data directory not found: {label}', 404)
    
    try:
        parser = M1Parser()
        result = parser.process_directory(data_dir, label)
        logger.info(f"Parsed {result['total_transactions']} transactions for {label}")
        return jsend_success(result)
    except Exception as e:
        logger.error(f"Error parsing files for {label}: {e}")
        return jsend_error(str(e), 500)
