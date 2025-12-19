from flask import Blueprint

from src.core import load_accounts
from src.core.loader import PROJECT_ROOT
from src.core.logger import get_logger
from src.processors.m1_parser import M1Parser
from src.processors.axai_parser import AxaiParser
from src.utils import jsend_success, jsend_fail, jsend_error

bp = Blueprint('parse', __name__)
logger = get_logger(__name__)

PARSERS = {
    'm1': M1Parser,
    'axai': AxaiParser,
}


@bp.route('/parse/<platform>/<label>', methods=['POST'])
def parse_files(platform: str, label: str):
    if platform not in PARSERS:
        return jsend_fail(f'Unknown platform: {platform}', 400)
    
    accounts = load_accounts()
    account = next((a for a in accounts if a['label'] == label), None)
    
    if not account:
        return jsend_fail(f'Account not found: {label}', 404)
    
    if account['platform'] != platform:
        return jsend_fail(f'Account {label} is not a {platform} account', 400)
    
    data_dir = PROJECT_ROOT / 'data' / label
    if not data_dir.exists():
        return jsend_fail(f'Data directory not found: {label}', 404)
    
    try:
        parser = PARSERS[platform]()
        result = parser.process_directory(data_dir, label)
        logger.info(f"Parsed {result['total_transactions']} transactions for {label}")
        return jsend_success(result)
    except Exception as e:
        logger.error(f"Error parsing files for {label}: {e}")
        return jsend_error(str(e), 500)
