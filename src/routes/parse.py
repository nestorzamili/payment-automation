import uuid
from threading import Thread

from flask import Blueprint

from src.core import load_accounts
from src.core.loader import PROJECT_ROOT
from src.core.logger import get_logger
from src.parser.m1 import M1Parser
from src.parser.axai import AxaiParser
from src.parser.kira import KiraParser
from src.parser.fiuu import FiuuParser
from src.utils import jsend_success

bp = Blueprint('parse', __name__)
logger = get_logger(__name__)

_parse_running = False


def run_parse_job(run_id: str):
    global _parse_running
    
    kira_dir = PROJECT_ROOT / 'data' / 'kira'
    if kira_dir.exists():
        try:
            parser = KiraParser()
            result = parser.process_directory(kira_dir, run_id=run_id)
            logger.info(f"Kira: parsed {result['total_transactions']} transactions")
        except Exception as e:
            logger.error(f"Kira parse error: {e}")
    
    accounts = load_accounts()
    pg_accounts = [a for a in accounts if a['platform'] in ('m1', 'axai', 'fiuu')]
    
    parsers = {'m1': M1Parser, 'axai': AxaiParser, 'fiuu': FiuuParser}
    
    for account in pg_accounts:
        label = account['label']
        platform = account['platform']
        data_dir = PROJECT_ROOT / 'data' / label
        
        if not data_dir.exists():
            continue
        
        if platform not in parsers:
            continue
        
        try:
            parser = parsers[platform]()
            result = parser.process_directory(data_dir, label, run_id=run_id)
            logger.info(f"{label}: parsed {result['total_transactions']} transactions")
        except Exception as e:
            logger.error(f"{label} parse error: {e}")
    
    _parse_running = False
    logger.info(f"Parse job completed (run_id: {run_id})")


@bp.route('/parse', methods=['POST'])
def parse_all():
    global _parse_running
    
    if _parse_running:
        return jsend_success({'status': 'running', 'message': 'Parse job already running'}, 200)
    
    _parse_running = True
    run_id = str(uuid.uuid4())
    thread = Thread(target=run_parse_job, args=(run_id,), daemon=True)
    thread.start()
    
    logger.info(f"Parse job started (run_id: {run_id})")
    return jsend_success({'run_id': run_id, 'message': 'Parse job started'}, 202)


