from flask import Blueprint

from src.core import load_accounts
from src.core.loader import PROJECT_ROOT
from src.core.logger import get_logger
from src.processors.m1_parser import M1Parser
from src.processors.axai_parser import AxaiParser
from src.processors.kira_parser import KiraParser
from src.utils import jsend_success, jsend_error

bp = Blueprint('parse', __name__)
logger = get_logger(__name__)


@bp.route('/parse', methods=['POST'])
def parse_all():
    results = {
        'kira': None,
        'pg': {}
    }
    errors = []
    
    kira_dir = PROJECT_ROOT / 'data' / 'kira'
    if kira_dir.exists():
        try:
            parser = KiraParser()
            result = parser.process_directory(kira_dir)
            results['kira'] = result
            logger.info(f"Kira: parsed {result['total_transactions']} transactions")
        except Exception as e:
            errors.append({'platform': 'kira', 'error': str(e)})
            logger.error(f"Kira parse error: {e}")
    else:
        results['kira'] = {'skipped': True, 'reason': 'no data directory'}
    
    accounts = load_accounts()
    pg_accounts = [a for a in accounts if a['platform'] in ('m1', 'axai', 'fiuu')]
    
    parsers = {
        'm1': M1Parser,
        'axai': AxaiParser,
    }
    
    for account in pg_accounts:
        label = account['label']
        platform = account['platform']
        data_dir = PROJECT_ROOT / 'data' / label
        
        if not data_dir.exists():
            results['pg'][label] = {'skipped': True, 'reason': 'no data directory'}
            continue
        
        if platform not in parsers:
            results['pg'][label] = {'skipped': True, 'reason': f'parser not implemented for {platform}'}
            continue
        
        try:
            parser = parsers[platform]()
            result = parser.process_directory(data_dir, label)
            results['pg'][label] = result
            logger.info(f"{label}: parsed {result['total_transactions']} transactions")
        except Exception as e:
            errors.append({'label': label, 'platform': platform, 'error': str(e)})
            logger.error(f"{label} parse error: {e}")
    
    summary = {
        'kira_transactions': results['kira'].get('total_transactions', 0) if isinstance(results['kira'], dict) and 'total_transactions' in results['kira'] else 0,
        'pg_transactions': sum(r.get('total_transactions', 0) for r in results['pg'].values() if isinstance(r, dict) and 'total_transactions' in r),
        'accounts_processed': len([r for r in results['pg'].values() if isinstance(r, dict) and not r.get('skipped')]),
        'accounts_skipped': len([r for r in results['pg'].values() if isinstance(r, dict) and r.get('skipped')])
    }
    
    response = {
        'summary': summary,
        'details': results
    }
    
    if errors:
        response['errors'] = errors
        return jsend_error('Completed with errors', 500, response)
    
    return jsend_success(response)
