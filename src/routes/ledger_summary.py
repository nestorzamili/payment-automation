from flask import Blueprint, request

from src.core.logger import get_logger
from src.services.ledger_summary import LedgerSummaryService
from src.utils.response import jsend_success, jsend_fail, jsend_error

logger = get_logger(__name__)

bp = Blueprint('ledger_summary', __name__)

VIEW_TYPE_MAP = {
    'Merchants': 'merchants',
    'Agents': 'agents',
    'Payout Pool Balance': 'payout_pool'
}


@bp.route('/summary', methods=['POST'])
def get_ledger_summary():
    try:
        data = request.get_json() or {}
        
        year = data.get('year')
        view_type = data.get('view_type')
        
        if not year or not view_type:
            return jsend_fail({'message': 'year and view_type are required'}, 400)
        
        normalized_view = VIEW_TYPE_MAP.get(view_type, view_type.lower())
        
        logger.info(f"Loading summary for: {year} - {view_type}")
        
        service = LedgerSummaryService()
        result = service.get_summary(year, normalized_view)
        
        return jsend_success({'data': result})
        
    except Exception as e:
        logger.error(f"Failed to get ledger summary: {e}")
        return jsend_error(str(e))
