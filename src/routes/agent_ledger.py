from flask import Blueprint, request

from src.core.logger import get_logger
from src.sheets.agent_ledger import AgentLedgerService
from src.utils.response import jsend_success, jsend_fail, jsend_error

logger = get_logger(__name__)

bp = Blueprint('agent_ledger', __name__)


@bp.route('/agent-ledger', methods=['POST'])
def update_agent_ledger():
    try:
        data = request.get_json() or {}
        
        merchant = data.get('merchant')
        year = data.get('year')
        month = data.get('month')
        manual_data = data.get('manual_data', [])
        
        if not merchant or not year or not month:
            return jsend_fail({'message': 'merchant, year, and month are required'}, 400)
        
        service = AgentLedgerService()
        
        if manual_data:
            logger.info(f"Saving {len(manual_data)} agent ledger manual inputs")
            service.save_manual_data(manual_data)
        
        logger.info(f"Loading agent ledger for: {merchant} {year}-{month:02d}")
        ledger_data = service.get_ledger_data(merchant, year, month)
        
        return jsend_success({
            'message': 'Agent ledger updated successfully',
            'rows': len(ledger_data),
            'data': ledger_data
        })
        
    except Exception as e:
        logger.error(f"Error updating agent ledger: {e}")
        return jsend_error(str(e))
