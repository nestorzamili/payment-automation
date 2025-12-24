from flask import Blueprint, request, jsonify

from src.core.logger import get_logger
from src.sheets.agent_ledger import AgentLedgerService
from src.sheets.client import SheetsClient
from src.utils.response import jsend_success, jsend_fail, jsend_error

logger = get_logger(__name__)

bp = Blueprint('agent_ledger', __name__)


@bp.route('/ledger/agent/update', methods=['POST'])
def update_agent_ledger():
    try:
        data = request.get_json()
        
        merchant = data.get('merchant')
        year = data.get('year')
        month = data.get('month')
        manual_data = data.get('manual_data', [])
        
        if not merchant or not year or not month:
            return jsend_fail({'error': 'merchant, year, month required'}, 400)
        
        sheets_client = SheetsClient()
        service = AgentLedgerService(sheets_client)
        
        if manual_data:
            logger.info(f"Saving {len(manual_data)} agent manual data rows by ID")
            service.save_manual_data(manual_data)
        
        logger.info(f"Loading agent ledger for: {merchant} {year}-{month:02d}")
        ledger_data = service.get_ledger(merchant, year, month)
        
        return jsend_success({'data': ledger_data})
        
    except Exception as e:
        logger.error(f"Failed to update agent ledger: {e}")
        return jsend_error(str(e))
