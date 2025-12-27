from flask import Blueprint, request

from src.core.logger import get_logger
from src.sheets.agent_balance import AgentBalanceService
from src.sheets.client import SheetsClient
from src.utils.response import jsend_success, jsend_fail, jsend_error

logger = get_logger(__name__)

bp = Blueprint('agent_balance', __name__, url_prefix='/balance/agent')


@bp.route('/update', methods=['POST'])
def update_agent_balance():
    try:
        data = request.get_json()
        
        merchant = data.get('merchant')
        year = data.get('year')
        month = data.get('month')
        manual_data = data.get('manual_data', [])
        
        if not merchant or not year or not month:
            return jsend_fail({'error': 'merchant, year, month required'}, 400)
        
        sheets_client = SheetsClient()
        service = AgentBalanceService(sheets_client)
        
        if manual_data:
            logger.info(f"Saving {len(manual_data)} agent manual data rows")
            service.save_manual_data(manual_data)
        
        logger.info(f"Loading agent balance for: {merchant} {year}-{month:02d}")
        balance_data = service.get_balance_data(merchant, year, month)
        
        return jsend_success({'data': balance_data})
        
    except Exception as e:
        logger.error(f"Failed to update agent balance: {e}")
        return jsend_error(str(e))
