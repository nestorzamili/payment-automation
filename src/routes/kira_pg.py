from flask import Blueprint, request

from src.sheets.kira_pg import KiraPGService
from src.sheets.transactions import get_all_joined_transactions
from src.sheets.client import SheetsClient
from src.sheets.parameters import ParameterLoader
from src.utils.holiday import load_malaysia_holidays
from src.core.logger import get_logger
from src.utils.response import jsend_success, jsend_fail, jsend_error

logger = get_logger(__name__)

bp = Blueprint('kira_pg', __name__, url_prefix='/kira-pg')


@bp.route('/update', methods=['POST'])
def update_kira_pg():
    try:
        data = request.get_json() or {}
        manual_data = data.get('manual_data', [])
        
        sheets_client = SheetsClient()
        param_loader = ParameterLoader(sheets_client)
        param_loader.load_all_parameters()
        
        service = KiraPGService(sheets_client, param_loader)
        
        if manual_data:
            logger.info(f"Saving {len(manual_data)} Kira PG manual inputs")
            service.save_manual_data(manual_data)
        
        joined_data = get_all_joined_transactions()
        
        if not joined_data:
            return jsend_success({
                'message': 'No transaction data found',
                'rows': 0,
                'data': []
            })
        
        add_on_holidays = param_loader.get_add_on_holidays()
        public_holidays = load_malaysia_holidays()
        
        kira_pg_data = service.get_kira_pg_data(joined_data, public_holidays, add_on_holidays)
        
        return jsend_success({
            'message': f'Kira PG updated successfully',
            'rows': len(kira_pg_data),
            'data': kira_pg_data
        })
            
    except Exception as e:
        logger.error(f"Error updating Kira PG: {e}")
        return jsend_error(str(e))
