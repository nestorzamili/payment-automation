from flask import Blueprint, request

from src.sheets.merchant_ledger import MerchantLedgerService
from src.core.logger import get_logger
from src.utils.response import jsend_success, jsend_fail, jsend_error

logger = get_logger(__name__)

bp = Blueprint('merchant_ledger', __name__)


@bp.route('/merchant-ledger', methods=['POST'])
def update_merchant_ledger():
    try:
        data = request.get_json() or {}
        
        merchant = data.get('merchant')
        year = data.get('year')
        month = data.get('month')
        manual_data = data.get('manual_data', [])
        
        if not merchant or not year or not month:
            return jsend_fail({'message': 'merchant, year, and month are required'}, 400)
        
        service = MerchantLedgerService()
        
        if manual_data:
            logger.info(f"Saving {len(manual_data)} merchant ledger manual inputs")
            service.save_manual_data(manual_data)
        
        ledger_data = service.get_ledger_data(merchant, year, month)
        
        return jsend_success({
            'message': 'Merchant ledger updated successfully',
            'rows': len(ledger_data),
            'data': ledger_data
        })
            
    except Exception as e:
        logger.error(f"Error updating merchant ledger: {e}")
        return jsend_error(str(e))


@bp.route('/merchants', methods=['GET'])
def list_merchants():
    try:
        merchants = MerchantLedgerService.list_merchants()
        return jsend_success({'merchants': merchants})
    except Exception as e:
        logger.error(f"Error listing merchants: {e}")
        return jsend_error(str(e))


@bp.route('/periods', methods=['GET'])
def list_periods():
    try:
        periods = MerchantLedgerService.list_periods()
        return jsend_success({'periods': periods})
    except Exception as e:
        logger.error(f"Error listing periods: {e}")
        return jsend_error(str(e))
