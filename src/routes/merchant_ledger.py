from flask import Blueprint

from src.services.merchant_ledger import MerchantLedgerSheetService, list_merchants, list_periods
from src.core.logger import get_logger
from src.utils.response import jsend_success, jsend_error

logger = get_logger(__name__)

bp = Blueprint('merchant_ledger', __name__, url_prefix='/api/ledger')


@bp.route('/merchant', methods=['POST'])
def sync_merchant_ledger():
    try:
        rows = MerchantLedgerSheetService.sync_sheet()
        
        return jsend_success({
            'message': 'Merchant Ledger synced successfully',
            'rows': rows
        })
            
    except ValueError as e:
        return jsend_error(str(e), 400)
    except Exception as e:
        logger.error(f"Error syncing merchant ledger: {e}")
        return jsend_error(str(e))


@bp.route('/merchants', methods=['GET'])
def get_merchants():
    try:
        merchants = list_merchants()
        return jsend_success({'merchants': merchants})
    except Exception as e:
        logger.error(f"Error listing merchants: {e}")
        return jsend_error(str(e))


@bp.route('/periods', methods=['GET'])
def get_periods():
    try:
        periods = list_periods()
        return jsend_success({'periods': periods})
    except Exception as e:
        logger.error(f"Error listing periods: {e}")
        return jsend_error(str(e))
