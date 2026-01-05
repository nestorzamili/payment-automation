from flask import Blueprint

from src.services.deposit import DepositSheetService
from src.core.logger import get_logger
from src.utils.response import jsend_success, jsend_error

logger = get_logger(__name__)

bp = Blueprint('deposit', __name__)


@bp.route('/deposit', methods=['POST'])
def sync_deposit():
    try:
        rows = DepositSheetService.sync_sheet()
        
        return jsend_success({
            'message': 'Deposit synced successfully',
            'rows': rows
        })
            
    except ValueError as e:
        return jsend_error(str(e), 400)
    except Exception as e:
        logger.error(f"Error syncing deposit: {e}")
        return jsend_error(str(e))
