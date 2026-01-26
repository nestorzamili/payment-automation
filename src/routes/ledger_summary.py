from flask import Blueprint

from src.core.logger import get_logger
from src.services.ledger_summary import SummarySheetService
from src.utils.response import jsend_success, jsend_error

logger = get_logger(__name__)

bp = Blueprint('ledger_summary', __name__, url_prefix='/api/sheets')


@bp.route('/summary', methods=['POST'])
def sync_summary():
    try:
        rows = SummarySheetService.sync_sheet()
        
        return jsend_success({
            'message': 'Summary synced successfully',
            'rows': rows
        })
            
    except ValueError as e:
        return jsend_error(str(e), 400)
    except Exception as e:
        logger.error(f"Error syncing summary: {e}")
        return jsend_error(str(e))
