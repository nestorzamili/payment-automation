from flask import Blueprint

from src.services.kira_pg import KiraPGSheetService
from src.core.logger import get_logger
from src.utils.response import jsend_success, jsend_error

logger = get_logger(__name__)

bp = Blueprint('kira_pg', __name__)


@bp.route('/kira-pg', methods=['POST'])
def sync_kira_pg():
    try:
        rows = KiraPGSheetService.sync_sheet()
        
        return jsend_success({
            'message': 'Kira PG synced successfully',
            'rows': rows
        })
            
    except ValueError as e:
        return jsend_error(str(e), 400)
    except Exception as e:
        logger.error(f"Error syncing Kira PG: {e}")
        return jsend_error(str(e))
