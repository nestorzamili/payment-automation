from flask import Blueprint

from src.services.agent_ledger import AgentLedgerSheetService
from src.core.logger import get_logger
from src.utils.response import jsend_success, jsend_error

logger = get_logger(__name__)

bp = Blueprint('agent_ledger', __name__, url_prefix='/api/ledger')


@bp.route('/agent', methods=['POST'])
def sync_agent_ledger():
    try:
        rows = AgentLedgerSheetService.sync_sheet()
        
        return jsend_success({
            'message': 'Agent Ledger synced successfully',
            'rows': rows
        })
            
    except ValueError as e:
        return jsend_error(str(e), 400)
    except Exception as e:
        logger.error(f"Error syncing agent ledger: {e}")
        return jsend_error(str(e))
