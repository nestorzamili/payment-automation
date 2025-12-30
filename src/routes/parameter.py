from flask import Blueprint

from src.services.parameters import ParameterService
from src.core.logger import get_logger
from src.utils.response import jsend_success, jsend_error

logger = get_logger(__name__)

bp = Blueprint('parameter', __name__)


@bp.route('/parameter', methods=['GET'])
def get_parameters():
    try:
        service = ParameterService()
        data = service.get_all_parameters()
        
        return jsend_success(data)
            
    except Exception as e:
        logger.error(f"Error getting parameters: {e}")
        return jsend_error(str(e))


@bp.route('/parameter', methods=['POST'])
def update_parameters():
    try:
        count = ParameterService.sync_from_sheet()
        
        return jsend_success({
            'message': f'Synced {count} parameters from sheet'
        })
            
    except Exception as e:
        logger.error(f"Error syncing parameters: {e}")
        return jsend_error(str(e))
