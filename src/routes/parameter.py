from flask import Blueprint, request

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
        data = request.get_json() or {}
        
        service = ParameterService()
        count = service.save_parameters(data)
        
        return jsend_success({
            'message': f'Saved {count} parameters'
        })
            
    except Exception as e:
        logger.error(f"Error updating parameters: {e}")
        return jsend_error(str(e))
