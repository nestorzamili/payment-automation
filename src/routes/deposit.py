from flask import Blueprint, request

from src.services.deposit import DepositService
from src.core.logger import get_logger
from src.utils.response import jsend_success, jsend_fail, jsend_error

logger = get_logger(__name__)

bp = Blueprint('deposit', __name__)


@bp.route('/deposit', methods=['POST'])
def update_deposit():
    try:
        data = request.get_json() or {}
        
        merchant = data.get('merchant')
        year = data.get('year')
        month = data.get('month')
        fee_data = data.get('fee_data', [])
        
        if not merchant or not year or not month:
            return jsend_fail({'message': 'merchant, year, and month are required'}, 400)
        
        service = DepositService()
        
        if fee_data:
            logger.info(f"Saving {len(fee_data)} deposit fee inputs")
            service.save_fee_inputs(fee_data)
        
        deposit_data = service.get_deposit_data(merchant, year, month)
        
        return jsend_success({
            'message': 'Deposit updated successfully',
            'rows': len(deposit_data),
            'data': deposit_data
        })
            
    except Exception as e:
        logger.error(f"Error updating deposit: {e}")
        return jsend_error(str(e))
