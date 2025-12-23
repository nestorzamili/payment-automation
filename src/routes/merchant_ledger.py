from flask import Blueprint, request

from src.sheets.merchant_ledger import MerchantLedgerService
from src.core.logger import get_logger
from src.utils.response import jsend_success, jsend_fail, jsend_error

logger = get_logger(__name__)

bp = Blueprint('merchant_ledger', __name__, url_prefix='/ledger/merchant')


@bp.route('/update', methods=['POST'])
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
            logger.info(f"Saving {len(manual_data)} manual data rows by ID")
            service.save_manual_data(manual_data)
        
        logger.info(f"Loading ledger for: {merchant} {year}-{month:02d}")
        ledger_data = service.get_ledger(merchant, year, month)
        
        if not ledger_data:
            return jsend_success({
                'message': 'No data found',
                'rows': 0,
                'data': []
            })
        
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
        from src.core.database import get_session
        from src.core.models import KiraTransaction
        from sqlalchemy import distinct
        
        session = get_session()
        
        try:
            merchants = session.query(distinct(KiraTransaction.merchant)).all()
            merchant_list = [m[0] for m in merchants if m[0]]
            
            return jsend_success({'merchants': sorted(merchant_list)})
            
        finally:
            session.close()
            
    except Exception as e:
        logger.error(f"Error listing merchants: {e}")
        return jsend_error(str(e))
