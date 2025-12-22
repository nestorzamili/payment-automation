from flask import Blueprint, request

from src.sheets.deposit import DepositService
from src.core.logger import get_logger
from src.utils.response import jsend_success, jsend_fail, jsend_error

logger = get_logger(__name__)

bp = Blueprint('deposit', __name__, url_prefix='/deposit')


@bp.route('/generate', methods=['POST'])
def generate_deposit():
    try:
        data = request.get_json() or {}
        
        from_date = data.get('from_date')
        to_date = data.get('to_date')
        
        if not from_date or not to_date:
            return jsend_fail({'message': 'from_date and to_date are required'}, 400)
        
        logger.info(f"Generating deposit: {from_date} to {to_date}")
        
        service = DepositService()
        df = service.generate_deposit(from_date, to_date)
        
        if df.empty:
            return jsend_fail({'message': 'No data found for the specified date range'}, 404)
        
        result = service.upload_to_sheet(df)
        
        if result['success']:
            return jsend_success({
                'message': 'Deposit generated and uploaded successfully',
                'rows': result['rows_uploaded'],
                'sheet': result['sheet_name']
            })
        else:
            return jsend_error(f"Upload failed: {result.get('error')}")
            
    except Exception as e:
        logger.error(f"Error generating deposit: {e}")
        return jsend_error(str(e))


@bp.route('/preview', methods=['POST'])
def preview_deposit():
    try:
        data = request.get_json() or {}
        
        from_date = data.get('from_date')
        to_date = data.get('to_date')
        
        if not from_date or not to_date:
            return jsend_fail({'message': 'from_date and to_date are required'}, 400)
        
        service = DepositService()
        df = service.generate_deposit(from_date, to_date)
        
        if df.empty:
            return jsend_fail({'message': 'No data found for the specified date range'}, 404)
        
        records = df.to_dict(orient='records')
        
        return jsend_success({
            'count': len(records),
            'data': records
        })
        
    except Exception as e:
        logger.error(f"Error previewing deposit: {e}")
        return jsend_error(str(e))
