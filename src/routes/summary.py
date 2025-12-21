from flask import Blueprint, request

from src.sheets.summary import SummaryService
from src.core.logger import get_logger
from src.utils.response import jsend_success, jsend_fail, jsend_error

logger = get_logger(__name__)

bp = Blueprint('summary', __name__, url_prefix='/summary')


@bp.route('/generate', methods=['POST'])
def generate_summary():
    try:
        data = request.get_json() or {}
        
        from_date = data.get('from_date')
        to_date = data.get('to_date')
        
        if not from_date or not to_date:
            return jsend_fail({'message': 'from_date and to_date are required'}, 400)
        
        logger.info(f"Generating summary: {from_date} to {to_date}")
        
        service = SummaryService()
        df = service.generate_summary(from_date, to_date)
        
        if df.empty:
            return jsend_fail({'message': 'No data found for the specified date range'}, 404)
        
        result = service.upload_to_sheet(df)
        
        if result['success']:
            return jsend_success({
                'message': 'Summary generated and uploaded successfully',
                'rows': result['rows_uploaded'],
                'sheet': result['sheet_name']
            })
        else:
            return jsend_error(f"Upload failed: {result.get('error')}")
            
    except Exception as e:
        logger.error(f"Error generating summary: {e}")
        return jsend_error(str(e))


@bp.route('/preview', methods=['POST'])
def preview_summary():
    try:
        data = request.get_json() or {}
        
        from_date = data.get('from_date')
        to_date = data.get('to_date')
        
        if not from_date or not to_date:
            return jsend_fail({'message': 'from_date and to_date are required'}, 400)
        
        service = SummaryService()
        df = service.generate_summary(from_date, to_date)
        
        if df.empty:
            return jsend_fail({'message': 'No data found for the specified date range'}, 404)
        
        records = df.to_dict(orient='records')
        
        return jsend_success({
            'count': len(records),
            'data': records
        })
        
    except Exception as e:
        logger.error(f"Error previewing summary: {e}")
        return jsend_error(str(e))
