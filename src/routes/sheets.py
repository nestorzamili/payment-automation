from flask import Blueprint, request

from src.sheets.summary import SummaryService
from src.sheets.deposit import DepositService
from src.core.logger import get_logger
from src.utils.response import jsend_success, jsend_fail, jsend_error

logger = get_logger(__name__)

bp = Blueprint('sheets', __name__, url_prefix='/sheets')


@bp.route('/update', methods=['POST'])
def update_sheets():
    try:
        data = request.get_json() or {}
        
        from_date = data.get('from_date')
        to_date = data.get('to_date')
        
        if not from_date or not to_date:
            return jsend_fail({'message': 'from_date and to_date are required'}, 400)
        
        logger.info(f"Updating sheets: {from_date} to {to_date}")
        
        results = {
            'summary': None,
            'deposit': None
        }
        errors = []
        
        try:
            summary_service = SummaryService()
            summary_df = summary_service.generate_summary(from_date, to_date)
            
            if summary_df.empty:
                results['summary'] = {'status': 'skipped', 'message': 'No data found'}
            else:
                summary_result = summary_service.upload_to_sheet(summary_df)
                if summary_result['success']:
                    results['summary'] = {
                        'status': 'success',
                        'rows': summary_result['rows_uploaded'],
                        'sheet': summary_result['sheet_name']
                    }
                else:
                    errors.append({'type': 'summary', 'error': summary_result.get('error')})
                    results['summary'] = {'status': 'error', 'error': summary_result.get('error')}
        except Exception as e:
            logger.error(f"Error generating summary: {e}")
            errors.append({'type': 'summary', 'error': str(e)})
            results['summary'] = {'status': 'error', 'error': str(e)}
        
        try:
            deposit_service = DepositService()
            deposit_df = deposit_service.generate_deposit(from_date, to_date)
            
            if deposit_df.empty:
                results['deposit'] = {'status': 'skipped', 'message': 'No data found'}
            else:
                deposit_result = deposit_service.upload_to_sheet(deposit_df)
                if deposit_result['success']:
                    results['deposit'] = {
                        'status': 'success',
                        'rows': deposit_result['rows_uploaded'],
                        'sheet': deposit_result['sheet_name']
                    }
                else:
                    errors.append({'type': 'deposit', 'error': deposit_result.get('error')})
                    results['deposit'] = {'status': 'error', 'error': deposit_result.get('error')}
        except Exception as e:
            logger.error(f"Error generating deposit: {e}")
            errors.append({'type': 'deposit', 'error': str(e)})
            results['deposit'] = {'status': 'error', 'error': str(e)}
        
        if errors:
            return jsend_error(
                message='Update completed with errors',
                http_code=500,
                data=results
            )
        
        if (results['summary'] and results['summary'].get('status') == 'skipped' and
            results['deposit'] and results['deposit'].get('status') == 'skipped'):
            return jsend_fail({'message': 'No data found for the specified date range'}, 404)
        
        return jsend_success({
            'message': 'Data updated successfully',
            'from_date': from_date,
            'to_date': to_date,
            'summary': results['summary'],
            'deposit': results['deposit']
        })
            
    except Exception as e:
        logger.error(f"Error updating sheets: {e}")
        return jsend_error(str(e))
