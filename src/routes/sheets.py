from flask import Blueprint

from src.sheets.transactions import get_all_joined_transactions
from src.sheets.summary import SummaryService
from src.sheets.deposit import DepositService
from src.sheets.client import SheetsClient
from src.sheets.parameters import ParameterLoader
from src.utils.holiday import load_malaysia_holidays
from src.core.logger import get_logger
from src.utils.response import jsend_success, jsend_error

logger = get_logger(__name__)

bp = Blueprint('sheets', __name__, url_prefix='/sheets')


@bp.route('/update', methods=['POST'])
def update_sheets():
    try:
        logger.info("Updating sheets")
        
        joined_data = get_all_joined_transactions()
        
        if not joined_data:
            return jsend_success({
                'message': 'No data found',
                'summary': '0 rows',
                'deposit': '0 rows'
            })
        
        sheets_client = SheetsClient()
        param_loader = ParameterLoader(sheets_client)
        param_loader.load_all_parameters()
        
        add_on_holidays = param_loader.get_add_on_holidays()
        public_holidays = load_malaysia_holidays()
        
        summary_rows = 0
        deposit_rows = 0
        
        try:
            summary_service = SummaryService(sheets_client, param_loader)
            summary_df = summary_service.generate_summary(
                joined_data, 
                public_holidays, 
                add_on_holidays
            )
            
            if not summary_df.empty:
                summary_service.upload_to_sheet(summary_df)
                summary_rows = len(summary_df)
        except Exception as e:
            logger.error(f"Error generating summary: {e}")
        
        try:
            deposit_service = DepositService(sheets_client, param_loader)
            deposit_df = deposit_service.generate_deposit(
                joined_data,
                public_holidays,
                add_on_holidays
            )
            
            if not deposit_df.empty:
                deposit_service.upload_to_sheet(deposit_df)
                deposit_rows = len(deposit_df)
        except Exception as e:
            logger.error(f"Error generating deposit: {e}")
        
        return jsend_success({
            'message': f'Updated {summary_rows} rows to Summary, {deposit_rows} rows to Deposit',
            'summary': f'{summary_rows} rows',
            'deposit': f'{deposit_rows} rows'
        })
            
    except Exception as e:
        logger.error(f"Error updating sheets: {e}")
        return jsend_error(str(e))
