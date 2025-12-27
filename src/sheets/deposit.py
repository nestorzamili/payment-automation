from typing import List, Dict, Any, Set, Optional

from sqlalchemy import and_

from src.core.database import get_session
from src.core.models import DepositFee
from src.core.logger import get_logger
from src.sheets.client import SheetsClient
from src.sheets.transaction import TransactionService
from src.sheets.parameters import ParameterLoader

logger = get_logger(__name__)


class DepositService:
    
    def __init__(
        self, 
        sheets_client: Optional[SheetsClient] = None, 
        add_on_holidays: Optional[Set[str]] = None,
        param_loader: Optional[ParameterLoader] = None
    ):
        self.sheets_client = sheets_client or SheetsClient()
        self.add_on_holidays = add_on_holidays or set()
        self.param_loader = param_loader
        self.tx_service = TransactionService(self.add_on_holidays, self.param_loader)
    
    def generate_from_joined_data(self, joined_data: List[Dict[str, Any]]) -> int:
        if not joined_data:
            return 0
        
        count = self.tx_service.aggregate_transactions()
        
        merchant_months = set()
        for row in joined_data:
            merchant = row['kira_merchant']
            date = row['kira_date']
            year = int(date[:4])
            month = int(date[5:7])
            merchant_months.add((merchant, year, month))
        
        for merchant, year, month in merchant_months:
            self.tx_service.fill_month_dates(merchant, year, month)
            self._init_balances(merchant, year, month)
        
        logger.info(f"Generated deposit data: {count} transaction records")
        return count
    
    def _init_balances(self, merchant: str, year: int, month: int):
        try:
            from src.sheets.merchant_balance import MerchantBalanceService
            from src.sheets.agent_balance import AgentBalanceService
            
            merchant_service = MerchantBalanceService(self.sheets_client)
            merchant_service.init_from_transactions(merchant, year, month)
            
            agent_service = AgentBalanceService(self.sheets_client)
            agent_service.init_from_transactions(merchant, year, month)
            
        except Exception as e:
            logger.error(f"Failed to init balances: {e}")
    
    def _normalize_channel(self, payment_method: str) -> str:
        if not payment_method:
            return 'EWALLET'
        
        pm_upper = payment_method.upper().strip()
        
        if pm_upper in ('FPX', 'FPXC') or 'FPX' in pm_upper:
            return 'FPX'
        
        return 'EWALLET'
    
    def get_deposit_data(self, merchant: str, year: int, month: int) -> List[Dict[str, Any]]:
        return self.tx_service.get_monthly_data(merchant, year, month)
    
    def save_fee_inputs(self, fee_data: List[Dict[str, Any]]) -> int:
        session = get_session()
        count = 0
        
        try:
            for row in fee_data:
                merchant = row.get('merchant')
                date = row.get('transaction_date')
                channel = row.get('channel')
                
                if not all([merchant, date, channel]):
                    continue
                
                fee_type = row.get('fee_type')
                fee_rate = self._to_float(row.get('fee_rate'))
                remarks = row.get('remarks')
                
                is_empty = (fee_type is None and fee_rate is None)
                
                existing = session.query(DepositFee).filter(
                    and_(
                        DepositFee.merchant == merchant,
                        DepositFee.transaction_date == date,
                        DepositFee.channel == channel,
                        DepositFee.fee_type != 'kira_pg'
                    )
                ).first()
                
                if existing:
                    if is_empty:
                        session.delete(existing)
                    else:
                        existing.fee_type = fee_type  # type: ignore
                        existing.fee_rate = fee_rate  # type: ignore
                        existing.remarks = remarks  # type: ignore
                elif not is_empty:
                    new_record = DepositFee(
                        merchant=merchant,
                        transaction_date=date,
                        channel=channel,
                        fee_type=fee_type,
                        fee_rate=fee_rate,
                        remarks=remarks
                    )
                    session.add(new_record)
                
                count += 1
            
            session.commit()
            logger.info(f"Saved {count} fee inputs")
            return count
            
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to save fee inputs: {e}")
            raise
        finally:
            session.close()
    
    def _to_float(self, value):
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            value = value.strip()
            if not value:
                return None
            try:
                return float(value)
            except ValueError:
                return None
        return None
    
    def upload_to_sheet(
        self, 
        merchant: str, 
        year: int, 
        month: int, 
        sheet_name: Optional[str] = None
    ) -> Dict[str, Any]:
        from src.core.loader import load_settings
        
        target_sheet = sheet_name
        if target_sheet is None:
            settings = load_settings()
            target_sheet = settings['google_sheets']['sheets']['deposit']
        
        try:
            data = self.get_deposit_data(merchant, year, month)
            
            if not data:
                return {'success': False, 'error': 'No data to upload'}
            
            columns = [
                'transaction_date',
                'fpx_amount', 'fpx_volume', 'fpx_fee_type', 'fpx_fee_rate', 'fpx_fee_amount', 'fpx_gross', 'fpx_settlement_date',
                'ewallet_amount', 'ewallet_volume', 'ewallet_fee_type', 'ewallet_fee_rate', 'ewallet_fee_amount', 'ewallet_gross', 'ewallet_settlement_date',
                'total_amount', 'total_fees',
                'available_fpx', 'available_ewallet', 'available_total',
                'remarks'
            ]
            
            rows = []
            for record in data:
                row = [record.get(col, '') for col in columns]
                rows.append(row)
            
            self.sheets_client.write_data(target_sheet, rows, start_cell='A7')
            
            month_str = f"{['Jan','Feb','Mar','Apr','May','Jun','Jul','Aug','Sep','Oct','Nov','Dec'][month-1]} {year}"
            self.sheets_client.write_data(target_sheet, [[merchant]], start_cell='B1')
            self.sheets_client.write_data(target_sheet, [[month_str]], start_cell='B2')
            
            logger.info(f"Uploaded {len(rows)} deposit rows to {target_sheet}")
            return {
                'success': True,
                'rows_uploaded': len(rows),
                'sheet_name': sheet_name,
                'merchant': merchant,
                'month': month_str
            }
            
        except Exception as e:
            logger.error(f"Failed to upload to sheet: {e}")
            return {'success': False, 'error': str(e)}
