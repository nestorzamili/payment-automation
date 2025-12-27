from typing import List, Dict, Any, Set
from calendar import monthrange

from sqlalchemy import and_

from src.core.database import get_session
from src.core.models import MerchantBalance, Transaction, DepositFee
from src.core.logger import get_logger
from src.sheets.client import SheetsClient
from src.sheets.transaction import TransactionService

logger = get_logger(__name__)


class MerchantBalanceService:
    
    def __init__(self, sheets_client: SheetsClient = None):
        self.sheets_client = sheets_client or SheetsClient()
    
    def _r(self, value):
        return round(value, 2) if value is not None else None
    
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
    
    def init_from_transactions(self, merchant: str, year: int, month: int):
        session = get_session()
        
        try:
            date_prefix = f"{year}-{month:02d}"
            _, last_day = monthrange(year, month)
            
            existing = session.query(MerchantBalance.transaction_date).filter(
                and_(
                    MerchantBalance.merchant == merchant,
                    MerchantBalance.transaction_date.like(f"{date_prefix}%")
                )
            ).all()
            
            existing_dates = {r[0] for r in existing}
            
            for day in range(1, last_day + 1):
                date_str = f"{year}-{month:02d}-{day:02d}"
                if date_str not in existing_dates:
                    new_record = MerchantBalance(
                        merchant=merchant,
                        transaction_date=date_str
                    )
                    session.add(new_record)
            
            self._recalculate_balances(session, merchant)
            session.commit()
            
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to init merchant balance: {e}")
            raise
        finally:
            session.close()
    
    def get_balance_data(self, merchant: str, year: int, month: int) -> List[Dict[str, Any]]:
        session = get_session()
        tx_service = TransactionService()
        
        try:
            tx_data = tx_service.get_monthly_data(merchant, year, month)
            
            date_prefix = f"{year}-{month:02d}"
            balances = session.query(MerchantBalance).filter(
                and_(
                    MerchantBalance.merchant == merchant,
                    MerchantBalance.transaction_date.like(f"{date_prefix}%")
                )
            ).order_by(MerchantBalance.transaction_date).all()
            
            balance_map = {b.transaction_date: b for b in balances}
            
            result = []
            for tx_row in tx_data:
                date = tx_row['transaction_date']
                balance = balance_map.get(date)
                
                row = {
                    'transaction_date': date,
                    'fpx_amount': tx_row['fpx_amount'],
                    'fpx_fee': tx_row['fpx_fee_amount'],
                    'fpx_gross': tx_row['fpx_gross'],
                    'ewallet_amount': tx_row['ewallet_amount'],
                    'ewallet_fee': tx_row['ewallet_fee_amount'],
                    'ewallet_gross': tx_row['ewallet_gross'],
                    'total_gross': tx_row['fpx_gross'] + tx_row['ewallet_gross'],
                    'total_fee': tx_row['total_fees'],
                    'available_fpx': tx_row['available_fpx'],
                    'available_ewallet': tx_row['available_ewallet'],
                    'available_total': tx_row['available_total'],
                    'settlement_fund': balance.settlement_fund if balance else None,
                    'settlement_charges': balance.settlement_charges if balance else None,
                    'withdrawal_amount': balance.withdrawal_amount if balance else None,
                    'withdrawal_charges': balance.withdrawal_charges if balance else None,
                    'topup_payout_pool': balance.topup_payout_pool if balance else None,
                    'payout_pool_balance': balance.payout_pool_balance if balance else None,
                    'available_balance': balance.available_balance if balance else None,
                    'total_balance': balance.total_balance if balance else None,
                    'remarks': balance.remarks if balance else tx_row.get('remarks')
                }
                
                if balance:
                    row['merchant_balance_id'] = balance.merchant_balance_id
                
                result.append(row)
            
            return result
            
        finally:
            session.close()
    
    def save_manual_data(self, manual_data: List[Dict[str, Any]]) -> int:
        session = get_session()
        count = 0
        
        try:
            valid_ids = []
            for row in manual_data:
                balance_id = row.get('merchant_balance_id')
                if balance_id and isinstance(balance_id, (int, float)):
                    valid_ids.append(int(balance_id))
            
            if not valid_ids:
                return 0
            
            records = session.query(MerchantBalance).filter(
                MerchantBalance.merchant_balance_id.in_(valid_ids)
            ).all()
            
            records_by_id = {r.merchant_balance_id: r for r in records}
            manual_by_id = {int(row['merchant_balance_id']): row for row in manual_data if row.get('merchant_balance_id')}
            
            merchants_to_recalc = set()
            
            for balance_id in valid_ids:
                existing = records_by_id.get(balance_id)
                row = manual_by_id.get(balance_id)
                
                if not existing or not row:
                    continue
                
                val = row.get('settlement_fund')
                if val == 'CLEAR':
                    existing.settlement_fund = None
                    merchants_to_recalc.add(existing.merchant)
                elif val is not None:
                    existing.settlement_fund = self._to_float(val)
                    merchants_to_recalc.add(existing.merchant)
                
                val = row.get('settlement_charges')
                if val == 'CLEAR':
                    existing.settlement_charges = None
                    merchants_to_recalc.add(existing.merchant)
                elif val is not None:
                    existing.settlement_charges = self._to_float(val)
                    merchants_to_recalc.add(existing.merchant)
                
                val = row.get('withdrawal_amount')
                if val == 'CLEAR':
                    existing.withdrawal_amount = None
                    existing.withdrawal_charges = None
                    merchants_to_recalc.add(existing.merchant)
                elif val is not None:
                    withdrawal_amount = self._to_float(val)
                    if withdrawal_amount is not None:
                        existing.withdrawal_amount = withdrawal_amount
                        existing.withdrawal_charges = self._r(withdrawal_amount * 0.01)
                        merchants_to_recalc.add(existing.merchant)
                
                val = row.get('topup_payout_pool')
                if val == 'CLEAR':
                    existing.topup_payout_pool = None
                    merchants_to_recalc.add(existing.merchant)
                elif val is not None:
                    existing.topup_payout_pool = self._to_float(val)
                    merchants_to_recalc.add(existing.merchant)
                
                remarks = row.get('remarks')
                if remarks == 'CLEAR':
                    existing.remarks = None
                elif remarks and isinstance(remarks, str) and remarks.strip():
                    existing.remarks = remarks.strip()
                
                count += 1
            
            for merchant in merchants_to_recalc:
                self._recalculate_balances(session, merchant)
            
            session.commit()
            logger.info(f"Updated {count} merchant balance rows")
            return count
            
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to save manual data: {e}")
            raise
        finally:
            session.close()
    
    def _recalculate_balances(self, session, merchant: str):
        rows = session.query(MerchantBalance).filter(
            MerchantBalance.merchant == merchant
        ).order_by(MerchantBalance.transaction_date).all()
        
        tx_service = TransactionService()
        
        prev_payout_pool = 0
        prev_available = 0
        
        for row in rows:
            has_payout_activity = (
                row.withdrawal_amount is not None 
                or row.topup_payout_pool is not None
                or prev_payout_pool != 0
            )
            
            if has_payout_activity:
                row.payout_pool_balance = self._r(
                    prev_payout_pool
                    - (row.withdrawal_amount or 0)
                    - (row.withdrawal_charges or 0)
                    + (row.topup_payout_pool or 0)
                )
            else:
                row.payout_pool_balance = None
            
            has_available_activity = (
                row.settlement_fund is not None
                or prev_available != 0
            )
            
            if has_available_activity:
                row.available_balance = self._r(
                    prev_available
                    - (row.settlement_fund or 0)
                    - (row.settlement_charges or 0)
                )
            else:
                row.available_balance = None
            
            if row.payout_pool_balance is not None or row.available_balance is not None:
                row.total_balance = self._r(
                    (row.payout_pool_balance or 0) + (row.available_balance or 0)
                )
            else:
                row.total_balance = None
            
            prev_payout_pool = row.payout_pool_balance if row.payout_pool_balance is not None else prev_payout_pool
            prev_available = row.available_balance if row.available_balance is not None else prev_available
    
    def upload_to_sheet(self, merchant: str, year: int, month: int, sheet_name: str = None) -> Dict[str, Any]:
        from src.core.loader import load_settings
        
        if sheet_name is None:
            settings = load_settings()
            sheet_name = settings['google_sheets']['sheets']['merchant_ledger']
        
        try:
            data = self.get_balance_data(merchant, year, month)
            
            if not data:
                return {'success': False, 'error': 'No data to upload'}
            
            columns = [
                'merchant_balance_id', 'transaction_date', 
                'fpx_amount', 'fpx_fee', 'fpx_gross',
                'ewallet_amount', 'ewallet_fee', 'ewallet_gross',
                'total_gross', 'total_fee',
                'available_fpx', 'available_ewallet', 'available_total',
                'settlement_fund', 'settlement_charges',
                'withdrawal_amount', 'withdrawal_charges',
                'topup_payout_pool', 'payout_pool_balance',
                'available_balance', 'total_balance',
                'remarks'
            ]
            
            rows = []
            for record in data:
                row = [record.get(col, '') for col in columns]
                rows.append(row)
            
            self.sheets_client.write_data(sheet_name, rows, start_cell='A5')
            
            logger.info(f"Uploaded {len(rows)} merchant balance rows to {sheet_name}")
            return {
                'success': True,
                'rows_uploaded': len(rows),
                'sheet_name': sheet_name
            }
            
        except Exception as e:
            logger.error(f"Failed to upload to sheet: {e}")
            return {'success': False, 'error': str(e)}
