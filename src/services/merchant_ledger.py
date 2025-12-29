from typing import List, Dict, Any, Optional
from calendar import monthrange

from sqlalchemy import and_

from src.core.database import get_session
from src.core.models import MerchantLedger
from src.core.logger import get_logger
from src.services.client import SheetsClient

logger = get_logger(__name__)


class MerchantLedgerService:
    
    def __init__(self, sheets_client: Optional[SheetsClient] = None):
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
            
            existing = session.query(MerchantLedger.transaction_date).filter(
                and_(
                    MerchantLedger.merchant == merchant,
                    MerchantLedger.transaction_date.like(f"{date_prefix}%")
                )
            ).all()
            
            existing_dates = {r[0] for r in existing}
            
            for day in range(1, last_day + 1):
                date_str = f"{year}-{month:02d}-{day:02d}"
                if date_str not in existing_dates:
                    new_record = MerchantLedger(
                        merchant=merchant,
                        transaction_date=date_str
                    )
                    session.add(new_record)
            
            session.commit()
            
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to init merchant ledger: {e}")
            raise
        finally:
            session.close()
    
    def get_ledger_data(self, merchant: str, year: int, month: int) -> List[Dict[str, Any]]:
        session = get_session()
        
        try:
            from src.core.models import Deposit
            
            date_prefix = f"{year}-{month:02d}"
            
            deposits = session.query(Deposit).filter(
                and_(
                    Deposit.merchant == merchant,
                    Deposit.transaction_date.like(f"{date_prefix}%")
                )
            ).order_by(Deposit.transaction_date).all()
            
            deposit_map = {d.transaction_date: d for d in deposits}
            
            ledgers = session.query(MerchantLedger).filter(
                and_(
                    MerchantLedger.merchant == merchant,
                    MerchantLedger.transaction_date.like(f"{date_prefix}%")
                )
            ).order_by(MerchantLedger.transaction_date).all()
            
            for ledger in ledgers:
                deposit = deposit_map.get(ledger.transaction_date)
                if deposit:
                    ledger.available_fpx = deposit.available_fpx
                    ledger.available_ewallet = deposit.available_ewallet
                    ledger.available_total = deposit.available_total
            
            self._recalculate_balances(session, merchant)
            session.commit()
            
            ledger_map = {lg.transaction_date: lg for lg in ledgers}
            
            result = []
            for deposit in deposits:
                date = deposit.transaction_date
                ledger = ledger_map.get(date)
                
                fpx_gross = self._r((deposit.fpx_amount or 0) - (deposit.fpx_fee_amount or 0))
                ewallet_gross = self._r((deposit.ewallet_amount or 0) - (deposit.ewallet_fee_amount or 0))
                
                row = {
                    'transaction_date': date,
                    'fpx_amount': deposit.fpx_amount,
                    'fpx_fee': deposit.fpx_fee_amount,
                    'fpx_gross': fpx_gross,
                    'ewallet_amount': deposit.ewallet_amount,
                    'ewallet_fee': deposit.ewallet_fee_amount,
                    'ewallet_gross': ewallet_gross,
                    'total_gross': self._r((fpx_gross or 0) + (ewallet_gross or 0)),
                    'total_fee': deposit.total_fees,
                    'available_fpx': deposit.available_fpx,
                    'available_ewallet': deposit.available_ewallet,
                    'available_total': deposit.available_total,
                    'settlement_fund': ledger.settlement_fund if ledger else None,
                    'settlement_charges': ledger.settlement_charges if ledger else None,
                    'withdrawal_amount': ledger.withdrawal_amount if ledger else None,
                    'withdrawal_rate': ledger.withdrawal_rate if ledger else None,
                    'withdrawal_charges': ledger.withdrawal_charges if ledger else None,
                    'topup_payout_pool': ledger.topup_payout_pool if ledger else None,
                    'payout_pool_balance': ledger.payout_pool_balance if ledger else None,
                    'available_balance': ledger.available_balance if ledger else None,
                    'total_balance': ledger.total_balance if ledger else None,
                    'updated_at': ledger.updated_at if ledger else None,
                    'remarks': ledger.remarks if ledger else deposit.remarks
                }
                
                if ledger:
                    row['id'] = ledger.id
                
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
                ledger_id = row.get('id')
                if ledger_id and isinstance(ledger_id, (int, float)):
                    valid_ids.append(int(ledger_id))
            
            if not valid_ids:
                return 0
            
            records = session.query(MerchantLedger).filter(
                MerchantLedger.id.in_(valid_ids)
            ).all()
            
            records_by_id = {r.id: r for r in records}
            manual_by_id = {int(row['id']): row for row in manual_data if row.get('id')}
            
            for ledger_id in valid_ids:
                existing = records_by_id.get(ledger_id)
                row = manual_by_id.get(ledger_id)
                
                if not existing or not row:
                    continue
                
                val = row.get('settlement_fund')
                if val == 'CLEAR':
                    existing.settlement_fund = None
                elif val is not None:
                    existing.settlement_fund = self._to_float(val)
                
                val = row.get('settlement_charges')
                if val == 'CLEAR':
                    existing.settlement_charges = None
                elif val is not None:
                    existing.settlement_charges = self._to_float(val)
                
                val = row.get('withdrawal_amount')
                if val == 'CLEAR':
                    existing.withdrawal_amount = None
                    existing.withdrawal_rate = None
                    existing.withdrawal_charges = None
                elif val is not None:
                    existing.withdrawal_amount = self._to_float(val)
                
                val = row.get('withdrawal_rate')
                if val == 'CLEAR':
                    existing.withdrawal_rate = None
                    existing.withdrawal_charges = None
                elif val is not None:
                    existing.withdrawal_rate = self._to_float(val)
                
                if existing.withdrawal_amount and existing.withdrawal_rate:
                    existing.withdrawal_charges = self._r(
                        existing.withdrawal_amount * existing.withdrawal_rate / 100
                    )
                else:
                    existing.withdrawal_charges = None
                
                val = row.get('topup_payout_pool')
                if val == 'CLEAR':
                    existing.topup_payout_pool = None
                elif val is not None:
                    existing.topup_payout_pool = self._to_float(val)
                
                remarks = row.get('remarks')
                if remarks == 'CLEAR':
                    existing.remarks = None
                elif remarks and isinstance(remarks, str) and remarks.strip():
                    existing.remarks = remarks.strip()
                
                count += 1
            
            session.commit()
            logger.info(f"Updated {count} merchant ledger rows")
            return count
            
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to save manual data: {e}")
            raise
        finally:
            session.close()
    
    def _recalculate_balances(self, session, merchant: str):
        rows = session.query(MerchantLedger).filter(
            MerchantLedger.merchant == merchant
        ).order_by(MerchantLedger.transaction_date).all()
        
        prev_topup_payout_pool = 0
        prev_available_balance = 0
        
        for row in rows:
            available_total = row.available_total or 0
            
            has_payout_activity = (
                row.withdrawal_amount is not None 
                or row.topup_payout_pool is not None
                or prev_topup_payout_pool != 0
            )
            
            if has_payout_activity:
                row.payout_pool_balance = self._r(
                    prev_topup_payout_pool
                    - (row.withdrawal_amount or 0)
                    - (row.withdrawal_charges or 0)
                    + (row.topup_payout_pool or 0)
                )
            else:
                row.payout_pool_balance = None
            
            has_available_activity = (
                row.settlement_fund is not None
                or available_total > 0
                or prev_available_balance != 0
            )
            
            if has_available_activity:
                row.available_balance = self._r(
                    prev_available_balance
                    + available_total
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
            
            prev_topup_payout_pool = row.payout_pool_balance if row.payout_pool_balance is not None else prev_topup_payout_pool
            prev_available_balance = row.available_balance if row.available_balance is not None else prev_available_balance
    
    def upload_to_sheet(self, merchant: str, year: int, month: int, sheet_name: str) -> Dict[str, Any]:
        try:
            data = self.get_ledger_data(merchant, year, month)
            
            if not data:
                return {'success': False, 'error': 'No data to upload'}
            
            columns = [
                'id', 'transaction_date', 
                'fpx_amount', 'fpx_fee', 'fpx_gross',
                'ewallet_amount', 'ewallet_fee', 'ewallet_gross',
                'total_gross', 'total_fee',
                'available_fpx', 'available_ewallet', 'available_total',
                'settlement_fund', 'settlement_charges',
                'withdrawal_amount', 'withdrawal_rate', 'withdrawal_charges',
                'topup_payout_pool', 'payout_pool_balance',
                'available_balance', 'total_balance',
                'remarks'
            ]
            
            rows = []
            for record in data:
                row = [record.get(col, '') for col in columns]
                rows.append(row)
            
            self.sheets_client.write_data(sheet_name, rows, start_cell='A5')
            
            logger.info(f"Uploaded {len(rows)} merchant ledger rows to {sheet_name}")
            return {
                'success': True,
                'rows_uploaded': len(rows),
                'sheet_name': sheet_name
            }
            
        except Exception as e:
            logger.error(f"Failed to upload to sheet: {e}")
            return {'success': False, 'error': str(e)}
    
    @staticmethod
    def list_merchants() -> List[str]:
        from src.core.models import KiraTransaction
        session = get_session()
        try:
            merchants = session.query(KiraTransaction.merchant).distinct().all()
            return sorted([m[0] for m in merchants if m[0]])
        finally:
            session.close()
    
    @staticmethod
    def list_periods() -> List[str]:
        from sqlalchemy import func
        from src.core.models import KiraTransaction
        
        session = get_session()
        try:
            results = session.query(
                func.substr(KiraTransaction.transaction_date, 1, 7).label('ym')
            ).distinct().all()
            
            months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                      'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
            
            periods = []
            for r in results:
                if r.ym:
                    year = r.ym[:4]
                    month_num = int(r.ym[5:7])
                    periods.append(f"{months[month_num-1]} {year}")
            
            periods.sort(key=lambda x: (x.split()[1], months.index(x.split()[0])))
            return periods
        finally:
            session.close()

