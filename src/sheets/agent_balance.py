from typing import List, Dict, Any, Set
from calendar import monthrange

from sqlalchemy import and_

from src.core.database import get_session
from src.core.models import AgentBalance, Transaction
from src.core.logger import get_logger
from src.sheets.client import SheetsClient
from src.sheets.transaction import TransactionService

logger = get_logger(__name__)


class AgentBalanceService:
    
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
            
            existing = session.query(AgentBalance.transaction_date).filter(
                and_(
                    AgentBalance.merchant == merchant,
                    AgentBalance.transaction_date.like(f"{date_prefix}%")
                )
            ).all()
            
            existing_dates = {r[0] for r in existing}
            
            for day in range(1, last_day + 1):
                date_str = f"{year}-{month:02d}-{day:02d}"
                if date_str not in existing_dates:
                    new_record = AgentBalance(
                        merchant=merchant,
                        transaction_date=date_str
                    )
                    session.add(new_record)
            
            self._recalculate_balances(session, merchant)
            session.commit()
            
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to init agent balance: {e}")
            raise
        finally:
            session.close()
    
    def get_balance_data(self, merchant: str, year: int, month: int) -> List[Dict[str, Any]]:
        session = get_session()
        tx_service = TransactionService()
        
        try:
            tx_data = tx_service.get_monthly_data(merchant, year, month)
            
            date_prefix = f"{year}-{month:02d}"
            balances = session.query(AgentBalance).filter(
                and_(
                    AgentBalance.merchant == merchant,
                    AgentBalance.transaction_date.like(f"{date_prefix}%")
                )
            ).order_by(AgentBalance.transaction_date).all()
            
            balance_map = {b.transaction_date: b for b in balances}
            
            result = []
            for tx_row in tx_data:
                date = tx_row['transaction_date']
                balance = balance_map.get(date)
                
                kira_fpx = tx_row['fpx_amount']
                kira_ewallet = tx_row['ewallet_amount']
                
                rate_fpx = balance.commission_rate_fpx if balance else None
                rate_ewallet = balance.commission_rate_ewallet if balance else None
                
                fpx_commission = self._r(kira_fpx * rate_fpx) if rate_fpx else None
                ewallet_commission = self._r(kira_ewallet * rate_ewallet) if rate_ewallet else None
                
                gross = None
                if fpx_commission is not None or ewallet_commission is not None:
                    gross = self._r((fpx_commission or 0) + (ewallet_commission or 0))
                
                available_fpx = self._r(tx_row['available_fpx'] * rate_fpx) if rate_fpx and tx_row['available_fpx'] else None
                available_ewallet = self._r(tx_row['available_ewallet'] * rate_ewallet) if rate_ewallet and tx_row['available_ewallet'] else None
                available_total = None
                if available_fpx is not None or available_ewallet is not None:
                    available_total = self._r((available_fpx or 0) + (available_ewallet or 0))
                
                row = {
                    'transaction_date': date,
                    'kira_amount_fpx': kira_fpx,
                    'commission_rate_fpx': rate_fpx,
                    'fpx_commission': fpx_commission,
                    'kira_amount_ewallet': kira_ewallet,
                    'commission_rate_ewallet': rate_ewallet,
                    'ewallet_commission': ewallet_commission,
                    'gross_amount': gross,
                    'available_fpx': available_fpx,
                    'available_ewallet': available_ewallet,
                    'available_total': available_total,
                    'withdrawal_amount': balance.withdrawal_amount if balance else None,
                    'balance': balance.balance if balance else None
                }
                
                if balance:
                    row['agent_balance_id'] = balance.agent_balance_id
                
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
                balance_id = row.get('agent_balance_id')
                if balance_id and isinstance(balance_id, (int, float)):
                    valid_ids.append(int(balance_id))
            
            if not valid_ids:
                return 0
            
            records = session.query(AgentBalance).filter(
                AgentBalance.agent_balance_id.in_(valid_ids)
            ).all()
            
            records_by_id = {r.agent_balance_id: r for r in records}
            manual_by_id = {int(row['agent_balance_id']): row for row in manual_data if row.get('agent_balance_id')}
            
            merchants_to_recalc = set()
            
            for balance_id in valid_ids:
                existing = records_by_id.get(balance_id)
                row = manual_by_id.get(balance_id)
                
                if not existing or not row:
                    continue
                
                rate_fpx_raw = row.get('commission_rate_fpx')
                if rate_fpx_raw == 'CLEAR':
                    existing.commission_rate_fpx = None
                    merchants_to_recalc.add(existing.merchant)
                elif rate_fpx_raw is not None:
                    existing.commission_rate_fpx = self._to_float(rate_fpx_raw)
                    merchants_to_recalc.add(existing.merchant)
                
                rate_ewallet_raw = row.get('commission_rate_ewallet')
                if rate_ewallet_raw == 'CLEAR':
                    existing.commission_rate_ewallet = None
                    merchants_to_recalc.add(existing.merchant)
                elif rate_ewallet_raw is not None:
                    existing.commission_rate_ewallet = self._to_float(rate_ewallet_raw)
                    merchants_to_recalc.add(existing.merchant)
                
                withdrawal_raw = row.get('withdrawal_amount')
                if withdrawal_raw == 'CLEAR':
                    existing.withdrawal_amount = None
                    merchants_to_recalc.add(existing.merchant)
                elif withdrawal_raw is not None:
                    existing.withdrawal_amount = self._to_float(withdrawal_raw)
                    merchants_to_recalc.add(existing.merchant)
                
                count += 1
            
            for merchant in merchants_to_recalc:
                self._recalculate_balances(session, merchant)
            
            session.commit()
            logger.info(f"Updated {count} agent balance rows")
            return count
            
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to save agent manual data: {e}")
            raise
        finally:
            session.close()
    
    def _recalculate_balances(self, session, merchant: str):
        rows = session.query(AgentBalance).filter(
            AgentBalance.merchant == merchant
        ).order_by(AgentBalance.transaction_date).all()
        
        prev_balance = 0
        
        for row in rows:
            has_activity = (
                row.withdrawal_amount is not None 
                or prev_balance != 0
            )
            
            if has_activity:
                row.balance = self._r(prev_balance - (row.withdrawal_amount or 0))
            else:
                row.balance = None
            
            prev_balance = row.balance if row.balance is not None else prev_balance
    
    def upload_to_sheet(self, merchant: str, year: int, month: int, sheet_name: str = None) -> Dict[str, Any]:
        from src.core.loader import load_settings
        
        if sheet_name is None:
            settings = load_settings()
            sheet_name = settings['google_sheets']['sheets']['agent_ledger']
        
        try:
            data = self.get_balance_data(merchant, year, month)
            
            if not data:
                return {'success': False, 'error': 'No data to upload'}
            
            columns = [
                'agent_balance_id', 'transaction_date',
                'kira_amount_fpx', 'commission_rate_fpx', 'fpx_commission',
                'kira_amount_ewallet', 'commission_rate_ewallet', 'ewallet_commission',
                'gross_amount',
                'available_fpx', 'available_ewallet', 'available_total',
                'withdrawal_amount', 'balance'
            ]
            
            rows = []
            for record in data:
                row = [record.get(col, '') for col in columns]
                rows.append(row)
            
            self.sheets_client.write_data(sheet_name, rows, start_cell='A5')
            
            logger.info(f"Uploaded {len(rows)} agent balance rows to {sheet_name}")
            return {
                'success': True,
                'rows_uploaded': len(rows),
                'sheet_name': sheet_name
            }
            
        except Exception as e:
            logger.error(f"Failed to upload to sheet: {e}")
            return {'success': False, 'error': str(e)}
