from typing import List, Dict, Any, Optional
from calendar import monthrange

from sqlalchemy import and_

from src.core.database import get_session
from src.core.models import AgentLedger
from src.core.logger import get_logger
from src.services.client import SheetsClient

logger = get_logger(__name__)


class AgentLedgerService:
    
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
            
            existing = session.query(AgentLedger.transaction_date).filter(
                and_(
                    AgentLedger.merchant == merchant,
                    AgentLedger.transaction_date.like(f"{date_prefix}%")
                )
            ).all()
            
            existing_dates = {r[0] for r in existing}
            
            for day in range(1, last_day + 1):
                date_str = f"{year}-{month:02d}-{day:02d}"
                if date_str not in existing_dates:
                    new_record = AgentLedger(
                        merchant=merchant,
                        transaction_date=date_str
                    )
                    session.add(new_record)
            
            session.commit()
            
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to init agent ledger: {e}")
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
            
            all_deposits = session.query(Deposit).filter(
                Deposit.merchant == merchant
            ).all()
            
            fpx_by_settlement = {}
            ewallet_by_settlement = {}
            for dep in all_deposits:
                if dep.fpx_settlement_date and dep.fpx_amount:
                    if dep.fpx_settlement_date not in fpx_by_settlement:
                        fpx_by_settlement[dep.fpx_settlement_date] = []
                    fpx_by_settlement[dep.fpx_settlement_date].append(dep)
                if dep.ewallet_settlement_date and dep.ewallet_amount:
                    if dep.ewallet_settlement_date not in ewallet_by_settlement:
                        ewallet_by_settlement[dep.ewallet_settlement_date] = []
                    ewallet_by_settlement[dep.ewallet_settlement_date].append(dep)
            
            self._recalculate_balances(session, merchant)
            session.commit()
            
            ledgers = session.query(AgentLedger).filter(
                and_(
                    AgentLedger.merchant == merchant,
                    AgentLedger.transaction_date.like(f"{date_prefix}%")
                )
            ).order_by(AgentLedger.transaction_date).all()
            
            ledger_map = {lg.transaction_date: lg for lg in ledgers}
            
            result = []
            for deposit in deposits:
                date = deposit.transaction_date
                ledger = ledger_map.get(date)
                
                kira_fpx = deposit.fpx_amount or 0
                kira_ewallet = deposit.ewallet_amount or 0
                
                rate_fpx = ledger.commission_rate_fpx if ledger else None
                rate_ewallet = ledger.commission_rate_ewallet if ledger else None
                
                fpx_commission = self._r(kira_fpx * rate_fpx / 1000) if rate_fpx else None
                ewallet_commission = self._r(kira_ewallet * rate_ewallet / 1000) if rate_ewallet else None
                
                gross = None
                if fpx_commission is not None or ewallet_commission is not None:
                    gross = self._r((fpx_commission or 0) + (ewallet_commission or 0))
                
                available_fpx = 0
                if date in fpx_by_settlement and rate_fpx:
                    fpx_sum = sum(d.fpx_amount or 0 for d in fpx_by_settlement[date])
                    available_fpx = self._r(fpx_sum * rate_fpx / 1000) or 0
                
                available_ewallet = 0
                if date in ewallet_by_settlement and rate_ewallet:
                    ewallet_sum = sum(d.ewallet_amount or 0 for d in ewallet_by_settlement[date])
                    available_ewallet = self._r(ewallet_sum * rate_ewallet / 1000) or 0
                
                available_total = self._r(available_fpx + available_ewallet)
                
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
                    'withdrawal_amount': ledger.withdrawal_amount if ledger else None,
                    'balance': ledger.balance if ledger else None,
                    'updated_at': ledger.updated_at if ledger else None
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
            
            records = session.query(AgentLedger).filter(
                AgentLedger.id.in_(valid_ids)
            ).all()
            
            records_by_id = {r.id: r for r in records}
            manual_by_id = {int(row['id']): row for row in manual_data if row.get('id')}
            
            for ledger_id in valid_ids:
                existing = records_by_id.get(ledger_id)
                row = manual_by_id.get(ledger_id)
                
                if not existing or not row:
                    continue
                
                rate_fpx_raw = row.get('commission_rate_fpx')
                if rate_fpx_raw == 'CLEAR':
                    existing.commission_rate_fpx = None
                elif rate_fpx_raw is not None:
                    existing.commission_rate_fpx = self._to_float(rate_fpx_raw)
                
                rate_ewallet_raw = row.get('commission_rate_ewallet')
                if rate_ewallet_raw == 'CLEAR':
                    existing.commission_rate_ewallet = None
                elif rate_ewallet_raw is not None:
                    existing.commission_rate_ewallet = self._to_float(rate_ewallet_raw)
                
                withdrawal_raw = row.get('withdrawal_amount')
                if withdrawal_raw == 'CLEAR':
                    existing.withdrawal_amount = None
                elif withdrawal_raw is not None:
                    existing.withdrawal_amount = self._to_float(withdrawal_raw)
                
                count += 1
            
            session.commit()
            logger.info(f"Updated {count} agent ledger rows")
            return count
            
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to save agent manual data: {e}")
            raise
        finally:
            session.close()
    
    def _recalculate_balances(self, session, merchant: str):
        from src.core.models import Deposit
        
        rows = session.query(AgentLedger).filter(
            AgentLedger.merchant == merchant
        ).order_by(AgentLedger.transaction_date).all()
        
        deposits = session.query(Deposit).filter(
            Deposit.merchant == merchant
        ).all()
        
        # Aggregate deposits by settlement date
        fpx_by_settlement = {}
        ewallet_by_settlement = {}
        for dep in deposits:
            if dep.fpx_settlement_date and dep.fpx_amount:
                if dep.fpx_settlement_date not in fpx_by_settlement:
                    fpx_by_settlement[dep.fpx_settlement_date] = []
                fpx_by_settlement[dep.fpx_settlement_date].append(dep)
            if dep.ewallet_settlement_date and dep.ewallet_amount:
                if dep.ewallet_settlement_date not in ewallet_by_settlement:
                    ewallet_by_settlement[dep.ewallet_settlement_date] = []
                ewallet_by_settlement[dep.ewallet_settlement_date].append(dep)
        
        prev_balance = 0
        
        for row in rows:
            date = row.transaction_date
            rate_fpx = row.commission_rate_fpx or 0
            rate_ewallet = row.commission_rate_ewallet or 0
            
            # Calculate available amounts based on settlement date
            avail_fpx = 0
            if date in fpx_by_settlement and rate_fpx:
                fpx_sum = sum(d.fpx_amount or 0 for d in fpx_by_settlement[date])
                avail_fpx = self._r(fpx_sum * rate_fpx / 1000) or 0
            
            avail_ewallet = 0
            if date in ewallet_by_settlement and rate_ewallet:
                ewallet_sum = sum(d.ewallet_amount or 0 for d in ewallet_by_settlement[date])
                avail_ewallet = self._r(ewallet_sum * rate_ewallet / 1000) or 0
            
            available_total = avail_fpx + avail_ewallet
            
            has_activity = (
                row.withdrawal_amount is not None 
                or available_total > 0
                or prev_balance != 0
            )
            
            if has_activity:
                row.balance = self._r(
                    prev_balance 
                    + available_total 
                    - (row.withdrawal_amount or 0)
                )
            else:
                row.balance = None
            
            prev_balance = row.balance if row.balance is not None else prev_balance
    
    def upload_to_sheet(self, merchant: str, year: int, month: int, sheet_name: str) -> Dict[str, Any]:
        try:
            data = self.get_ledger_data(merchant, year, month)
            
            if not data:
                return {'success': False, 'error': 'No data to upload'}
            
            columns = [
                'id', 'transaction_date',
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
            
            logger.info(f"Uploaded {len(rows)} agent ledger rows to {sheet_name}")
            return {
                'success': True,
                'rows_uploaded': len(rows),
                'sheet_name': sheet_name
            }
            
        except Exception as e:
            logger.error(f"Failed to upload to sheet: {e}")
            return {'success': False, 'error': str(e)}
