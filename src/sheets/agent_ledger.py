from typing import List, Dict, Any
from sqlalchemy import and_

from src.core.database import get_session
from src.core.models import AgentLedger
from src.core.logger import get_logger
from src.sheets.client import SheetsClient

logger = get_logger(__name__)


class AgentLedgerService:
    
    def __init__(self, sheets_client: SheetsClient = None):
        self.sheets_client = sheets_client or SheetsClient()
    
    def _r(self, value):
        return round(value, 2) if value is not None else None
    
    def init_from_deposit(self, deposit_rows: List[Dict[str, Any]]) -> int:
        if not deposit_rows:
            return 0
        
        aggregated = self._aggregate_deposit_data(deposit_rows)
        settlement_map = self._build_settlement_map(deposit_rows)
        return self._upsert_ledger_rows(aggregated, settlement_map)
    
    def _aggregate_deposit_data(self, deposit_rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        result = {}
        
        for row in deposit_rows:
            merchant = row['Merchant']
            date = row['Transaction Date']
            channel = row['Channel'].upper()
            kira_amount = row['Kira Amount']
            
            key = (merchant, date)
            if key not in result:
                result[key] = {
                    'merchant': merchant,
                    'transaction_date': date,
                    'kira_amount_fpx': 0,
                    'kira_amount_ewallet': 0
                }
            
            if channel in ('FPX', 'FPXC'):
                result[key]['kira_amount_fpx'] += kira_amount
            else:
                result[key]['kira_amount_ewallet'] += kira_amount
        
        return result
    
    def _build_settlement_map(self, deposit_rows: List[Dict[str, Any]]) -> Dict:
        settlement_map = {}
        
        for row in deposit_rows:
            merchant = row['Merchant']
            settlement_date = row['Settlement Date']
            channel = row['Channel'].upper()
            kira_amount = row['Kira Amount']
            
            channel_type = 'fpx' if channel in ('FPX', 'FPXC') else 'ewallet'
            key = (merchant, settlement_date, channel_type)
            
            if key not in settlement_map:
                settlement_map[key] = 0
            settlement_map[key] += kira_amount
        
        return settlement_map
    
    def _upsert_ledger_rows(self, aggregated: Dict, settlement_map: Dict) -> int:
        session = get_session()
        count = 0
        
        try:
            sorted_keys = sorted(aggregated.keys(), key=lambda x: (x[0], x[1]))
            
            existing_records = {}
            for merchant, date in sorted_keys:
                record = session.query(AgentLedger).filter(
                    and_(
                        AgentLedger.merchant == merchant,
                        AgentLedger.transaction_date == date
                    )
                ).first()
                if record:
                    existing_records[(merchant, date)] = record
            
            for key in sorted_keys:
                data = aggregated[key]
                merchant = data['merchant']
                transaction_date = data['transaction_date']
                
                kira_fpx = self._r(data['kira_amount_fpx'])
                kira_ewallet = self._r(data['kira_amount_ewallet'])
                
                settlement_kira_fpx = self._r(settlement_map.get((merchant, transaction_date, 'fpx'), 0))
                settlement_kira_ewallet = self._r(settlement_map.get((merchant, transaction_date, 'ewallet'), 0))
                
                existing = existing_records.get((merchant, transaction_date))
                
                if existing:
                    existing.kira_amount_fpx = kira_fpx
                    existing.kira_amount_ewallet = kira_ewallet
                    existing.settlement_kira_fpx = settlement_kira_fpx
                    existing.settlement_kira_ewallet = settlement_kira_ewallet
                    
                    if existing.commission_rate_fpx is not None:
                        existing.fpx = self._r(kira_fpx * existing.commission_rate_fpx)
                        existing.available_settlement_fpx = self._r(settlement_kira_fpx * existing.commission_rate_fpx)
                    
                    if existing.commission_rate_ewallet is not None:
                        existing.ewallet = self._r(kira_ewallet * existing.commission_rate_ewallet)
                        existing.available_settlement_ewallet = self._r(settlement_kira_ewallet * existing.commission_rate_ewallet)
                    
                    if existing.fpx is not None or existing.ewallet is not None:
                        existing.gross_amount = self._r((existing.fpx or 0) + (existing.ewallet or 0))
                    else:
                        existing.gross_amount = None
                    
                    if existing.available_settlement_fpx is not None or existing.available_settlement_ewallet is not None:
                        existing.available_settlement_total = self._r((existing.available_settlement_fpx or 0) + (existing.available_settlement_ewallet or 0))
                    else:
                        existing.available_settlement_total = None
                else:
                    new_record = AgentLedger(
                        merchant=merchant,
                        transaction_date=transaction_date,
                        kira_amount_fpx=kira_fpx,
                        kira_amount_ewallet=kira_ewallet,
                        settlement_kira_fpx=settlement_kira_fpx,
                        settlement_kira_ewallet=settlement_kira_ewallet
                    )
                    session.add(new_record)
                
                count += 1
            
            affected_merchants = set(data['merchant'] for data in aggregated.values())
            for merchant in affected_merchants:
                self._recalculate_balances(session, merchant)
            
            session.commit()
            logger.info(f"Upserted {count} agent ledger rows")
            return count
            
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to upsert agent ledger: {e}")
            raise
        finally:
            session.close()
    
    def get_ledger(self, merchant: str, year: int, month: int) -> List[Dict[str, Any]]:
        session = get_session()
        
        try:
            date_prefix = f"{year}-{month:02d}"
            
            records = session.query(AgentLedger).filter(
                and_(
                    AgentLedger.merchant == merchant,
                    AgentLedger.transaction_date.like(f"{date_prefix}%")
                )
            ).order_by(AgentLedger.transaction_date).all()
            
            return [r.to_dict() for r in records]
            
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
                AgentLedger.agent_ledger_id.in_(valid_ids)
            ).all()
            
            records_by_id = {r.agent_ledger_id: r for r in records}
            manual_by_id = {int(row['id']): row for row in manual_data if row.get('id')}
            
            merchants_to_recalc = set()
            
            for ledger_id in valid_ids:
                existing = records_by_id.get(ledger_id)
                row = manual_by_id.get(ledger_id)
                
                if not existing or not row:
                    continue
                
                rate_fpx_raw = row.get('commission_rate_fpx')
                if rate_fpx_raw == 'CLEAR':
                    existing.commission_rate_fpx = None
                    existing.fpx = None
                    existing.available_settlement_fpx = None
                    merchants_to_recalc.add(existing.merchant)
                elif rate_fpx_raw is not None:
                    rate_fpx = self._to_float(rate_fpx_raw)
                    if rate_fpx is not None:
                        existing.commission_rate_fpx = rate_fpx
                        existing.fpx = self._r((existing.kira_amount_fpx or 0) * rate_fpx)
                        merchants_to_recalc.add(existing.merchant)
                
                rate_ewallet_raw = row.get('commission_rate_ewallet')
                if rate_ewallet_raw == 'CLEAR':
                    existing.commission_rate_ewallet = None
                    existing.ewallet = None
                    existing.available_settlement_ewallet = None
                    merchants_to_recalc.add(existing.merchant)
                elif rate_ewallet_raw is not None:
                    rate_ewallet = self._to_float(rate_ewallet_raw)
                    if rate_ewallet is not None:
                        existing.commission_rate_ewallet = rate_ewallet
                        existing.ewallet = self._r((existing.kira_amount_ewallet or 0) * rate_ewallet)
                        merchants_to_recalc.add(existing.merchant)
                
                if existing.fpx is not None or existing.ewallet is not None:
                    existing.gross_amount = self._r((existing.fpx or 0) + (existing.ewallet or 0))
                else:
                    existing.gross_amount = None
                
                withdrawal_raw = row.get('withdrawal_amount')
                if withdrawal_raw == 'CLEAR':
                    existing.withdrawal_amount = None
                    merchants_to_recalc.add(existing.merchant)
                elif withdrawal_raw is not None:
                    withdrawal = self._to_float(withdrawal_raw)
                    if withdrawal is not None:
                        existing.withdrawal_amount = withdrawal
                        merchants_to_recalc.add(existing.merchant)
                
                count += 1
            
            for merchant in merchants_to_recalc:
                self._recalculate_commissions_and_balances(session, merchant)
            
            session.commit()
            logger.info(f"Updated {count} agent manual data rows by ID")
            return count
            
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to save agent manual data: {e}")
            raise
        finally:
            session.close()
    
    def _recalculate_commissions_and_balances(self, session, merchant: str):
        rows = session.query(AgentLedger).filter(
            AgentLedger.merchant == merchant
        ).order_by(AgentLedger.transaction_date).all()
        
        for row in rows:
            if row.commission_rate_fpx is not None:
                row.fpx = self._r((row.kira_amount_fpx or 0) * row.commission_rate_fpx)
                row.available_settlement_fpx = self._r((row.settlement_kira_fpx or 0) * row.commission_rate_fpx)
            
            if row.commission_rate_ewallet is not None:
                row.ewallet = self._r((row.kira_amount_ewallet or 0) * row.commission_rate_ewallet)
                row.available_settlement_ewallet = self._r((row.settlement_kira_ewallet or 0) * row.commission_rate_ewallet)
            
            if row.fpx is not None or row.ewallet is not None:
                row.gross_amount = self._r((row.fpx or 0) + (row.ewallet or 0))
            else:
                row.gross_amount = None
            
            if row.available_settlement_fpx is not None or row.available_settlement_ewallet is not None:
                row.available_settlement_total = self._r((row.available_settlement_fpx or 0) + (row.available_settlement_ewallet or 0))
            else:
                row.available_settlement_total = None
        
        self._recalculate_balances(session, merchant)
    
    def _recalculate_balances(self, session, merchant: str):
        rows = session.query(AgentLedger).filter(
            AgentLedger.merchant == merchant
        ).order_by(AgentLedger.transaction_date).all()
        
        prev_balance = 0
        
        for row in rows:
            has_activity = (
                row.withdrawal_amount is not None 
                or row.available_settlement_total 
                or prev_balance != 0
            )
            
            if has_activity:
                row.balance = self._r(
                    prev_balance
                    + (row.available_settlement_total or 0)
                    - (row.withdrawal_amount or 0)
                )
            else:
                row.balance = None
            
            prev_balance = row.balance if row.balance is not None else prev_balance
    
    def upload_to_sheet(self, data: List[Dict[str, Any]], sheet_name: str = None) -> Dict[str, Any]:
        from src.core.loader import load_settings
        
        if sheet_name is None:
            settings = load_settings()
            sheet_name = settings['google_sheets']['sheets'].get('agent_ledger', 'Agents Balance & Settlement Ledger')
        
        if not data:
            return {'success': False, 'error': 'No data to upload'}
        
        try:
            columns = [
                'agent_ledger_id', 'transaction_date',
                'commission_rate_fpx', 'fpx',
                'commission_rate_ewallet', 'ewallet',
                'gross_amount',
                'available_settlement_fpx', 'available_settlement_ewallet', 'available_settlement_total',
                'withdrawal_amount', 'balance',
                'updated_at'
            ]
            
            rows = []
            for record in data:
                row = [record.get(col, '') for col in columns]
                rows.append(row)
            
            self.sheets_client.write_data(sheet_name, rows, start_cell='A5')
            
            logger.info(f"Uploaded {len(rows)} rows to {sheet_name}")
            return {
                'success': True,
                'rows_uploaded': len(rows),
                'sheet_name': sheet_name
            }
            
        except Exception as e:
            logger.error(f"Failed to upload to sheet: {e}")
            return {'success': False, 'error': str(e)}
