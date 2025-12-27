import pandas as pd
from typing import List, Dict, Any, Set

from sqlalchemy import and_

from src.core.database import get_session
from src.core.models import Transaction, DepositFee
from src.core.logger import get_logger
from src.sheets.client import SheetsClient
from src.sheets.parameters import ParameterLoader

logger = get_logger(__name__)


class KiraPGService:
    
    def __init__(self, sheets_client: SheetsClient = None, param_loader: ParameterLoader = None):
        self.sheets_client = sheets_client or SheetsClient()
        self.param_loader = param_loader or ParameterLoader(self.sheets_client)
    
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
    
    def get_kira_pg_data(self) -> List[Dict[str, Any]]:
        session = get_session()
        
        try:
            transactions = session.query(Transaction).all()
            
            if not transactions:
                return []
            
            fee_map = self._load_fee_map()
            
            rows = []
            for tx in transactions:
                if not tx.pg_account_label:
                    continue
                if (tx.kira_amount or 0) == 0 and (tx.pg_amount or 0) == 0:
                    continue
                if tx.merchant and 'test' in tx.merchant.lower():
                    continue
                    
                fee_key = (tx.merchant, tx.transaction_date, tx.channel)
                fee_record = fee_map.get(fee_key)
                
                fee_rate = fee_record.fee_rate if fee_record else None
                remarks = fee_record.remarks if fee_record else None
                
                fees = 0
                if fee_rate is not None:
                    fees = self._r((tx.pg_amount or 0) * fee_rate / 100)
                
                settlement_amount = self._r((tx.pg_amount or 0) - fees) if fees else None
                
                daily_variance = self._r((tx.kira_amount or 0) - (tx.pg_amount or 0))
                
                rows.append({
                    'pg_merchant': tx.pg_account_label,
                    'channel': tx.channel,
                    'kira_amount': self._r(tx.kira_amount),
                    'mdr': self._r(tx.mdr),
                    'kira_settlement_amount': self._r(tx.kira_settlement_amount),
                    'pg_date': tx.transaction_date,
                    'amount_pg': self._r(tx.pg_amount),
                    'transaction_count': tx.volume,
                    'settlement_rule': 'T+1',
                    'settlement_date': tx.settlement_date,
                    'fee_rate': fee_rate,
                    'fees': fees,
                    'settlement_amount': settlement_amount,
                    'daily_variance': daily_variance,
                    'cumulative_variance': 0,
                    'remarks': remarks,
                    'merchant': tx.merchant
                })
            
            rows.sort(key=lambda x: (x['pg_date'], x['pg_merchant'] or '', x['channel']))
            
            cumulative = 0
            for row in rows:
                cumulative += row['daily_variance'] or 0
                row['cumulative_variance'] = self._r(cumulative)
            
            logger.info(f"Generated {len(rows)} Kira PG rows from transactions")
            return rows
            
        finally:
            session.close()
    
    def _normalize_channel(self, channel: str) -> str:
        if not channel:
            return 'EWALLET'
        ch_upper = channel.upper().strip()
        if ch_upper in ('FPX', 'FPXC') or 'FPX' in ch_upper:
            return 'FPX'
        return 'EWALLET'
    
    def _load_fee_map(self) -> Dict:
        session = get_session()
        try:
            fees = session.query(DepositFee).all()
            return {(f.merchant, f.transaction_date, f.channel): f for f in fees}
        finally:
            session.close()
    
    def save_manual_data(self, manual_data: List[Dict[str, Any]]) -> int:
        session = get_session()
        count = 0
        
        try:
            tx_map = {}
            transactions = session.query(Transaction).all()
            for tx in transactions:
                key = (tx.pg_account_label, tx.transaction_date, tx.channel)
                if key not in tx_map:
                    tx_map[key] = tx.merchant
            
            for row in manual_data:
                pg_merchant = row.get('pg_merchant')
                date = row.get('pg_date')
                channel = self._normalize_channel(row.get('channel'))
                
                lookup_key = (pg_merchant, date, channel)
                merchant = tx_map.get(lookup_key)
                
                if not merchant:
                    logger.debug(f"No merchant found for key: {lookup_key}")
                    continue
                
                logger.debug(f"Saving fee for {merchant}/{date}/{channel}: rate={row.get('fee_rate')}")
                
                existing = session.query(DepositFee).filter(
                    and_(
                        DepositFee.merchant == merchant,
                        DepositFee.transaction_date == date,
                        DepositFee.channel == channel
                    )
                ).first()
                
                fee_rate_raw = row.get('fee_rate')
                remarks_raw = row.get('remarks')
                
                if existing:
                    if fee_rate_raw == 'CLEAR':
                        existing.fee_rate = None
                    elif fee_rate_raw is not None:
                        existing.fee_rate = self._to_float(fee_rate_raw)
                    
                    if remarks_raw == 'CLEAR':
                        existing.remarks = None
                    elif remarks_raw is not None and str(remarks_raw).strip():
                        existing.remarks = str(remarks_raw).strip()
                else:
                    new_record = DepositFee(
                        merchant=merchant,
                        transaction_date=date,
                        channel=channel,
                        fee_rate=self._to_float(fee_rate_raw) if fee_rate_raw != 'CLEAR' else None,
                        remarks=str(remarks_raw).strip() if remarks_raw and remarks_raw != 'CLEAR' else None
                    )
                    session.add(new_record)
                
                count += 1
            
            session.commit()
            logger.info(f"Saved {count} Kira PG manual inputs")
            return count
            
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to save manual data: {e}")
            raise
        finally:
            session.close()
    
    def upload_to_sheet(self, df: pd.DataFrame, sheet_name: str = None) -> Dict[str, Any]:
        from src.core.loader import load_settings
        
        if sheet_name is None:
            settings = load_settings()
            sheet_name = settings['google_sheets']['sheets']['kira_pg']
        
        try:
            self.sheets_client.upload_dataframe(sheet_name, df, include_header=True, clear_first=True)
            logger.info(f"Uploaded {len(df)} rows to {sheet_name}")
            
            return {
                'success': True,
                'rows_uploaded': len(df),
                'sheet_name': sheet_name
            }
        except Exception as e:
            logger.error(f"Failed to upload to sheet: {e}")
            return {
                'success': False,
                'error': str(e)
            }
