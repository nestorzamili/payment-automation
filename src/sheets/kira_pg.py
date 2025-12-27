import pandas as pd
from typing import List, Dict, Any, Optional

from sqlalchemy import func

from src.core.database import get_session
from src.core.models import KiraPGFee, KiraTransaction, PGTransaction
from src.core.logger import get_logger
from src.sheets.client import SheetsClient
from src.sheets.parameters import ParameterLoader
from src.utils.holiday import load_malaysia_holidays, calculate_settlement_date

logger = get_logger(__name__)


class KiraPGService:
    
    def __init__(
        self, 
        sheets_client: Optional[SheetsClient] = None, 
        param_loader: Optional[ParameterLoader] = None
    ):
        self.sheets_client = sheets_client or SheetsClient()
        self.param_loader = param_loader or ParameterLoader(self.sheets_client)
        self.param_loader.load_all_parameters()
        self.public_holidays = load_malaysia_holidays()
        self.add_on_holidays = self.param_loader.get_add_on_holidays()
    
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
    
    def _normalize_channel(self, channel: Optional[str]) -> str:
        if not channel:
            return 'EWALLET'
        ch_upper = channel.upper().strip()
        if ch_upper in ('FPX', 'FPXC') or 'FPX' in ch_upper:
            return 'FPX'
        return 'EWALLET'
    
    def get_kira_pg_data(self) -> List[Dict[str, Any]]:
        session = get_session()
        
        try:
            kira_agg = session.query(
                PGTransaction.account_label.label('pg_account_label'),
                func.substr(KiraTransaction.transaction_date, 1, 10).label('kira_date'),
                KiraTransaction.payment_method,
                func.sum(KiraTransaction.amount).label('kira_amount'),
                func.sum(KiraTransaction.mdr).label('mdr'),
                func.sum(KiraTransaction.settlement_amount).label('kira_settlement_amount'),
            ).join(
                PGTransaction,
                KiraTransaction.transaction_id == PGTransaction.transaction_id
            ).filter(
                ~KiraTransaction.merchant.ilike('%test%')
            ).group_by(
                PGTransaction.account_label,
                func.substr(KiraTransaction.transaction_date, 1, 10),
                KiraTransaction.payment_method
            ).all()
            
            pg_agg = session.query(
                PGTransaction.account_label.label('pg_account_label'),
                func.substr(PGTransaction.transaction_date, 1, 10).label('pg_date'),
                PGTransaction.channel,
                func.sum(PGTransaction.amount).label('pg_amount'),
                func.count().label('volume'),
            ).group_by(
                PGTransaction.account_label,
                func.substr(PGTransaction.transaction_date, 1, 10),
                PGTransaction.channel
            ).all()
            
            kira_map: Dict[tuple, Dict[str, float]] = {}
            for row in kira_agg:
                channel = self._normalize_channel(row.payment_method)
                key = (row.pg_account_label, row.kira_date, channel)
                if key not in kira_map:
                    kira_map[key] = {'kira_amount': 0, 'mdr': 0, 'kira_settlement_amount': 0}
                kira_map[key]['kira_amount'] += row.kira_amount or 0
                kira_map[key]['mdr'] += row.mdr or 0
                kira_map[key]['kira_settlement_amount'] += row.kira_settlement_amount or 0
            
            pg_map: Dict[tuple, Dict[str, float]] = {}
            for row in pg_agg:
                channel = self._normalize_channel(row.channel)
                key = (row.pg_account_label, row.pg_date, channel)
                if key not in pg_map:
                    pg_map[key] = {'pg_amount': 0, 'volume': 0}
                pg_map[key]['pg_amount'] += row.pg_amount or 0
                pg_map[key]['volume'] += row.volume or 0
            
            all_keys = set(kira_map.keys()) | set(pg_map.keys())
            
            fee_map = self._load_fee_map(session)
            rows = []
            
            for key in all_keys:
                pg_account_label, date, channel = key
                
                kira_data = kira_map.get(key, {'kira_amount': 0, 'mdr': 0, 'kira_settlement_amount': 0})
                pg_data = pg_map.get(key, {'pg_amount': 0, 'volume': 0})
                
                if kira_data['kira_amount'] == 0 and pg_data['pg_amount'] == 0:
                    continue
                
                fee_record = fee_map.get(key)
                fee_rate = fee_record.fee_rate if fee_record else None
                remarks = fee_record.remarks if fee_record else None
                
                fees = 0
                if fee_rate is not None:
                    fees = self._r(pg_data['pg_amount'] * fee_rate / 100)
                
                settlement_amount = self._r(pg_data['pg_amount'] - fees) if fees else None
                daily_variance = self._r(kira_data['kira_amount'] - pg_data['pg_amount'])
                
                settlement_rule = self.param_loader.get_settlement_rule(channel)
                settlement_date = calculate_settlement_date(
                    date, settlement_rule, self.public_holidays, self.add_on_holidays
                )
                
                rows.append({
                    'pg_merchant': pg_account_label,
                    'channel': channel,
                    'kira_amount': self._r(kira_data['kira_amount']),
                    'mdr': self._r(kira_data['mdr']),
                    'kira_settlement_amount': self._r(kira_data['kira_settlement_amount']),
                    'pg_date': date,
                    'amount_pg': self._r(pg_data['pg_amount']),
                    'transaction_count': pg_data['volume'],
                    'settlement_rule': settlement_rule,
                    'settlement_date': settlement_date,
                    'fee_rate': fee_rate,
                    'fees': fees,
                    'settlement_amount': settlement_amount,
                    'daily_variance': daily_variance,
                    'cumulative_variance': 0,
                    'remarks': remarks,
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
    
    def _load_fee_map(self, session) -> Dict[tuple, KiraPGFee]:
        fees = session.query(KiraPGFee).all()
        return {(f.pg_account_label, f.transaction_date, f.channel): f for f in fees}
    
    def save_manual_data(self, manual_data: List[Dict[str, Any]]) -> int:
        session = get_session()
        count = 0
        
        try:
            for row in manual_data:
                pg_merchant = row.get('pg_merchant')
                date = row.get('pg_date')
                channel = self._normalize_channel(row.get('channel'))
                
                if not pg_merchant or not date:
                    continue
                
                logger.debug(f"Saving fee for {pg_merchant}/{date}/{channel}: rate={row.get('fee_rate')}")
                
                existing = session.query(KiraPGFee).filter(
                    KiraPGFee.pg_account_label == pg_merchant,
                    KiraPGFee.transaction_date == date,
                    KiraPGFee.channel == channel
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
                    new_record = KiraPGFee(
                        pg_account_label=pg_merchant,
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
    
    def upload_to_sheet(self, df: pd.DataFrame, sheet_name: str) -> Dict[str, Any]:
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
