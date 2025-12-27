import pandas as pd
from typing import List, Dict, Any, Set
from datetime import datetime

from sqlalchemy import and_

from src.core.database import get_session
from src.core.models import KiraTransaction, PGTransaction, DepositFee
from src.core.logger import get_logger
from src.sheets.client import SheetsClient
from src.sheets.parameters import ParameterLoader
from src.utils.holiday import load_malaysia_holidays, calculate_settlement_date

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
    
    def generate_kira_pg(
        self, 
        joined_data: List[Dict[str, Any]], 
        public_holidays: Set[str] = None,
        add_on_holidays: Set[str] = None
    ) -> pd.DataFrame:
        logger.info(f"Generating Kira PG from {len(joined_data)} transactions")
        
        if not joined_data:
            return pd.DataFrame()
        
        if public_holidays is None or add_on_holidays is None:
            self.param_loader.load_all_parameters()
            add_on_holidays = self.param_loader.get_add_on_holidays()
            public_holidays = load_malaysia_holidays()
        
        rows = self._calculate_rows(joined_data, public_holidays, add_on_holidays)
        
        df = pd.DataFrame(rows)
        
        if 'PG KIRA Daily Variance' in df.columns:
            df['PG KIRA Cumulative Variance'] = df['PG KIRA Daily Variance'].cumsum()
        
        logger.info(f"Generated {len(df)} Kira PG rows")
        return df
    
    def get_kira_pg_data(
        self,
        joined_data: List[Dict[str, Any]],
        public_holidays: Set[str] = None,
        add_on_holidays: Set[str] = None
    ) -> List[Dict[str, Any]]:
        if not joined_data:
            return []
        
        if public_holidays is None or add_on_holidays is None:
            self.param_loader.load_all_parameters()
            add_on_holidays = self.param_loader.get_add_on_holidays()
            public_holidays = load_malaysia_holidays()
        
        return self._calculate_rows(joined_data, public_holidays, add_on_holidays)
    
    def _calculate_rows(
        self, 
        joined_data: List[Dict[str, Any]],
        public_holidays: Set[str],
        add_on_holidays: Set[str]
    ) -> List[Dict[str, Any]]:
        df = pd.DataFrame(joined_data)
        
        grouped = df.groupby(['kira_date', 'pg_account_label', 'channel']).agg({
            'kira_amount': 'sum',
            'kira_mdr': 'sum',
            'kira_settlement_amount': 'sum',
            'pg_amount': 'sum',
            'transaction_id': 'count',
            'transaction_type': 'first',
            'platform': 'first',
            'kira_merchant': 'first'
        }).reset_index()
        
        grouped = grouped.rename(columns={'transaction_id': 'transaction_count'})
        
        fee_map = self._load_fee_map()
        
        rows = []
        
        for _, row in grouped.iterrows():
            kira_date = row['kira_date']
            account_label = row['pg_account_label']
            merchant = row['kira_merchant']
            transaction_type = row['transaction_type']
            channel = row['channel']
            
            settlement_rule = self.param_loader.get_settlement_rule(transaction_type)
            
            settlement_date = calculate_settlement_date(
                kira_date,
                settlement_rule,
                public_holidays,
                add_on_holidays
            )
            
            kira_amount = row['kira_amount'] or 0
            pg_amount = row['pg_amount'] or 0
            daily_variance = kira_amount - pg_amount
            
            fee_key = (merchant, kira_date, self._normalize_channel(channel))
            fee_record = fee_map.get(fee_key)
            
            fee_rate = fee_record.fee_rate if fee_record else None
            remarks = fee_record.remarks if fee_record else None
            
            fees = 0
            if fee_rate is not None:
                fees = self._r(pg_amount * fee_rate / 100)
            
            settlement_amount = self._r(pg_amount - fees) if fees else None
            
            rows.append({
                'pg_merchant': account_label,
                'channel': channel,
                'kira_amount': self._r(kira_amount),
                'mdr': self._r(row['kira_mdr'] or 0),
                'kira_settlement_amount': self._r(row['kira_settlement_amount'] or 0),
                'pg_date': kira_date,
                'amount_pg': self._r(pg_amount),
                'transaction_count': int(row['transaction_count']),
                'settlement_rule': settlement_rule,
                'settlement_date': settlement_date,
                'fee_rate': fee_rate,
                'fees': fees,
                'settlement_amount': settlement_amount,
                'daily_variance': self._r(daily_variance),
                'cumulative_variance': 0,
                'remarks': remarks
            })
        
        rows.sort(key=lambda x: (x['pg_date'], x['pg_merchant'], x['channel']))
        
        cumulative = 0
        for row in rows:
            cumulative += row['daily_variance'] or 0
            row['cumulative_variance'] = self._r(cumulative)
        
        return rows
    
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
            from src.sheets.transactions import get_all_joined_transactions
            joined_data = get_all_joined_transactions()
            
            pg_to_merchant = {}
            for tx in joined_data:
                key = (tx['pg_account_label'], tx['kira_date'][:10], self._normalize_channel(tx['channel']))
                if key not in pg_to_merchant:
                    pg_to_merchant[key] = tx['kira_merchant']
            
            for row in manual_data:
                pg_merchant = row.get('pg_merchant')
                date = row.get('pg_date')
                channel = self._normalize_channel(row.get('channel'))
                
                lookup_key = (pg_merchant, date, channel)
                merchant = pg_to_merchant.get(lookup_key)
                
                if not merchant:
                    continue
                
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
