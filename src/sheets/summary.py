import pandas as pd
from typing import List, Dict, Any, Set
from datetime import datetime

from src.core.database import get_session
from src.core.models import KiraTransaction, PGTransaction
from src.core.logger import get_logger
from src.sheets.client import SheetsClient
from src.sheets.parameters import ParameterLoader
from src.utils.holiday import load_malaysia_holidays, calculate_settlement_date

logger = get_logger(__name__)


class SummaryService:
    
    def __init__(self, sheets_client: SheetsClient = None):
        self.sheets_client = sheets_client or SheetsClient()
        self.param_loader = ParameterLoader(self.sheets_client)
    
    def generate_summary(self, from_date: str, to_date: str) -> pd.DataFrame:
        logger.info(f"Generating summary for {from_date} to {to_date}")
        
        self.param_loader.load_all_parameters()
        add_on_holidays = self.param_loader.get_add_on_holidays()
        public_holidays = load_malaysia_holidays()
        
        joined_data = self._get_joined_transactions(from_date, to_date)
        
        if not joined_data:
            logger.warning("No transaction data found for the date range")
            return pd.DataFrame()
        
        summary_rows = self._calculate_summary_rows(
            joined_data, 
            public_holidays, 
            add_on_holidays
        )
        
        df = pd.DataFrame(summary_rows)
        
        if 'PG KIRA Daily Variance' in df.columns:
            df['PG KIRA Cumulative Variance'] = df['PG KIRA Daily Variance'].cumsum()
        
        logger.info(f"Generated {len(df)} summary rows")
        return df
    
    def _get_joined_transactions(self, from_date: str, to_date: str) -> List[Dict[str, Any]]:
        session = get_session()
        
        try:
            results = session.query(
                KiraTransaction,
                PGTransaction
            ).join(
                PGTransaction,
                KiraTransaction.transaction_id == PGTransaction.transaction_id
            ).filter(
                KiraTransaction.transaction_date >= from_date,
                KiraTransaction.transaction_date <= to_date + ' 23:59:59'
            ).all()
            
            joined_data = []
            for kira, pg in results:
                joined_data.append({
                    'transaction_id': kira.transaction_id,
                    'kira_amount': kira.amount,
                    'kira_mdr': kira.mdr,
                    'kira_settlement_amount': kira.settlement_amount,
                    'kira_merchant': kira.merchant,
                    'kira_date': kira.transaction_date[:10],
                    'pg_amount': pg.amount,
                    'account_label': pg.account_label,
                    'transaction_type': pg.transaction_type,
                    'channel': pg.channel,
                    'platform': pg.platform
                })
            
            logger.info(f"Found {len(joined_data)} joined transactions")
            return joined_data
            
        finally:
            session.close()
    
    def _calculate_summary_rows(
        self, 
        joined_data: List[Dict[str, Any]],
        public_holidays: Set[str],
        add_on_holidays: Set[str]
    ) -> List[Dict[str, Any]]:
        
        df = pd.DataFrame(joined_data)
        
        grouped = df.groupby(['kira_date', 'account_label', 'channel']).agg({
            'kira_amount': 'sum',
            'kira_mdr': 'sum',
            'kira_settlement_amount': 'sum',
            'pg_amount': 'sum',
            'transaction_id': 'count',
            'transaction_type': 'first',
            'platform': 'first'
        }).reset_index()
        
        grouped = grouped.rename(columns={'transaction_id': 'transaction_count'})
        
        summary_rows = []
        
        for _, row in grouped.iterrows():
            kira_date = row['kira_date']
            account_label = row['account_label']
            transaction_type = row['transaction_type']
            channel = row['channel']
            
            settlement_rule = self.param_loader.get_settlement_rule(transaction_type)
            
            settlement_date = calculate_settlement_date(
                kira_date,
                settlement_rule,
                public_holidays,
                add_on_holidays
            )
            
            kira_date_parsed = datetime.strptime(kira_date, '%Y-%m-%d')
            fee_config = self.param_loader.get_fee_config(
                kira_date_parsed.year,
                kira_date_parsed.month,
                row['platform'],
                channel
            )
            
            fees = self._calculate_fee(
                row['pg_amount'],
                row['transaction_count'],
                fee_config
            )
            
            settlement_amount = row['pg_amount'] - fees
            
            kira_amount = row['kira_amount'] or 0
            pg_amount = row['pg_amount'] or 0
            daily_variance = kira_amount - pg_amount
            
            summary_rows.append({
                'PG Merchant': account_label,
                'Channel': channel,
                'Kira Amount': round(kira_amount, 2),
                'MDR': round(row['kira_mdr'] or 0, 2),
                'KIRA Settlement Amount': round(row['kira_settlement_amount'] or 0, 2),
                'PG Date': kira_date,
                'Amount PG': round(row['pg_amount'], 2),
                'Transaction Count': int(row['transaction_count']),
                'Settlement Rule': settlement_rule,
                'Settlement Date': settlement_date,
                'Fees': round(fees, 2),
                'Settlement Amount': round(settlement_amount, 2),
                'PG KIRA Daily Variance': round(daily_variance, 2),
                'PG KIRA Cumulative Variance': 0
            })
        
        summary_rows.sort(key=lambda x: (x['PG Date'], x['PG Merchant'], x['Channel']))
        
        return summary_rows
    
    def _calculate_fee(
        self, 
        amount_pg: float, 
        tx_count: int, 
        fee_config: Dict[str, Any]
    ) -> float:
        fee_type = fee_config.get('fee_type', 'percent')
        fee_value = fee_config.get('fee_value', 0.0)
        
        if fee_type == 'percent':
            return amount_pg * (fee_value / 100)
        elif fee_type == 'per_order':
            return tx_count * fee_value
        
        return 0.0
    
    def upload_to_sheet(self, df: pd.DataFrame, sheet_name: str = None) -> Dict[str, Any]:
        from src.core.loader import load_settings
        
        if sheet_name is None:
            settings = load_settings()
            sheet_name = settings['google_sheets']['sheets']['summary']
        
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
