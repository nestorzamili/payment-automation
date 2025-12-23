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


class DepositService:
    
    def __init__(self, sheets_client: SheetsClient = None):
        self.sheets_client = sheets_client or SheetsClient()
        self.param_loader = ParameterLoader(self.sheets_client)
    
    def generate_deposit(self, from_date: str, to_date: str) -> pd.DataFrame:
        logger.info(f"Generating deposit for {from_date} to {to_date}")
        
        self.param_loader.load_all_parameters()
        add_on_holidays = self.param_loader.get_add_on_holidays()
        public_holidays = load_malaysia_holidays()
        
        joined_data = self._get_joined_transactions(from_date, to_date)
        
        if not joined_data:
            logger.warning("No transaction data found for the date range")
            return pd.DataFrame()
        
        deposit_rows = self._calculate_deposit_rows(
            joined_data, 
            public_holidays, 
            add_on_holidays
        )
        
        self._init_merchant_ledger(deposit_rows)
        
        df = pd.DataFrame(deposit_rows)
        
        logger.info(f"Generated {len(df)} deposit rows")
        return df
    
    def _init_merchant_ledger(self, deposit_rows: List[Dict[str, Any]]):
        try:
            from src.sheets.merchant_ledger import MerchantLedgerService
            ledger_service = MerchantLedgerService(self.sheets_client)
            count = ledger_service.init_from_deposit(deposit_rows)
            logger.info(f"Initialized {count} merchant ledger rows")
        except Exception as e:
            logger.error(f"Failed to init merchant ledger: {e}")
    
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
                    'kira_merchant': kira.merchant,
                    'kira_payment_method': kira.payment_method,
                    'kira_date': kira.transaction_date[:10],
                    'pg_account_label': pg.account_label
                })
            
            logger.info(f"Found {len(joined_data)} joined transactions")
            return joined_data
            
        finally:
            session.close()
    
    def _normalize_channel(self, payment_method: str) -> str:
        if not payment_method:
            return 'EWALLET'
        
        pm_upper = payment_method.upper().strip()
        
        if pm_upper == 'FPX' or 'FPX B2C' in pm_upper:
            return 'FPX'
        if pm_upper == 'FPXC' or 'FPX B2B' in pm_upper:
            return 'FPXC'
        
        return 'EWALLET'
    
    def _calculate_deposit_rows(
        self, 
        joined_data: List[Dict[str, Any]],
        public_holidays: Set[str],
        add_on_holidays: Set[str]
    ) -> List[Dict[str, Any]]:
        
        df = pd.DataFrame(joined_data)
        
        df['channel'] = df['kira_payment_method'].apply(self._normalize_channel)
        
        grouped = df.groupby(['kira_date', 'kira_merchant', 'channel', 'pg_account_label']).agg({
            'kira_amount': 'sum',
            'transaction_id': 'count'
        }).reset_index()
        
        grouped = grouped.rename(columns={'transaction_id': 'transaction_count'})
        
        deposit_rows = []
        
        for _, row in grouped.iterrows():
            kira_date = row['kira_date']
            merchant = row['kira_merchant']
            channel = row['channel']
            pg_merchant = row['pg_account_label']
            kira_amount = row['kira_amount']
            
            settlement_rule = self.param_loader.get_settlement_rule(channel.lower())
            
            settlement_date = calculate_settlement_date(
                kira_date,
                settlement_rule,
                public_holidays,
                add_on_holidays
            )
            
            kira_date_parsed = datetime.strptime(kira_date, '%Y-%m-%d')
            fee_percent = self.param_loader.get_deposit_fee(
                kira_date_parsed.year,
                kira_date_parsed.month,
                merchant,
                channel
            )
            
            fees = round(kira_amount * (fee_percent / 100), 2)
            gross_amount = round(kira_amount - fees, 2)
            
            deposit_rows.append({
                'Merchant': merchant,
                'Channel': channel,
                'PG Merchant': pg_merchant,
                'Transaction Date': kira_date,
                'Settlement Rule': settlement_rule,
                'Settlement Date': settlement_date,
                'Kira Amount': round(kira_amount, 2),
                'Fees': fees,
                'Gross Amount (Deposit)': gross_amount
            })
        
        deposit_rows.sort(key=lambda x: (x['Settlement Date'], x['Merchant'], x['Channel']))
        
        return deposit_rows
    
    def upload_to_sheet(self, df: pd.DataFrame, sheet_name: str = None) -> Dict[str, Any]:
        from src.core.loader import load_settings
        
        if sheet_name is None:
            settings = load_settings()
            sheet_name = settings['google_sheets']['sheets']['deposit']
        
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
