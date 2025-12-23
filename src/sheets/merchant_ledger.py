import pandas as pd
from typing import List, Dict, Any
from datetime import datetime

from sqlalchemy import and_

from src.core.database import get_session
from src.core.models import MerchantLedger, KiraTransaction, PGTransaction
from src.core.logger import get_logger
from src.sheets.client import SheetsClient
from src.sheets.parameters import ParameterLoader
from src.utils.holiday import load_malaysia_holidays, calculate_settlement_date

logger = get_logger(__name__)


class MerchantLedgerService:
    
    def __init__(self, sheets_client: SheetsClient = None):
        self.sheets_client = sheets_client or SheetsClient()
        self.param_loader = ParameterLoader(self.sheets_client)
    
    def init_from_deposit(self, deposit_rows: List[Dict[str, Any]]) -> int:
        if not deposit_rows:
            return 0
        
        aggregated = self._aggregate_deposit_data(deposit_rows)
        return self._upsert_ledger_rows(aggregated, deposit_rows)
    
    def _aggregate_deposit_data(self, deposit_rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        result = {}
        
        for row in deposit_rows:
            merchant = row['Merchant']
            date = row['Transaction Date']
            channel = row['Channel'].upper()
            amount = row['Kira Amount']
            fee = row['Fees']
            gross = row['Gross Amount (Deposit)']
            
            key = (merchant, date)
            if key not in result:
                result[key] = {
                    'merchant': merchant,
                    'transaction_date': date,
                    'fpx': 0, 'fee_fpx': 0, 'gross_fpx': 0,
                    'ewallet': 0, 'fee_ewallet': 0, 'gross_ewallet': 0
                }
            
            if channel in ('FPX', 'FPXC'):
                result[key]['fpx'] += amount
                result[key]['fee_fpx'] += fee
                result[key]['gross_fpx'] += gross
            else:
                result[key]['ewallet'] += amount
                result[key]['fee_ewallet'] += fee
                result[key]['gross_ewallet'] += gross
        
        return result
    
    def _calc_cumulative_by_settlement(
        self, 
        deposit_rows: List[Dict[str, Any]], 
        merchant: str, 
        as_of_date: str,
        include_channels: set = None,
        exclude_channels: set = None
    ) -> float:
        total = 0.0
        for row in deposit_rows:
            if row['Merchant'] != merchant:
                continue
            
            channel = row['Channel'].upper()
            if include_channels and channel not in include_channels:
                continue
            if exclude_channels and channel in exclude_channels:
                continue
            
            settlement_date = row['Settlement Date']
            if settlement_date and settlement_date <= as_of_date:
                total += row['Gross Amount (Deposit)']
        
        return total
    
    def _upsert_ledger_rows(self, aggregated: Dict, deposit_rows: List[Dict[str, Any]]) -> int:
        session = get_session()
        count = 0
        
        try:
            sorted_keys = sorted(aggregated.keys(), key=lambda x: (x[0], x[1]))
            
            for key in sorted_keys:
                data = aggregated[key]
                merchant = data['merchant']
                transaction_date = data['transaction_date']
                
                total_gross = data['gross_fpx'] + data['gross_ewallet']
                total_fee = data['fee_fpx'] + data['fee_ewallet']
                
                cum_fpx = self._calc_cumulative_by_settlement(
                    deposit_rows, merchant, transaction_date, 
                    include_channels={'FPX', 'FPXC'}
                )
                cum_ewallet = self._calc_cumulative_by_settlement(
                    deposit_rows, merchant, transaction_date, 
                    exclude_channels={'FPX', 'FPXC'}
                )
                cum_total = cum_fpx + cum_ewallet
                
                existing = session.query(MerchantLedger).filter(
                    and_(
                        MerchantLedger.merchant == merchant,
                        MerchantLedger.transaction_date == transaction_date
                    )
                ).first()
                
                if existing:
                    existing.fpx = data['fpx']
                    existing.fee_fpx = data['fee_fpx']
                    existing.gross_fpx = data['gross_fpx']
                    existing.ewallet = data['ewallet']
                    existing.fee_ewallet = data['fee_ewallet']
                    existing.gross_ewallet = data['gross_ewallet']
                    existing.total_gross = total_gross
                    existing.total_fee = total_fee
                    existing.cum_fpx = cum_fpx
                    existing.cum_ewallet = cum_ewallet
                    existing.cum_total = cum_total
                else:
                    new_record = MerchantLedger(
                        merchant=merchant,
                        transaction_date=transaction_date,
                        fpx=data['fpx'],
                        fee_fpx=data['fee_fpx'],
                        gross_fpx=data['gross_fpx'],
                        ewallet=data['ewallet'],
                        fee_ewallet=data['fee_ewallet'],
                        gross_ewallet=data['gross_ewallet'],
                        total_gross=total_gross,
                        total_fee=total_fee,
                        cum_fpx=cum_fpx,
                        cum_ewallet=cum_ewallet,
                        cum_total=cum_total
                    )
                    session.add(new_record)
                
                count += 1
            
            session.commit()
            logger.info(f"Upserted {count} merchant ledger rows")
            return count
            
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to upsert merchant ledger: {e}")
            raise
        finally:
            session.close()
    
    def get_ledger(self, merchant: str, year: int, month: int) -> List[Dict[str, Any]]:
        session = get_session()
        
        try:
            date_prefix = f"{year}-{month:02d}"
            
            records = session.query(MerchantLedger).filter(
                and_(
                    MerchantLedger.merchant == merchant,
                    MerchantLedger.transaction_date.like(f"{date_prefix}%")
                )
            ).order_by(MerchantLedger.transaction_date).all()
            
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
            for row in manual_data:
                ledger_id = row.get('id')
                if not ledger_id:
                    continue
                
                existing = session.query(MerchantLedger).filter(
                    MerchantLedger.merchant_ledger_id == ledger_id
                ).first()
                
                if existing:
                    val = self._to_float(row.get('settlement_fund'))
                    if val is not None:
                        existing.settlement_fund = val
                    
                    val = self._to_float(row.get('settlement_charges'))
                    if val is not None:
                        existing.settlement_charges = val
                    
                    val = self._to_float(row.get('withdrawal_amount'))
                    if val is not None:
                        existing.withdrawal_amount = val
                    
                    val = self._to_float(row.get('withdrawal_charges'))
                    if val is not None:
                        existing.withdrawal_charges = val
                    
                    val = self._to_float(row.get('topup_payout_pool'))
                    if val is not None:
                        existing.topup_payout_pool = val
                    
                    val = self._to_float(row.get('payout_pool_balance'))
                    if val is not None:
                        existing.payout_pool_balance = val
                    
                    val = self._to_float(row.get('available_balance'))
                    if val is not None:
                        existing.available_balance = val
                    
                    val = self._to_float(row.get('total_balance'))
                    if val is not None:
                        existing.total_balance = val
                    
                    remarks = row.get('remarks')
                    if remarks and isinstance(remarks, str) and remarks.strip():
                        existing.remarks = remarks.strip()
                    
                    count += 1
            
            session.commit()
            logger.info(f"Updated {count} manual data rows by ID")
            return count
            
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to save manual data: {e}")
            raise
        finally:
            session.close()
    
    def upload_to_sheet(self, data: List[Dict[str, Any]], sheet_name: str = None) -> Dict[str, Any]:
        from src.core.loader import load_settings
        
        if sheet_name is None:
            settings = load_settings()
            sheet_name = settings['google_sheets']['sheets']['merchant_ledger']
        
        if not data:
            return {'success': False, 'error': 'No data to upload'}
        
        try:
            columns = [
                'transaction_date', 'fpx', 'fee_fpx', 'gross_fpx',
                'ewallet', 'fee_ewallet', 'gross_ewallet',
                'total_gross', 'total_fee',
                'cum_fpx', 'cum_ewallet', 'cum_total',
                'settlement_fund', 'settlement_charges',
                'withdrawal_amount', 'withdrawal_charges',
                'topup_payout_pool', 'payout_pool_balance',
                'available_balance', 'total_balance',
                'updated_at', 'remarks'
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
