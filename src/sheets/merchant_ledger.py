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
    
    def _calc_available_settlement(
        self, 
        deposit_rows: List[Dict[str, Any]], 
        merchant: str, 
        settlement_date: str,
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
            
            row_settlement_date = row['Settlement Date']
            if row_settlement_date and row_settlement_date == settlement_date:
                total += row['Gross Amount (Deposit)']
        
        return total
    
    def _r(self, value):
        return round(value, 2) if value is not None else 0
    
    def _upsert_ledger_rows(self, aggregated: Dict, deposit_rows: List[Dict[str, Any]]) -> int:
        session = get_session()
        count = 0
        
        try:
            sorted_keys = sorted(aggregated.keys(), key=lambda x: (x[0], x[1]))
            
            for key in sorted_keys:
                data = aggregated[key]
                merchant = data['merchant']
                transaction_date = data['transaction_date']
                
                total_gross = self._r(data['gross_fpx'] + data['gross_ewallet'])
                total_fee = self._r(data['fee_fpx'] + data['fee_ewallet'])
                
                available_fpx = self._r(self._calc_available_settlement(
                    deposit_rows, merchant, transaction_date, 
                    include_channels={'FPX', 'FPXC'}
                ))
                available_ewallet = self._r(self._calc_available_settlement(
                    deposit_rows, merchant, transaction_date, 
                    exclude_channels={'FPX', 'FPXC'}
                ))
                available_total = self._r(available_fpx + available_ewallet)
                
                existing = session.query(MerchantLedger).filter(
                    and_(
                        MerchantLedger.merchant == merchant,
                        MerchantLedger.transaction_date == transaction_date
                    )
                ).first()
                
                if existing:
                    existing.fpx = self._r(data['fpx'])
                    existing.fee_fpx = self._r(data['fee_fpx'])
                    existing.gross_fpx = self._r(data['gross_fpx'])
                    existing.ewallet = self._r(data['ewallet'])
                    existing.fee_ewallet = self._r(data['fee_ewallet'])
                    existing.gross_ewallet = self._r(data['gross_ewallet'])
                    existing.total_gross = total_gross
                    existing.total_fee = total_fee
                    existing.available_settlement_amount_fpx = available_fpx
                    existing.available_settlement_amount_ewallet = available_ewallet
                    existing.available_settlement_amount_total = available_total
                else:
                    new_record = MerchantLedger(
                        merchant=merchant,
                        transaction_date=transaction_date,
                        fpx=self._r(data['fpx']),
                        fee_fpx=self._r(data['fee_fpx']),
                        gross_fpx=self._r(data['gross_fpx']),
                        ewallet=self._r(data['ewallet']),
                        fee_ewallet=self._r(data['fee_ewallet']),
                        gross_ewallet=self._r(data['gross_ewallet']),
                        total_gross=total_gross,
                        total_fee=total_fee,
                        available_settlement_amount_fpx=available_fpx,
                        available_settlement_amount_ewallet=available_ewallet,
                        available_settlement_amount_total=available_total
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
                    # Update manual input fields
                    val = self._to_float(row.get('settlement_fund'))
                    if val is not None:
                        existing.settlement_fund = val
                    
                    val = self._to_float(row.get('settlement_charges'))
                    if val is not None:
                        existing.settlement_charges = val
                    
                    withdrawal_amount = self._to_float(row.get('withdrawal_amount'))
                    if withdrawal_amount is not None:
                        existing.withdrawal_amount = withdrawal_amount
                        
                        year = int(existing.transaction_date[:4])
                        month = int(existing.transaction_date[5:7])
                        rate = self.param_loader.get_withdrawal_rate(year, month, existing.merchant)
                        existing.withdrawal_charges = withdrawal_amount * rate
                    
                    val = self._to_float(row.get('topup_payout_pool'))
                    if val is not None:
                        existing.topup_payout_pool = val
                    
                    remarks = row.get('remarks')
                    if remarks and isinstance(remarks, str) and remarks.strip():
                        existing.remarks = remarks.strip()
                    
                    prev_row = session.query(MerchantLedger).filter(
                        and_(
                            MerchantLedger.merchant == existing.merchant,
                            MerchantLedger.transaction_date < existing.transaction_date
                        )
                    ).order_by(MerchantLedger.transaction_date.desc()).first()
                    
                    prev_payout_pool = prev_row.payout_pool_balance if prev_row and prev_row.payout_pool_balance else 0
                    prev_available = prev_row.available_balance if prev_row and prev_row.available_balance else 0
                    
                    if existing.withdrawal_amount is not None or existing.topup_payout_pool is not None:
                        existing.payout_pool_balance = self._r(
                            prev_payout_pool
                            - (existing.withdrawal_amount or 0)
                            - (existing.withdrawal_charges or 0)
                            + (existing.topup_payout_pool or 0)
                        )
                    else:
                        existing.payout_pool_balance = None
                    
                    if existing.settlement_fund is not None:
                        existing.available_balance = self._r(
                            prev_available
                            + (existing.available_settlement_amount_total or 0)
                            - (existing.settlement_fund or 0)
                            - (existing.settlement_charges or 0)
                        )
                    else:
                        existing.available_balance = None
                    
                    if existing.payout_pool_balance is not None and existing.available_balance is not None:
                        existing.total_balance = self._r(existing.payout_pool_balance + existing.available_balance)
                    else:
                        existing.total_balance = None
                    
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
                'merchant_ledger_id', 'transaction_date', 
                'fpx', 'fee_fpx', 'gross_fpx',
                'ewallet', 'fee_ewallet', 'gross_ewallet',
                'total_gross', 'total_fee',
                'available_settlement_amount_fpx', 'available_settlement_amount_ewallet', 'available_settlement_amount_total',
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
