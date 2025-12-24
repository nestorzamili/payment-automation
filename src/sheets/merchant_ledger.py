from typing import List, Dict, Any
from sqlalchemy import and_

from src.core.database import get_session
from src.core.models import MerchantLedger
from src.core.logger import get_logger
from src.sheets.client import SheetsClient
from src.sheets.parameters import ParameterLoader

logger = get_logger(__name__)


class MerchantLedgerService:
    
    def __init__(self, sheets_client: SheetsClient = None):
        self.sheets_client = sheets_client or SheetsClient()
        self.param_loader = ParameterLoader(self.sheets_client)
    
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
    
    def _build_settlement_map(self, deposit_rows: List[Dict[str, Any]]) -> Dict:
        settlement_map = {}
        
        for row in deposit_rows:
            merchant = row['Merchant']
            settlement_date = row['Settlement Date']
            channel = row['Channel'].upper()
            gross = row['Gross Amount (Deposit)']
            
            channel_type = 'fpx' if channel in ('FPX', 'FPXC') else 'ewallet'
            key = (merchant, settlement_date, channel_type)
            
            if key not in settlement_map:
                settlement_map[key] = 0
            settlement_map[key] += gross
        
        return settlement_map
    
    def _upsert_ledger_rows(self, aggregated: Dict, settlement_map: Dict) -> int:
        session = get_session()
        count = 0
        
        try:
            sorted_keys = sorted(aggregated.keys(), key=lambda x: (x[0], x[1]))
            
            existing_keys = [(k[0], k[1]) for k in sorted_keys]
            existing_records = {}
            
            for merchant, date in existing_keys:
                record = session.query(MerchantLedger).filter(
                    and_(
                        MerchantLedger.merchant == merchant,
                        MerchantLedger.transaction_date == date
                    )
                ).first()
                if record:
                    existing_records[(merchant, date)] = record
            
            for key in sorted_keys:
                data = aggregated[key]
                merchant = data['merchant']
                transaction_date = data['transaction_date']
                
                total_gross = self._r(data['gross_fpx'] + data['gross_ewallet'])
                total_fee = self._r(data['fee_fpx'] + data['fee_ewallet'])
                
                available_fpx = self._r(settlement_map.get((merchant, transaction_date, 'fpx'), 0))
                available_ewallet = self._r(settlement_map.get((merchant, transaction_date, 'ewallet'), 0))
                available_total = self._r((available_fpx or 0) + (available_ewallet or 0))
                
                existing = existing_records.get((merchant, transaction_date))
                
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
            
            affected_merchants = set(data['merchant'] for data in aggregated.values())
            for merchant in affected_merchants:
                self._recalculate_balances(session, merchant)
            
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
            valid_ids = []
            for row in manual_data:
                ledger_id = row.get('id')
                if ledger_id and isinstance(ledger_id, (int, float)):
                    valid_ids.append(int(ledger_id))
            
            if not valid_ids:
                return 0
            
            records = session.query(MerchantLedger).filter(
                MerchantLedger.merchant_ledger_id.in_(valid_ids)
            ).all()
            
            records_by_id = {r.merchant_ledger_id: r for r in records}
            manual_by_id = {int(row['id']): row for row in manual_data if row.get('id')}
            
            merchants_to_recalc = set()
            
            for ledger_id in valid_ids:
                existing = records_by_id.get(ledger_id)
                row = manual_by_id.get(ledger_id)
                
                if not existing or not row:
                    continue
                
                val = row.get('settlement_fund')
                if val == 'CLEAR':
                    existing.settlement_fund = None
                    merchants_to_recalc.add(existing.merchant)
                elif val is not None:
                    existing.settlement_fund = self._to_float(val)
                    merchants_to_recalc.add(existing.merchant)
                
                val = row.get('settlement_charges')
                if val == 'CLEAR':
                    existing.settlement_charges = None
                    merchants_to_recalc.add(existing.merchant)
                elif val is not None:
                    existing.settlement_charges = self._to_float(val)
                    merchants_to_recalc.add(existing.merchant)
                
                val = row.get('withdrawal_amount')
                if val == 'CLEAR':
                    existing.withdrawal_amount = None
                    existing.withdrawal_charges = None
                    merchants_to_recalc.add(existing.merchant)
                elif val is not None:
                    withdrawal_amount = self._to_float(val)
                    if withdrawal_amount is not None:
                        existing.withdrawal_amount = withdrawal_amount
                        
                        year = int(existing.transaction_date[:4])
                        month = int(existing.transaction_date[5:7])
                        if not self.param_loader._loaded:
                            self.param_loader.load_all_parameters()
                        rate = self.param_loader.get_withdrawal_rate(year, month, existing.merchant)
                        existing.withdrawal_charges = self._r(withdrawal_amount * rate)
                        merchants_to_recalc.add(existing.merchant)
                
                val = row.get('topup_payout_pool')
                if val == 'CLEAR':
                    existing.topup_payout_pool = None
                    merchants_to_recalc.add(existing.merchant)
                elif val is not None:
                    existing.topup_payout_pool = self._to_float(val)
                    merchants_to_recalc.add(existing.merchant)
                
                remarks = row.get('remarks')
                if remarks == 'CLEAR':
                    existing.remarks = None
                elif remarks and isinstance(remarks, str) and remarks.strip():
                    existing.remarks = remarks.strip()
                
                count += 1
            
            for merchant in merchants_to_recalc:
                self._recalculate_balances(session, merchant)
            
            session.commit()
            logger.info(f"Updated {count} manual data rows by ID")
            return count
            
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to save manual data: {e}")
            raise
        finally:
            session.close()
    
    def _recalculate_balances(self, session, merchant: str):
        rows = session.query(MerchantLedger).filter(
            MerchantLedger.merchant == merchant
        ).order_by(MerchantLedger.transaction_date).all()
        
        prev_payout_pool = 0
        prev_available = 0
        
        for row in rows:
            has_payout_activity = (
                row.withdrawal_amount is not None 
                or row.topup_payout_pool is not None
                or prev_payout_pool != 0
            )
            
            if has_payout_activity:
                row.payout_pool_balance = self._r(
                    prev_payout_pool
                    - (row.withdrawal_amount or 0)
                    - (row.withdrawal_charges or 0)
                    + (row.topup_payout_pool or 0)
                )
            else:
                row.payout_pool_balance = None
            
            has_available_activity = (
                row.settlement_fund is not None
                or row.available_settlement_amount_total
                or prev_available != 0
            )
            
            if has_available_activity:
                row.available_balance = self._r(
                    prev_available
                    + (row.available_settlement_amount_total or 0)
                    - (row.settlement_fund or 0)
                    - (row.settlement_charges or 0)
                )
            else:
                row.available_balance = None
            
            if row.payout_pool_balance is not None or row.available_balance is not None:
                row.total_balance = self._r(
                    (row.payout_pool_balance or 0) + (row.available_balance or 0)
                )
            else:
                row.total_balance = None
            
            prev_payout_pool = row.payout_pool_balance if row.payout_pool_balance is not None else prev_payout_pool
            prev_available = row.available_balance if row.available_balance is not None else prev_available
    
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
