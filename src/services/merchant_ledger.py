from typing import Dict, List, Any, Optional
from calendar import monthrange
import re

from sqlalchemy import and_

from src.core.database import get_session
from src.core.models import MerchantLedger, Deposit
from src.core.logger import get_logger
from src.services.client import SheetsClient
from src.utils.helpers import r, to_float

logger = get_logger(__name__)

MERCHANT_LEDGER_SHEET = 'Merchants Balance & Settlement Ledger'
DATA_START_ROW = 5
DATA_RANGE = 'A5:X50'

MONTHS = {
    'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
    'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
}


def init_merchant_ledger(merchant: str, year: int, month: int):
    session = get_session()
    
    try:
        date_prefix = f"{year}-{month:02d}"
        _, last_day = monthrange(year, month)
        
        existing = session.query(MerchantLedger.transaction_date).filter(
            and_(
                MerchantLedger.merchant == merchant,
                MerchantLedger.transaction_date.like(f"{date_prefix}%")
            )
        ).all()
        
        existing_dates = {rec[0] for rec in existing}
        
        for day in range(1, last_day + 1):
            date_str = f"{year}-{month:02d}-{day:02d}"
            if date_str not in existing_dates:
                session.add(MerchantLedger(merchant=merchant, transaction_date=date_str))
        
        session.commit()
        
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to init merchant ledger: {e}")
        raise
    finally:
        session.close()


def _recalculate_balances(session, merchant: str):
    rows = session.query(MerchantLedger).filter(
        MerchantLedger.merchant == merchant
    ).order_by(MerchantLedger.transaction_date).all()
    
    prev_topup_payout_pool = 0
    prev_available_balance = 0
    
    for row in rows:
        available_total = row.available_total or 0
        
        has_payout_activity = (
            row.withdrawal_amount is not None 
            or row.topup_payout_pool is not None
            or prev_topup_payout_pool != 0
        )
        
        if has_payout_activity:
            row.payout_pool_balance = r(
                prev_topup_payout_pool
                - (row.withdrawal_amount or 0)
                - (row.withdrawal_charges or 0)
                + (row.topup_payout_pool or 0)
            )
        else:
            row.payout_pool_balance = None
        
        has_available_activity = (
            row.settlement_fund is not None
            or available_total > 0
            or prev_available_balance != 0
        )
        
        if has_available_activity:
            row.available_balance = r(
                prev_available_balance
                + available_total
                - (row.settlement_fund or 0)
                - (row.settlement_charges or 0)
            )
        else:
            row.available_balance = None
        
        if row.payout_pool_balance is not None or row.available_balance is not None:
            row.total_balance = r(
                (row.payout_pool_balance or 0) + (row.available_balance or 0)
            )
        else:
            row.total_balance = None
        
        prev_topup_payout_pool = row.payout_pool_balance if row.payout_pool_balance is not None else prev_topup_payout_pool
        prev_available_balance = row.available_balance if row.available_balance is not None else prev_available_balance


def list_merchants() -> List[str]:
    from src.core.models import KiraTransaction
    session = get_session()
    try:
        merchants = session.query(KiraTransaction.merchant).distinct().all()
        return sorted([m[0] for m in merchants if m[0]])
    finally:
        session.close()


def list_periods() -> List[str]:
    from sqlalchemy import func
    from src.core.models import KiraTransaction
    
    session = get_session()
    try:
        results = session.query(
            func.substr(KiraTransaction.transaction_date, 1, 7).label('ym')
        ).distinct().all()
        
        periods = []
        for rec in results:
            if rec.ym:
                year = rec.ym[:4]
                month_num = int(rec.ym[5:7])
                month_names = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 
                              'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
                periods.append(f"{month_names[month_num-1]} {year}")
        
        periods.sort(key=lambda x: (x.split()[1], MONTHS[x.split()[0]]))
        return periods
    finally:
        session.close()


class MerchantLedgerSheetService:
    _client: Optional[SheetsClient] = None
    
    @classmethod
    def get_client(cls) -> SheetsClient:
        if cls._client is None:
            cls._client = SheetsClient()
        return cls._client
    
    @classmethod
    def sync_sheet(cls) -> int:
        client = cls.get_client()
        
        merchant_value = client.read_data(MERCHANT_LEDGER_SHEET, 'B1')
        if not merchant_value or not merchant_value[0]:
            raise ValueError("Merchant not selected")
        merchant = merchant_value[0][0]
        
        period_value = client.read_data(MERCHANT_LEDGER_SHEET, 'B2')
        if not period_value or not period_value[0]:
            raise ValueError("Period not selected")
        
        year, month = cls._parse_period(period_value[0][0])
        if not year or not month:
            raise ValueError("Invalid period format")
        
        session = get_session()
        
        try:
            manual_inputs = cls._read_manual_inputs()
            cls._apply_manual_inputs(session, manual_inputs)
            
            _recalculate_balances(session, merchant)
            session.commit()
            
            data = cls._get_ledger_data(session, merchant, year, month)
            cls._write_to_sheet(data)
            
            return len(data)
            
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to sync Merchant Ledger sheet: {e}")
            raise
        finally:
            session.close()
    
    @classmethod
    def _parse_period(cls, period_str: str) -> tuple:
        if not period_str:
            return None, None
        
        match = re.match(r'(\w+)\s+(\d{4})', str(period_str))
        if not match:
            return None, None
        
        month_name = match.group(1)
        year = int(match.group(2))
        month = MONTHS.get(month_name)
        
        return year, month
    
    @classmethod
    def _read_manual_inputs(cls) -> List[Dict[str, Any]]:
        client = cls.get_client()
        data = client.read_data(MERCHANT_LEDGER_SHEET, DATA_RANGE)
        
        manual_inputs = []
        for row in data:
            if len(row) < 1 or not row[0]:
                continue
            
            record_id = row[0]
            settlement_fund = row[13] if len(row) > 13 else ''
            settlement_charges = row[14] if len(row) > 14 else ''
            withdrawal_amount = row[15] if len(row) > 15 else ''
            withdrawal_rate = row[16] if len(row) > 16 else ''
            topup_payout_pool = row[18] if len(row) > 18 else ''
            remarks = row[23].strip() if len(row) > 23 and row[23] else ''
            
            manual_inputs.append({
                'id': int(record_id),
                'settlement_fund': to_float(settlement_fund) if settlement_fund else None,
                'settlement_charges': to_float(settlement_charges) if settlement_charges else None,
                'withdrawal_amount': to_float(withdrawal_amount) if withdrawal_amount else None,
                'withdrawal_rate': to_float(withdrawal_rate) if withdrawal_rate else None,
                'topup_payout_pool': to_float(topup_payout_pool) if topup_payout_pool else None,
                'remarks': remarks if remarks else None,
            })
        
        return manual_inputs
    
    @classmethod
    def _apply_manual_inputs(cls, session, manual_inputs: List[Dict]) -> int:
        if not manual_inputs:
            return 0
        
        ids = [m['id'] for m in manual_inputs]
        records = session.query(MerchantLedger).filter(MerchantLedger.id.in_(ids)).all()
        records_by_id = {rec.id: rec for rec in records}
        
        count = 0
        for input_data in manual_inputs:
            record = records_by_id.get(input_data['id'])
            if not record:
                continue
            
            record.settlement_fund = input_data['settlement_fund']
            record.settlement_charges = input_data['settlement_charges']
            record.withdrawal_amount = input_data['withdrawal_amount']
            record.withdrawal_rate = input_data['withdrawal_rate']
            
            if record.withdrawal_amount and record.withdrawal_rate:
                record.withdrawal_charges = r(record.withdrawal_amount * record.withdrawal_rate / 100)
            else:
                record.withdrawal_charges = None
            
            record.topup_payout_pool = input_data['topup_payout_pool']
            record.remarks = input_data['remarks']
            
            count += 1
        
        logger.info(f"Applied {count} manual inputs to Merchant Ledger")
        return count
    
    @classmethod
    def _get_ledger_data(cls, session, merchant: str, year: int, month: int) -> List[Dict]:
        date_prefix = f"{year}-{month:02d}"
        
        deposits = session.query(Deposit).filter(
            and_(
                Deposit.merchant == merchant,
                Deposit.transaction_date.like(f"{date_prefix}%")
            )
        ).order_by(Deposit.transaction_date).all()
        
        deposit_map = {d.transaction_date: d for d in deposits}
        
        ledgers = session.query(MerchantLedger).filter(
            and_(
                MerchantLedger.merchant == merchant,
                MerchantLedger.transaction_date.like(f"{date_prefix}%")
            )
        ).order_by(MerchantLedger.transaction_date).all()
        
        ledger_map = {lg.transaction_date: lg for lg in ledgers}
        
        for ledger in ledgers:
            deposit = deposit_map.get(ledger.transaction_date)
            if deposit:
                ledger.available_fpx = deposit.available_fpx
                ledger.available_ewallet = deposit.available_ewallet
                ledger.available_total = deposit.available_total
        
        result = []
        for deposit in deposits:
            date = deposit.transaction_date
            ledger = ledger_map.get(date)
            
            fpx_gross = r((deposit.fpx_amount or 0) - (deposit.fpx_fee_amount or 0))
            ewallet_gross = r((deposit.ewallet_amount or 0) - (deposit.ewallet_fee_amount or 0))
            
            result.append({
                'id': ledger.id if ledger else '',
                'transaction_date': date,
                'fpx_amount': deposit.fpx_amount,
                'fpx_fee': deposit.fpx_fee_amount,
                'fpx_gross': fpx_gross,
                'ewallet_amount': deposit.ewallet_amount,
                'ewallet_fee': deposit.ewallet_fee_amount,
                'ewallet_gross': ewallet_gross,
                'total_gross': r((fpx_gross or 0) + (ewallet_gross or 0)),
                'total_fee': deposit.total_fees,
                'available_fpx': deposit.available_fpx,
                'available_ewallet': deposit.available_ewallet,
                'available_total': deposit.available_total,
                'settlement_fund': ledger.settlement_fund if ledger else None,
                'settlement_charges': ledger.settlement_charges if ledger else None,
                'withdrawal_amount': ledger.withdrawal_amount if ledger else None,
                'withdrawal_rate': ledger.withdrawal_rate if ledger else None,
                'withdrawal_charges': ledger.withdrawal_charges if ledger else None,
                'topup_payout_pool': ledger.topup_payout_pool if ledger else None,
                'payout_pool_balance': ledger.payout_pool_balance if ledger else None,
                'available_balance': ledger.available_balance if ledger else None,
                'total_balance': ledger.total_balance if ledger else None,
                'remarks': ledger.remarks if ledger else deposit.remarks,
            })
        
        return result
    
    @classmethod
    def _write_to_sheet(cls, data: List[Dict]):
        client = cls.get_client()
        
        rows = []
        for rec in data:
            rows.append([
                rec.get('id', ''),
                rec.get('transaction_date', ''),
                rec.get('fpx_amount') if rec.get('fpx_amount') is not None else 0,
                rec.get('fpx_fee') if rec.get('fpx_fee') is not None else 0,
                rec.get('fpx_gross') if rec.get('fpx_gross') is not None else 0,
                rec.get('ewallet_amount') if rec.get('ewallet_amount') is not None else 0,
                rec.get('ewallet_fee') if rec.get('ewallet_fee') is not None else 0,
                rec.get('ewallet_gross') if rec.get('ewallet_gross') is not None else 0,
                rec.get('total_gross') if rec.get('total_gross') is not None else 0,
                rec.get('total_fee') if rec.get('total_fee') is not None else 0,
                rec.get('available_fpx') if rec.get('available_fpx') is not None else 0,
                rec.get('available_ewallet') if rec.get('available_ewallet') is not None else 0,
                rec.get('available_total') if rec.get('available_total') is not None else 0,
                rec.get('settlement_fund') or '',
                rec.get('settlement_charges') or '',
                rec.get('withdrawal_amount') or '',
                rec.get('withdrawal_rate') or '',
                rec.get('withdrawal_charges') or '',
                rec.get('topup_payout_pool') or '',
                rec.get('payout_pool_balance') or '',
                rec.get('available_balance') or '',
                rec.get('total_balance') or '',
                '',
                rec.get('remarks') or '',
            ])
        
        worksheet = client.spreadsheet.worksheet(MERCHANT_LEDGER_SHEET)
        worksheet.batch_clear([DATA_RANGE])
        
        if rows:
            client.write_data(MERCHANT_LEDGER_SHEET, rows, f'A{DATA_START_ROW}')
        
        logger.info(f"Wrote {len(rows)} rows to Merchant Ledger sheet")
