from typing import Dict, List, Any, Optional
from calendar import monthrange
import re

from sqlalchemy import and_

from src.core.database import get_session
from src.core.models import MerchantLedger, Deposit
from src.core.logger import get_logger
from src.services.client import SheetsClient
from src.utils.helpers import round_decimal, to_float, safe_get_value, parse_period, MONTHS

logger = get_logger(__name__)

MERCHANT_LEDGER_SHEET = 'Merchants Balance & Settlement Ledger'
DATA_START_ROW = 5
DATA_RANGE = 'A5:X50'



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


def _get_previous_month_balance(session, merchant: str, year: int, month: int) -> tuple:
    prev_month = month - 1
    prev_year = year
    if prev_month == 0:
        prev_month = 12
        prev_year = year - 1

    prev_date_prefix = f"{prev_year}-{prev_month:02d}"

    last_record = session.query(MerchantLedger).filter(
        and_(
            MerchantLedger.merchant == merchant,
            MerchantLedger.transaction_date.like(f"{prev_date_prefix}%")
        )
    ).order_by(MerchantLedger.transaction_date.desc()).first()

    if last_record:
        return (
            last_record.payout_pool_balance or 0,
            last_record.available_balance or 0
        )
    return (0, 0)


def _recalculate_balances(session, merchant: str, year: int, month: int):
    date_prefix = f"{year}-{month:02d}"

    prev_payout, prev_available = _get_previous_month_balance(session, merchant, year, month)

    deposits = session.query(Deposit).filter(
        and_(
            Deposit.merchant == merchant,
            Deposit.transaction_date.like(f"{date_prefix}%")
        )
    ).all()
    deposit_map = {d.transaction_date: d for d in deposits}

    rows = session.query(MerchantLedger).filter(
        and_(
            MerchantLedger.merchant == merchant,
            MerchantLedger.transaction_date.like(f"{date_prefix}%")
        )
    ).order_by(MerchantLedger.transaction_date).all()

    for row in rows:
        deposit = deposit_map.get(row.transaction_date)
        available_total = deposit.available_total if deposit else 0

        has_payout_activity = (
            row.withdrawal_amount is not None
            or row.topup_payout_pool is not None
            or prev_payout != 0
        )

        if has_payout_activity:
            row.payout_pool_balance = round_decimal(
                prev_payout
                - (row.withdrawal_amount or 0)
                - (row.withdrawal_charges or 0)
                + (row.topup_payout_pool or 0)
            )
            prev_payout = row.payout_pool_balance
        else:
            row.payout_pool_balance = None

        has_available_activity = (
            row.settlement_fund is not None
            or available_total > 0
            or prev_available != 0
        )

        if has_available_activity:
            row.available_balance = round_decimal(
                prev_available
                + available_total
                - (row.settlement_fund or 0)
                - (row.settlement_charges or 0)
            )
            prev_available = row.available_balance
        else:
            row.available_balance = None

        if row.payout_pool_balance is not None or row.available_balance is not None:
            row.total_balance = round_decimal(
                (row.payout_pool_balance or 0) + (row.available_balance or 0)
            )
        else:
            row.total_balance = None


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

        header_data = client.read_data(MERCHANT_LEDGER_SHEET, 'B1:B2')
        if not header_data or len(header_data) < 2:
            raise ValueError("Merchant or Period not selected")

        merchant = header_data[0][0] if header_data[0] else None
        period_str = header_data[1][0] if header_data[1] else None

        if not merchant:
            raise ValueError("Merchant not selected")
        if not period_str:
            raise ValueError("Period not selected")

        year, month = parse_period(period_str)
        if not year or not month:
            raise ValueError("Invalid period format")
        
        init_merchant_ledger(merchant, year, month)
        
        session = get_session()
        
        try:
            manual_inputs = cls._read_manual_inputs()
            cls._apply_manual_inputs(session, manual_inputs)
            
            _recalculate_balances(session, merchant, year, month)
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
    def _read_manual_inputs(cls) -> List[Dict[str, Any]]:
        client = cls.get_client()
        data = client.read_data(MERCHANT_LEDGER_SHEET, DATA_RANGE)
        
        manual_inputs = []
        for row in data:
            if len(row) < 1 or not row[0]:
                continue
            
            record_id = row[0]
            settlement_fund = safe_get_value(row, 13)
            settlement_charges = safe_get_value(row, 14)
            withdrawal_amount = safe_get_value(row, 15)
            withdrawal_rate = safe_get_value(row, 16)
            topup_payout_pool = safe_get_value(row, 18)
            remarks_val = safe_get_value(row, 23)
            remarks = remarks_val.strip() if remarks_val else None
            
            manual_inputs.append({
                'id': int(record_id),
                'settlement_fund': to_float(settlement_fund),
                'settlement_charges': to_float(settlement_charges),
                'withdrawal_amount': to_float(withdrawal_amount),
                'withdrawal_rate': to_float(withdrawal_rate),
                'topup_payout_pool': to_float(topup_payout_pool),
                'remarks': remarks,
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
                record.withdrawal_charges = round_decimal(record.withdrawal_amount * record.withdrawal_rate / 100)
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
        
        all_dates = set(deposit_map.keys()) | set(ledger_map.keys())
        
        result = []
        for date in sorted(all_dates):
            deposit = deposit_map.get(date)
            ledger = ledger_map.get(date)
            
            if deposit:
                fpx_gross = round_decimal((deposit.fpx_amount or 0) - (deposit.fpx_fee_amount or 0))
                ewallet_gross = round_decimal((deposit.ewallet_amount or 0) - (deposit.ewallet_fee_amount or 0))
                
                result.append({
                    'id': ledger.id if ledger else '',
                    'transaction_date': date,
                    'fpx_amount': deposit.fpx_amount,
                    'fpx_fee': deposit.fpx_fee_amount,
                    'fpx_gross': fpx_gross,
                    'ewallet_amount': deposit.ewallet_amount,
                    'ewallet_fee': deposit.ewallet_fee_amount,
                    'ewallet_gross': ewallet_gross,
                    'total_gross': round_decimal((fpx_gross or 0) + (ewallet_gross or 0)),
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
                    'updated_at': ledger.updated_at if ledger else None,
                })
            elif ledger:
                result.append({
                    'id': ledger.id,
                    'transaction_date': date,
                    'fpx_amount': None,
                    'fpx_fee': None,
                    'fpx_gross': None,
                    'ewallet_amount': None,
                    'ewallet_fee': None,
                    'ewallet_gross': None,
                    'total_gross': None,
                    'total_fee': None,
                    'available_fpx': ledger.available_fpx,
                    'available_ewallet': ledger.available_ewallet,
                    'available_total': ledger.available_total,
                    'settlement_fund': ledger.settlement_fund,
                    'settlement_charges': ledger.settlement_charges,
                    'withdrawal_amount': ledger.withdrawal_amount,
                    'withdrawal_rate': ledger.withdrawal_rate,
                    'withdrawal_charges': ledger.withdrawal_charges,
                    'topup_payout_pool': ledger.topup_payout_pool,
                    'payout_pool_balance': ledger.payout_pool_balance,
                    'available_balance': ledger.available_balance,
                    'total_balance': ledger.total_balance,
                    'remarks': ledger.remarks,
                    'updated_at': ledger.updated_at,
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
                rec.get('settlement_fund') if rec.get('settlement_fund') is not None else '',
                rec.get('settlement_charges') if rec.get('settlement_charges') is not None else '',
                rec.get('withdrawal_amount') if rec.get('withdrawal_amount') is not None else '',
                rec.get('withdrawal_rate') if rec.get('withdrawal_rate') is not None else '',
                rec.get('withdrawal_charges') if rec.get('withdrawal_charges') is not None else '',
                rec.get('topup_payout_pool') if rec.get('topup_payout_pool') is not None else '',
                rec.get('payout_pool_balance') if rec.get('payout_pool_balance') is not None else '',
                rec.get('available_balance') if rec.get('available_balance') is not None else '',
                rec.get('total_balance') if rec.get('total_balance') is not None else '',
                rec.get('updated_at') or '',
                rec.get('remarks') if rec.get('remarks') is not None else '',
            ])
        
        worksheet = client.spreadsheet.worksheet(MERCHANT_LEDGER_SHEET)
        worksheet.batch_clear([DATA_RANGE])
        
        if rows:
            client.write_data(MERCHANT_LEDGER_SHEET, rows, f'A{DATA_START_ROW}')
        
        logger.info(f"Wrote {len(rows)} rows to Merchant Ledger sheet")
