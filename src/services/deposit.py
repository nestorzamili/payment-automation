from typing import Dict, List, Any, Optional, Set
from calendar import monthrange
import re

from sqlalchemy import func

from src.core.database import get_session
from src.core.models import Deposit, KiraTransaction
from src.core.logger import get_logger
from src.services.client import SheetsClient
from src.services.parameters import ParameterService
from src.utils.helpers import normalize_channel, r, to_float, calculate_fee
from src.utils.holiday import load_malaysia_holidays, calculate_settlement_date

logger = get_logger(__name__)

DEPOSIT_SHEET = 'Deposit'
DATA_START_ROW = 7
DATA_RANGE = 'A7:X50'

MONTHS = {
    'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
    'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
}


def init_deposit():
    session = get_session()
    
    try:
        add_on_holidays = ParameterService.load_parameters()
        public_holidays = load_malaysia_holidays()
        
        merchants = session.query(KiraTransaction.merchant).distinct().all()
        merchants = [m[0] for m in merchants if m[0]]
        
        year_months = session.query(
            func.substr(KiraTransaction.transaction_date, 1, 7).label('ym')
        ).distinct().all()
        year_months = [ym[0] for ym in year_months if ym[0]]
        
        if not merchants or not year_months:
            return
        
        existing = session.query(Deposit).all()
        existing_map = {(e.merchant, e.transaction_date): e for e in existing}
        
        count = 0
        for ym in sorted(year_months):
            year = int(ym[:4])
            month = int(ym[5:7])
            _, last_day = monthrange(year, month)
            date_prefix = f"{year}-{month:02d}"
            
            for merchant in merchants:
                kira_agg = session.query(
                    func.substr(KiraTransaction.transaction_date, 1, 10).label('tx_date'),
                    KiraTransaction.payment_method,
                    func.sum(KiraTransaction.amount).label('amount'),
                    func.sum(KiraTransaction.settlement_amount).label('settlement_amount'),
                    func.count().label('volume'),
                ).filter(
                    KiraTransaction.merchant == merchant,
                    KiraTransaction.transaction_date.like(f"{date_prefix}%")
                ).group_by(
                    func.substr(KiraTransaction.transaction_date, 1, 10),
                    KiraTransaction.payment_method
                ).all()
                
                tx_map: Dict[tuple, Dict] = {}
                for row in kira_agg:
                    channel = normalize_channel(row.payment_method)
                    key = (row.tx_date, channel)
                    if key not in tx_map:
                        tx_map[key] = {'amount': 0, 'settlement_amount': 0, 'volume': 0}
                    tx_map[key]['amount'] += row.amount or 0
                    tx_map[key]['settlement_amount'] += row.settlement_amount or 0
                    tx_map[key]['volume'] += row.volume or 0
                
                for day in range(1, last_day + 1):
                    date_str = f"{year}-{month:02d}-{day:02d}"
                    
                    fpx_data = tx_map.get((date_str, 'FPX'), {'amount': 0, 'volume': 0})
                    ewallet_data = tx_map.get((date_str, 'EWALLET'), {'amount': 0, 'volume': 0})
                    
                    existing_record = existing_map.get((merchant, date_str))
                    
                    fpx_fee_type = existing_record.fpx_fee_type if existing_record else None
                    fpx_fee_rate = existing_record.fpx_fee_rate if existing_record else None
                    ewallet_fee_type = existing_record.ewallet_fee_type if existing_record else None
                    ewallet_fee_rate = existing_record.ewallet_fee_rate if existing_record else None
                    remarks = existing_record.remarks if existing_record else None
                    
                    fpx_rule = existing_record.fpx_settlement_rule if existing_record else None
                    ewallet_rule = existing_record.ewallet_settlement_rule if existing_record else None
                    
                    fpx_settlement_date = calculate_settlement_date(
                        date_str, fpx_rule, public_holidays, add_on_holidays
                    ) if fpx_rule else None
                    
                    ewallet_settlement_date = calculate_settlement_date(
                        date_str, ewallet_rule, public_holidays, add_on_holidays
                    ) if ewallet_rule else None
                    
                    fpx_fee_amount = calculate_fee(
                        fpx_fee_type, fpx_fee_rate, fpx_data['amount'], fpx_data.get('volume', 0)
                    )
                    ewallet_fee_amount = calculate_fee(
                        ewallet_fee_type, ewallet_fee_rate, ewallet_data['amount'], ewallet_data.get('volume', 0)
                    )
                    
                    record_data = {
                        'merchant': merchant,
                        'transaction_date': date_str,
                        'fpx_amount': r(fpx_data['amount']),
                        'fpx_volume': fpx_data['volume'],
                        'fpx_fee_type': fpx_fee_type,
                        'fpx_fee_rate': fpx_fee_rate,
                        'fpx_fee_amount': fpx_fee_amount,
                        'fpx_gross': r(fpx_data['amount'] - (fpx_fee_amount or 0)),
                        'fpx_settlement_rule': fpx_rule,
                        'fpx_settlement_date': fpx_settlement_date,
                        'ewallet_amount': r(ewallet_data['amount']),
                        'ewallet_volume': ewallet_data['volume'],
                        'ewallet_fee_type': ewallet_fee_type,
                        'ewallet_fee_rate': ewallet_fee_rate,
                        'ewallet_fee_amount': ewallet_fee_amount,
                        'ewallet_gross': r(ewallet_data['amount'] - (ewallet_fee_amount or 0)),
                        'ewallet_settlement_rule': ewallet_rule,
                        'ewallet_settlement_date': ewallet_settlement_date,
                        'total_amount': r(fpx_data['amount'] + ewallet_data['amount']),
                        'total_fees': r((fpx_fee_amount or 0) + (ewallet_fee_amount or 0)),
                        'available_fpx': 0,
                        'available_ewallet': 0,
                        'available_total': 0,
                        'remarks': remarks,
                    }
                    
                    if existing_record:
                        for attr, value in record_data.items():
                            setattr(existing_record, attr, value)
                    else:
                        session.add(Deposit(**record_data))
                    
                    count += 1
                
                _calculate_available_settlements(
                    session, merchant, year, month, public_holidays, add_on_holidays
                )
        
        session.commit()
        logger.info(f"Initialized {count} deposit records")
        
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to init deposit: {e}")
        raise
    finally:
        session.close()


def _calculate_available_settlements(
    session, merchant: str, year: int, month: int,
    public_holidays: Set[str], add_on_holidays: Set[str]
):
    date_prefix = f"{year}-{month:02d}"
    
    deposits = session.query(Deposit).filter(
        Deposit.merchant == merchant,
        Deposit.transaction_date.like(f"{date_prefix}%")
    ).all()
    
    prev_month = month - 1
    prev_year = year
    if prev_month == 0:
        prev_month = 12
        prev_year = year - 1
    prev_date_prefix = f"{prev_year}-{prev_month:02d}"
    
    prev_deposits = session.query(Deposit).filter(
        Deposit.merchant == merchant,
        Deposit.transaction_date.like(f"{prev_date_prefix}%")
    ).all()
    
    all_deposits = list(prev_deposits) + list(deposits)
    
    fpx_settlement: Dict[str, float] = {}
    ewallet_settlement: Dict[str, float] = {}
    
    for dep in all_deposits:
        if dep.fpx_settlement_rule and dep.fpx_gross:
            settlement_date = calculate_settlement_date(
                dep.transaction_date, dep.fpx_settlement_rule, public_holidays, add_on_holidays
            )
            if settlement_date and settlement_date.startswith(date_prefix):
                fpx_settlement[settlement_date] = fpx_settlement.get(settlement_date, 0) + dep.fpx_gross
        
        if dep.ewallet_settlement_rule and dep.ewallet_gross:
            settlement_date = calculate_settlement_date(
                dep.transaction_date, dep.ewallet_settlement_rule, public_holidays, add_on_holidays
            )
            if settlement_date and settlement_date.startswith(date_prefix):
                ewallet_settlement[settlement_date] = ewallet_settlement.get(settlement_date, 0) + dep.ewallet_gross
    
    for deposit in deposits:
        deposit.available_fpx = r(fpx_settlement.get(deposit.transaction_date, 0))
        deposit.available_ewallet = r(ewallet_settlement.get(deposit.transaction_date, 0))
        deposit.available_total = r((deposit.available_fpx or 0) + (deposit.available_ewallet or 0))


class DepositSheetService:
    _client: Optional[SheetsClient] = None
    
    @classmethod
    def get_client(cls) -> SheetsClient:
        if cls._client is None:
            cls._client = SheetsClient()
        return cls._client
    
    @classmethod
    def sync_sheet(cls) -> int:
        client = cls.get_client()
        
        merchant_value = client.read_data(DEPOSIT_SHEET, 'B1')
        if not merchant_value or not merchant_value[0]:
            raise ValueError("Merchant not selected")
        merchant = merchant_value[0][0]
        
        period_value = client.read_data(DEPOSIT_SHEET, 'B2')
        if not period_value or not period_value[0]:
            raise ValueError("Period not selected")
        
        year, month = cls._parse_period(period_value[0][0])
        if not year or not month:
            raise ValueError("Invalid period format")
        
        session = get_session()
        
        try:
            add_on_holidays = ParameterService.load_parameters()
            public_holidays = load_malaysia_holidays()
            
            manual_inputs = cls._read_manual_inputs()
            cls._apply_manual_inputs(session, manual_inputs, public_holidays, add_on_holidays)
            
            _calculate_available_settlements(
                session, merchant, year, month, public_holidays, add_on_holidays
            )
            
            session.commit()
            
            date_prefix = f"{year}-{month:02d}"
            records = session.query(Deposit).filter(
                Deposit.merchant == merchant,
                Deposit.transaction_date.like(f"{date_prefix}%")
            ).order_by(Deposit.transaction_date).all()
            
            cls._write_to_sheet(records)
            
            return len(records)
            
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to sync Deposit sheet: {e}")
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
        data = client.read_data(DEPOSIT_SHEET, DATA_RANGE)
        
        manual_inputs = []
        for row in data:
            if len(row) < 24 or not row[0]:
                continue
            
            record_id = row[0]
            fpx_fee_type = row[4] if len(row) > 4 else ''
            fpx_fee_rate = row[5] if len(row) > 5 else ''
            fpx_settlement_rule = row[8] if len(row) > 8 else ''
            ewallet_fee_type = row[12] if len(row) > 12 else ''
            ewallet_fee_rate = row[13] if len(row) > 13 else ''
            ewallet_settlement_rule = row[16] if len(row) > 16 else ''
            remarks = row[23] if len(row) > 23 else ''
            
            has_data = any([fpx_fee_type, fpx_fee_rate, fpx_settlement_rule,
                          ewallet_fee_type, ewallet_fee_rate, ewallet_settlement_rule, remarks])
            
            if not has_data:
                continue
            
            manual_inputs.append({
                'id': int(record_id),
                'fpx_fee_type': fpx_fee_type if fpx_fee_type else None,
                'fpx_fee_rate': to_float(fpx_fee_rate) if fpx_fee_rate else None,
                'fpx_settlement_rule': fpx_settlement_rule if fpx_settlement_rule else None,
                'ewallet_fee_type': ewallet_fee_type if ewallet_fee_type else None,
                'ewallet_fee_rate': to_float(ewallet_fee_rate) if ewallet_fee_rate else None,
                'ewallet_settlement_rule': ewallet_settlement_rule if ewallet_settlement_rule else None,
                'remarks': remarks if remarks else None,
            })
        
        return manual_inputs
    
    @classmethod
    def _apply_manual_inputs(cls, session, manual_inputs: List[Dict],
                             public_holidays: Set[str], add_on_holidays: Set[str]) -> int:
        if not manual_inputs:
            return 0
        
        ids = [m['id'] for m in manual_inputs]
        records = session.query(Deposit).filter(Deposit.id.in_(ids)).all()
        records_by_id = {rec.id: rec for rec in records}
        
        count = 0
        for input_data in manual_inputs:
            record = records_by_id.get(input_data['id'])
            if not record:
                continue
            
            if input_data['fpx_fee_type'] is not None:
                record.fpx_fee_type = input_data['fpx_fee_type'].lower()
            if input_data['fpx_fee_rate'] is not None:
                record.fpx_fee_rate = input_data['fpx_fee_rate']
            if input_data['fpx_settlement_rule'] is not None:
                record.fpx_settlement_rule = input_data['fpx_settlement_rule'].upper()
                record.fpx_settlement_date = calculate_settlement_date(
                    record.transaction_date, record.fpx_settlement_rule,
                    public_holidays, add_on_holidays
                )
            
            if input_data['ewallet_fee_type'] is not None:
                record.ewallet_fee_type = input_data['ewallet_fee_type'].lower()
            if input_data['ewallet_fee_rate'] is not None:
                record.ewallet_fee_rate = input_data['ewallet_fee_rate']
            if input_data['ewallet_settlement_rule'] is not None:
                record.ewallet_settlement_rule = input_data['ewallet_settlement_rule'].upper()
                record.ewallet_settlement_date = calculate_settlement_date(
                    record.transaction_date, record.ewallet_settlement_rule,
                    public_holidays, add_on_holidays
                )
            
            if input_data['remarks'] is not None:
                record.remarks = input_data['remarks']
            
            record.fpx_fee_amount = calculate_fee(
                record.fpx_fee_type, record.fpx_fee_rate,
                record.fpx_amount or 0, record.fpx_volume or 0
            )
            record.fpx_gross = r((record.fpx_amount or 0) - (record.fpx_fee_amount or 0))
            
            record.ewallet_fee_amount = calculate_fee(
                record.ewallet_fee_type, record.ewallet_fee_rate,
                record.ewallet_amount or 0, record.ewallet_volume or 0
            )
            record.ewallet_gross = r((record.ewallet_amount or 0) - (record.ewallet_fee_amount or 0))
            
            record.total_fees = r((record.fpx_fee_amount or 0) + (record.ewallet_fee_amount or 0))
            
            count += 1
        
        logger.info(f"Applied {count} manual inputs to Deposit")
        return count
    
    @classmethod
    def _write_to_sheet(cls, records: List[Deposit]):
        client = cls.get_client()
        
        rows = []
        for rec in records:
            rows.append([
                rec.id,
                rec.transaction_date or '',
                rec.fpx_amount or '',
                rec.fpx_volume or '',
                rec.fpx_fee_type or '',
                rec.fpx_fee_rate or '',
                rec.fpx_fee_amount or '',
                rec.fpx_gross or '',
                rec.fpx_settlement_rule or '',
                rec.fpx_settlement_date or '',
                rec.ewallet_amount or '',
                rec.ewallet_volume or '',
                rec.ewallet_fee_type or '',
                rec.ewallet_fee_rate or '',
                rec.ewallet_fee_amount or '',
                rec.ewallet_gross or '',
                rec.ewallet_settlement_rule or '',
                rec.ewallet_settlement_date or '',
                rec.total_amount or '',
                rec.total_fees or '',
                rec.available_fpx or '',
                rec.available_ewallet or '',
                rec.available_total or '',
                rec.remarks or '',
            ])
        
        worksheet = client.spreadsheet.worksheet(DEPOSIT_SHEET)
        worksheet.batch_clear([DATA_RANGE])
        
        if rows:
            client.write_data(DEPOSIT_SHEET, rows, f'A{DATA_START_ROW}')
            
            end_row = DATA_START_ROW + len(rows)
            fee_types = ['percentage', 'per_volume', 'flat']
            settlement_rules = ['T+1', 'T+2', 'T+3']
            
            client.set_dropdown_range(DEPOSIT_SHEET, 'E', DATA_START_ROW, end_row, fee_types)
            client.set_dropdown_range(DEPOSIT_SHEET, 'I', DATA_START_ROW, end_row, settlement_rules)
            client.set_dropdown_range(DEPOSIT_SHEET, 'M', DATA_START_ROW, end_row, fee_types)
            client.set_dropdown_range(DEPOSIT_SHEET, 'Q', DATA_START_ROW, end_row, settlement_rules)
        
        logger.info(f"Wrote {len(rows)} rows to Deposit sheet")
