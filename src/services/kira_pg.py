from typing import Dict, List, Any, Optional
import re

from sqlalchemy import func

from src.core.database import get_session
from src.core.models import KiraPG, KiraTransaction, PGTransaction
from src.core.logger import get_logger
from src.services.client import SheetsClient
from src.services.parameters import ParameterService
from src.utils.helpers import categorize_channel, round_decimal, to_float
from src.utils.holiday import load_malaysia_holidays, calculate_settlement_date

logger = get_logger(__name__)

KIRA_PG_SHEET = 'Kira PG'
DATA_START_ROW = 4
DATA_RANGE = 'A4:R300'

MONTHS = {
    'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
    'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
}


def init_kira_pg():
    session = get_session()
    
    try:
        kira_agg = session.query(
            PGTransaction.account_label.label('pg_account_label'),
            func.substr(KiraTransaction.transaction_date, 1, 10).label('tx_date'),
            KiraTransaction.payment_method,
            func.sum(KiraTransaction.amount).label('kira_amount'),
            func.sum(KiraTransaction.mdr).label('mdr'),
            func.sum(KiraTransaction.settlement_amount).label('kira_settlement_amount'),
        ).join(
            PGTransaction,
            KiraTransaction.transaction_id == PGTransaction.transaction_id
        ).group_by(
            PGTransaction.account_label,
            func.substr(KiraTransaction.transaction_date, 1, 10),
            KiraTransaction.payment_method
        ).all()
        
        pg_agg = session.query(
            PGTransaction.account_label.label('pg_account_label'),
            func.substr(PGTransaction.transaction_date, 1, 10).label('tx_date'),
            PGTransaction.channel,
            func.sum(PGTransaction.amount).label('pg_amount'),
            func.count().label('volume'),
        ).join(
            KiraTransaction,
            PGTransaction.transaction_id == KiraTransaction.transaction_id
        ).group_by(
            PGTransaction.account_label,
            func.substr(PGTransaction.transaction_date, 1, 10),
            PGTransaction.channel
        ).all()
        
        kira_map: Dict[tuple, Dict] = {}
        for row in kira_agg:
            channel = categorize_channel(row.payment_method)
            key = (row.pg_account_label, row.tx_date, channel)
            if key not in kira_map:
                kira_map[key] = {'kira_amount': 0, 'mdr': 0, 'kira_settlement_amount': 0}
            kira_map[key]['kira_amount'] += row.kira_amount or 0
            kira_map[key]['mdr'] += row.mdr or 0
            kira_map[key]['kira_settlement_amount'] += row.kira_settlement_amount or 0
        
        pg_map: Dict[tuple, Dict] = {}
        for row in pg_agg:
            channel = categorize_channel(row.channel)
            key = (row.pg_account_label, row.tx_date, channel)
            if key not in pg_map:
                pg_map[key] = {'pg_amount': 0, 'volume': 0}
            pg_map[key]['pg_amount'] += row.pg_amount or 0
            pg_map[key]['volume'] += row.volume or 0
        
        all_keys = set(kira_map.keys()) | set(pg_map.keys())
        
        existing = session.query(KiraPG).all()
        existing_map = {(e.pg_account_label, e.transaction_date, e.channel): e for e in existing}
        
        records = []
        for key in all_keys:
            pg_account_label, tx_date, channel = key
            
            kira_data = kira_map.get(key, {'kira_amount': 0, 'mdr': 0, 'kira_settlement_amount': 0})
            pg_data = pg_map.get(key, {'pg_amount': 0, 'volume': 0})
            
            if kira_data['kira_amount'] == 0 and pg_data['pg_amount'] == 0:
                continue
            
            existing_record = existing_map.get(key)
            settlement_rule = existing_record.settlement_rule if existing_record else None
            settlement_date = existing_record.settlement_date if existing_record else None
            fee_type = existing_record.fee_type if existing_record else None
            fee_rate = existing_record.fee_rate if existing_record else None
            remarks = existing_record.remarks if existing_record else None
            
            fees = _calculate_fee(fee_type, fee_rate, pg_data['pg_amount'])
            settlement_amount = round_decimal(pg_data['pg_amount'] - fees) if fees is not None else None
            daily_variance = round_decimal(kira_data['kira_amount'] - pg_data['pg_amount'])
            
            records.append({
                'key': key,
                'pg_account_label': pg_account_label,
                'transaction_date': tx_date,
                'channel': channel,
                'kira_amount': round_decimal(kira_data['kira_amount']),
                'mdr': round_decimal(kira_data['mdr']),
                'kira_settlement_amount': round_decimal(kira_data['kira_settlement_amount']),
                'pg_amount': round_decimal(pg_data['pg_amount']),
                'volume': pg_data['volume'],
                'settlement_rule': settlement_rule,
                'settlement_date': settlement_date,
                'fee_type': fee_type,
                'fee_rate': fee_rate,
                'fees': fees,
                'settlement_amount': settlement_amount,
                'daily_variance': daily_variance,
                'remarks': remarks,
            })
        
        records.sort(key=lambda x: (x['transaction_date'], x['pg_account_label'] or '', x['channel']))
        
        cumulative = 0
        for record in records:
            cumulative += record['daily_variance'] or 0
            record['cumulative_variance'] = round_decimal(cumulative)
        
        for record in records:
            key = record.pop('key')
            existing_record = existing_map.get(key)
            
            if existing_record:
                for attr, value in record.items():
                    setattr(existing_record, attr, value)
            else:
                session.add(KiraPG(**record))
        
        session.commit()
        logger.info(f"Initialized {len(records)} kira_pg records")
        
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to init kira_pg: {e}")
        raise
    finally:
        session.close()


def _calculate_fee(fee_type: Optional[str], fee_rate: Optional[float], amount: Optional[float]) -> Optional[float]:
    if fee_rate is None:
        return None
    if fee_type == 'flat':
        return round_decimal(fee_rate)
    return round_decimal(amount * fee_rate / 100) if amount else None


def _recalculate_cumulative_variance(session, year_month: str):
    records = session.query(KiraPG).filter(
        KiraPG.transaction_date.like(f"{year_month}%")
    ).order_by(
        KiraPG.transaction_date,
        KiraPG.pg_account_label,
        KiraPG.channel
    ).all()
    
    cumulative = 0
    for record in records:
        cumulative += record.daily_variance or 0
        record.cumulative_variance = round_decimal(cumulative)


class KiraPGSheetService:
    _client: Optional[SheetsClient] = None
    
    @classmethod
    def get_client(cls) -> SheetsClient:
        if cls._client is None:
            cls._client = SheetsClient()
        return cls._client
    
    @classmethod
    def sync_sheet(cls) -> int:
        client = cls.get_client()
        
        period_value = client.read_data(KIRA_PG_SHEET, 'B1')
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
            
            session.commit()
            
            date_prefix = f"{year}-{month:02d}"
            records = session.query(KiraPG).filter(
                KiraPG.transaction_date.like(f"{date_prefix}%")
            ).order_by(
                KiraPG.transaction_date,
                KiraPG.pg_account_label,
                KiraPG.channel
            ).all()
            
            cls._write_to_sheet(records)
            
            return len(records)
            
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to sync Kira PG sheet: {e}")
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
        data = client.read_data(KIRA_PG_SHEET, DATA_RANGE)
        
        manual_inputs = []
        for row in data:
            if len(row) < 1 or not row[0]:
                continue
            
            record_id = row[0]
            settlement_rule = row[9].strip() if len(row) > 9 and row[9] else ''
            fee_type = row[11].strip() if len(row) > 11 and row[11] else ''
            fee_rate = row[12] if len(row) > 12 else ''
            remarks = row[17].strip() if len(row) > 17 and row[17] else ''
            
            manual_inputs.append({
                'id': int(record_id),
                'settlement_rule': settlement_rule if settlement_rule else None,
                'fee_type': fee_type if fee_type else None,
                'fee_rate': to_float(fee_rate) if fee_rate else None,
                'remarks': remarks if remarks else None,
            })
        
        return manual_inputs
    
    @classmethod
    def _apply_manual_inputs(cls, session, manual_inputs: List[Dict],
                             public_holidays, add_on_holidays) -> int:
        if not manual_inputs:
            return 0
        
        ids = [m['id'] for m in manual_inputs]
        records = session.query(KiraPG).filter(KiraPG.id.in_(ids)).all()
        records_by_id = {rec.id: rec for rec in records}
        
        affected_dates = set()
        count = 0
        
        for input_data in manual_inputs:
            record = records_by_id.get(input_data['id'])
            if not record:
                continue
            
            if input_data['settlement_rule'] is not None:
                record.settlement_rule = input_data['settlement_rule'].upper()
                record.settlement_date = calculate_settlement_date(
                    record.transaction_date, record.settlement_rule,
                    public_holidays, add_on_holidays
                )
            else:
                record.settlement_rule = None
                record.settlement_date = None
            
            if input_data['fee_type'] is not None:
                record.fee_type = input_data['fee_type'].lower()
            else:
                record.fee_type = None
            
            record.fee_rate = input_data['fee_rate']
            
            record.remarks = input_data['remarks']
            
            record.fees = _calculate_fee(record.fee_type, record.fee_rate, record.pg_amount)
            record.settlement_amount = round_decimal(record.pg_amount - record.fees) if record.fees is not None else None
            
            affected_dates.add(record.transaction_date[:7])
            count += 1
        
        for ym in affected_dates:
            _recalculate_cumulative_variance(session, ym)
        
        logger.info(f"Applied {count} manual inputs to Kira PG")
        return count
    
    @classmethod
    def _write_to_sheet(cls, records: List[KiraPG]):
        client = cls.get_client()
        
        rows = []
        for rec in records:
            rows.append([
                rec.id,
                rec.pg_account_label or '',
                rec.channel or '',
                rec.kira_amount or '',
                rec.mdr or '',
                rec.kira_settlement_amount or '',
                rec.transaction_date or '',
                rec.pg_amount or '',
                rec.volume or '',
                rec.settlement_rule or '',
                rec.settlement_date or '',
                rec.fee_type or '',
                rec.fee_rate or '',
                rec.fees if rec.fees is not None else '',
                rec.settlement_amount if rec.settlement_amount is not None else '',
                rec.daily_variance if rec.daily_variance is not None else '',
                rec.cumulative_variance if rec.cumulative_variance is not None else '',
                rec.remarks or '',
            ])
        
        worksheet = client.spreadsheet.worksheet(KIRA_PG_SHEET)
        client.clear_data_validation(KIRA_PG_SHEET, DATA_RANGE)
        worksheet.batch_clear([DATA_RANGE])

        if rows:
            client.write_data(KIRA_PG_SHEET, rows, f'A{DATA_START_ROW}')
            
            end_row = DATA_START_ROW + len(rows)
            client.set_dropdown_range(KIRA_PG_SHEET, 'J', DATA_START_ROW, end_row, ['T+1', 'T+2', 'T+3', 'T+4', 'T+5'])
            client.set_dropdown_range(KIRA_PG_SHEET, 'L', DATA_START_ROW, end_row, ['percentage', 'flat'])
        
        logger.info(f"Wrote {len(rows)} rows to Kira PG sheet")
