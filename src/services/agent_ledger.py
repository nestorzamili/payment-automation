from typing import Dict, List, Any, Optional
from calendar import monthrange
import re

from sqlalchemy import and_

from src.core.database import get_session
from src.core.models import AgentLedger, Deposit
from src.core.logger import get_logger
from src.services.client import SheetsClient
from src.utils.helpers import round_decimal, to_float, safe_get_value, parse_period, MONTHS

logger = get_logger(__name__)

AGENT_LEDGER_SHEET = 'Agents Balance & Settlement Ledger'
DATA_START_ROW = 5
DATA_RANGE = 'A5:Q50'



def init_agent_ledger(merchant: str, year: int, month: int):
    session = get_session()
    
    try:
        date_prefix = f"{year}-{month:02d}"
        _, last_day = monthrange(year, month)
        
        existing = session.query(AgentLedger.transaction_date).filter(
            and_(
                AgentLedger.merchant == merchant,
                AgentLedger.transaction_date.like(f"{date_prefix}%")
            )
        ).all()
        
        existing_dates = {rec[0] for rec in existing}
        
        for day in range(1, last_day + 1):
            date_str = f"{year}-{month:02d}-{day:02d}"
            if date_str not in existing_dates:
                session.add(AgentLedger(merchant=merchant, transaction_date=date_str))
        
        session.commit()
        
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to init agent ledger: {e}")
        raise
    finally:
        session.close()


def _aggregate_by_settlement(deposits, date_prefix: str):
    fpx_by_settlement = {}
    ewallet_by_settlement = {}

    for dep in deposits:
        if dep.fpx_settlement_date and dep.fpx_amount:
            if dep.fpx_settlement_date.startswith(date_prefix):
                if dep.fpx_settlement_date not in fpx_by_settlement:
                    fpx_by_settlement[dep.fpx_settlement_date] = 0
                fpx_by_settlement[dep.fpx_settlement_date] += dep.fpx_amount or 0

        if dep.ewallet_settlement_date and dep.ewallet_amount:
            if dep.ewallet_settlement_date.startswith(date_prefix):
                if dep.ewallet_settlement_date not in ewallet_by_settlement:
                    ewallet_by_settlement[dep.ewallet_settlement_date] = 0
                ewallet_by_settlement[dep.ewallet_settlement_date] += dep.ewallet_amount or 0

    return fpx_by_settlement, ewallet_by_settlement


def _get_previous_month_accum_balance(session, merchant: str, year: int, month: int) -> float:
    prev_month = month - 1
    prev_year = year
    if prev_month == 0:
        prev_month = 12
        prev_year = year - 1

    prev_date_prefix = f"{prev_year}-{prev_month:02d}"

    last_record = session.query(AgentLedger).filter(
        and_(
            AgentLedger.merchant == merchant,
            AgentLedger.transaction_date.like(f"{prev_date_prefix}%")
        )
    ).order_by(AgentLedger.transaction_date.desc()).first()

    return last_record.accumulative_balance or 0 if last_record else 0


def _recalculate_balances(session, merchant: str, year: int, month: int,
                          fpx_by_settlement: dict, ewallet_by_settlement: dict):
    date_prefix = f"{year}-{month:02d}"

    prev_accum = _get_previous_month_accum_balance(session, merchant, year, month)

    rows = session.query(AgentLedger).filter(
        and_(
            AgentLedger.merchant == merchant,
            AgentLedger.transaction_date.like(f"{date_prefix}%")
        )
    ).order_by(AgentLedger.transaction_date).all()

    for row in rows:
        date = row.transaction_date
        rate_fpx = row.commission_rate_fpx or 0
        rate_ewallet = row.commission_rate_ewallet or 0

        avail_fpx = 0
        if date in fpx_by_settlement and rate_fpx:
            avail_fpx = round_decimal(fpx_by_settlement[date] * rate_fpx / 100) or 0

        avail_ewallet = 0
        if date in ewallet_by_settlement and rate_ewallet:
            avail_ewallet = round_decimal(ewallet_by_settlement[date] * rate_ewallet / 100) or 0

        available_total = avail_fpx + avail_ewallet
        
        row.available_fpx = avail_fpx if avail_fpx > 0 else None
        row.available_ewallet = avail_ewallet if avail_ewallet > 0 else None
        row.available_total = round_decimal(available_total) if available_total > 0 else None
        
        commission_amount = row.commission_amount or 0
        debit = row.debit or 0

        has_activity = available_total > 0 or commission_amount > 0 or debit > 0 or prev_accum != 0

        if has_activity:
            row.balance = round_decimal(available_total + commission_amount - debit)
            row.accumulative_balance = round_decimal(prev_accum + row.balance)
            prev_accum = row.accumulative_balance
        else:
            row.balance = None
            row.accumulative_balance = None


class AgentLedgerSheetService:
    _client: Optional[SheetsClient] = None
    
    @classmethod
    def get_client(cls) -> SheetsClient:
        if cls._client is None:
            cls._client = SheetsClient()
        return cls._client
    
    @classmethod
    def sync_sheet(cls) -> int:
        client = cls.get_client()

        header_data = client.read_data(AGENT_LEDGER_SHEET, 'B1:B2')
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

        init_agent_ledger(merchant, year, month)

        session = get_session()

        try:
            manual_inputs = cls._read_manual_inputs()
            cls._apply_manual_inputs(session, manual_inputs)

            date_prefix = f"{year}-{month:02d}"
            deposits = session.query(Deposit).filter(
                and_(
                    Deposit.merchant == merchant,
                    Deposit.transaction_date.like(f"{date_prefix}%")
                )
            ).all()

            prev_deposits = cls._get_prev_month_deposits(session, merchant, year, month)
            all_deposits = list(prev_deposits) + list(deposits)

            fpx_by_settlement, ewallet_by_settlement = _aggregate_by_settlement(all_deposits, date_prefix)

            _recalculate_balances(session, merchant, year, month, fpx_by_settlement, ewallet_by_settlement)
            session.commit()

            data = cls._get_ledger_data(session, merchant, year, month, fpx_by_settlement, ewallet_by_settlement)
            cls._write_to_sheet(data)
            
            return len(data)
            
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to sync Agent Ledger sheet: {e}")
            raise
        finally:
            session.close()
    


    @classmethod
    def _get_prev_month_deposits(cls, session, merchant: str, year: int, month: int):
        prev_month = month - 1
        prev_year = year
        if prev_month == 0:
            prev_month = 12
            prev_year = year - 1

        prev_date_prefix = f"{prev_year}-{prev_month:02d}"

        return session.query(Deposit).filter(
            and_(
                Deposit.merchant == merchant,
                Deposit.transaction_date.like(f"{prev_date_prefix}%")
            )
        ).all()
    
    @classmethod
    def _read_manual_inputs(cls) -> List[Dict[str, Any]]:
        client = cls.get_client()
        data = client.read_data(AGENT_LEDGER_SHEET, DATA_RANGE)
        
        manual_inputs = []
        for row in data:
            if len(row) < 1 or not row[0]:
                continue
            
            record_id = row[0]
            commission_rate_fpx = safe_get_value(row, 2)
            commission_rate_ewallet = safe_get_value(row, 4)
            volume = safe_get_value(row, 10)
            commission_rate = safe_get_value(row, 11)
            debit = safe_get_value(row, 13)
            
            manual_inputs.append({
                'id': int(record_id),
                'commission_rate_fpx': to_float(commission_rate_fpx),
                'commission_rate_ewallet': to_float(commission_rate_ewallet),
                'volume': to_float(volume),
                'commission_rate': to_float(commission_rate),
                'debit': to_float(debit),
            })
        
        return manual_inputs
    
    @classmethod
    def _apply_manual_inputs(cls, session, manual_inputs: List[Dict]) -> int:
        if not manual_inputs:
            return 0
        
        ids = [m['id'] for m in manual_inputs]
        records = session.query(AgentLedger).filter(AgentLedger.id.in_(ids)).all()
        records_by_id = {rec.id: rec for rec in records}
        
        count = 0
        for input_data in manual_inputs:
            record = records_by_id.get(input_data['id'])
            if not record:
                continue
            
            record.commission_rate_fpx = input_data['commission_rate_fpx']
            record.commission_rate_ewallet = input_data['commission_rate_ewallet']
            record.volume = input_data['volume']
            record.commission_rate = input_data['commission_rate']
            record.debit = input_data['debit']
            
            if record.volume and record.commission_rate:
                record.commission_amount = round_decimal(record.volume * record.commission_rate / 100)
            else:
                record.commission_amount = None
            
            count += 1
        
        logger.info(f"Applied {count} manual inputs to Agent Ledger")
        return count
    
    @classmethod
    def _get_ledger_data(cls, session, merchant: str, year: int, month: int,
                         fpx_by_settlement: dict, ewallet_by_settlement: dict) -> List[Dict]:
        date_prefix = f"{year}-{month:02d}"
        
        deposits = session.query(Deposit).filter(
            and_(
                Deposit.merchant == merchant,
                Deposit.transaction_date.like(f"{date_prefix}%")
            )
        ).order_by(Deposit.transaction_date).all()
        
        ledgers = session.query(AgentLedger).filter(
            and_(
                AgentLedger.merchant == merchant,
                AgentLedger.transaction_date.like(f"{date_prefix}%")
            )
        ).order_by(AgentLedger.transaction_date).all()
        
        deposit_map = {d.transaction_date: d for d in deposits}
        ledger_map = {lg.transaction_date: lg for lg in ledgers}
        
        all_dates = set(deposit_map.keys()) | set(ledger_map.keys())
        
        result = []
        for date in sorted(all_dates):
            deposit = deposit_map.get(date)
            ledger = ledger_map.get(date)
            
            if deposit:
                kira_fpx = deposit.fpx_amount or 0
                kira_ewallet = deposit.ewallet_amount or 0
                
                rate_fpx = ledger.commission_rate_fpx if ledger else None
                rate_ewallet = ledger.commission_rate_ewallet if ledger else None
                
                fpx_commission = round_decimal(kira_fpx * rate_fpx / 100) if rate_fpx else None
                ewallet_commission = round_decimal(kira_ewallet * rate_ewallet / 100) if rate_ewallet else None
                
                gross = None
                if fpx_commission is not None or ewallet_commission is not None:
                    gross = round_decimal((fpx_commission or 0) + (ewallet_commission or 0))
                
                available_fpx = 0
                if date in fpx_by_settlement and rate_fpx:
                    available_fpx = round_decimal(fpx_by_settlement[date] * rate_fpx / 100) or 0

                available_ewallet = 0
                if date in ewallet_by_settlement and rate_ewallet:
                    available_ewallet = round_decimal(ewallet_by_settlement[date] * rate_ewallet / 100) or 0
                
                result.append({
                    'id': ledger.id if ledger else '',
                    'transaction_date': date,
                    'commission_rate_fpx': rate_fpx,
                    'fpx_commission': fpx_commission,
                    'commission_rate_ewallet': rate_ewallet,
                    'ewallet_commission': ewallet_commission,
                    'gross_amount': gross,
                    'available_fpx': available_fpx,
                    'available_ewallet': available_ewallet,
                    'available_total': round_decimal(available_fpx + available_ewallet),
                    'volume': ledger.volume if ledger else None,
                    'commission_rate': ledger.commission_rate if ledger else None,
                    'commission_amount': ledger.commission_amount if ledger else None,
                    'debit': ledger.debit if ledger else None,
                    'balance': ledger.balance if ledger else None,
                    'accumulative_balance': ledger.accumulative_balance if ledger else None,
                    'updated_at': ledger.updated_at if ledger else None,
                })
            elif ledger:
                rate_fpx = ledger.commission_rate_fpx
                rate_ewallet = ledger.commission_rate_ewallet
                
                available_fpx = 0
                if date in fpx_by_settlement and rate_fpx:
                    available_fpx = round_decimal(fpx_by_settlement[date] * rate_fpx / 100) or 0

                available_ewallet = 0
                if date in ewallet_by_settlement and rate_ewallet:
                    available_ewallet = round_decimal(ewallet_by_settlement[date] * rate_ewallet / 100) or 0
                
                result.append({
                    'id': ledger.id,
                    'transaction_date': date,
                    'commission_rate_fpx': rate_fpx,
                    'fpx_commission': None,
                    'commission_rate_ewallet': rate_ewallet,
                    'ewallet_commission': None,
                    'gross_amount': None,
                    'available_fpx': available_fpx,
                    'available_ewallet': available_ewallet,
                    'available_total': round_decimal(available_fpx + available_ewallet),
                    'volume': ledger.volume,
                    'commission_rate': ledger.commission_rate,
                    'commission_amount': ledger.commission_amount,
                    'debit': ledger.debit,
                    'balance': ledger.balance,
                    'accumulative_balance': ledger.accumulative_balance,
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
                rec.get('commission_rate_fpx') if rec.get('commission_rate_fpx') is not None else '',
                rec.get('fpx_commission') if rec.get('fpx_commission') is not None else 0,
                rec.get('commission_rate_ewallet') if rec.get('commission_rate_ewallet') is not None else '',
                rec.get('ewallet_commission') if rec.get('ewallet_commission') is not None else 0,
                rec.get('gross_amount') if rec.get('gross_amount') is not None else 0,
                rec.get('available_fpx') if rec.get('available_fpx') is not None else 0,
                rec.get('available_ewallet') if rec.get('available_ewallet') is not None else 0,
                rec.get('available_total') if rec.get('available_total') is not None else 0,
                rec.get('volume') if rec.get('volume') is not None else '',
                rec.get('commission_rate') if rec.get('commission_rate') is not None else '',
                rec.get('commission_amount') if rec.get('commission_amount') is not None else 0,
                rec.get('debit') if rec.get('debit') is not None else '',
                rec.get('balance') if rec.get('balance') is not None else 0,
                rec.get('accumulative_balance') if rec.get('accumulative_balance') is not None else 0,
                rec.get('updated_at') or '',
            ])
        
        worksheet = client.spreadsheet.worksheet(AGENT_LEDGER_SHEET)
        worksheet.batch_clear([DATA_RANGE])
        
        if rows:
            client.write_data(AGENT_LEDGER_SHEET, rows, f'A{DATA_START_ROW}')
        
        logger.info(f"Wrote {len(rows)} rows to Agent Ledger sheet")
