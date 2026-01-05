from typing import Dict, List, Any, Optional
from calendar import monthrange
import re

from sqlalchemy import and_

from src.core.database import get_session
from src.core.models import AgentLedger, Deposit
from src.core.logger import get_logger
from src.services.client import SheetsClient
from src.utils.helpers import r, to_float

logger = get_logger(__name__)

AGENT_LEDGER_SHEET = 'Agents Balance & Settlement Ledger'
DATA_START_ROW = 5
DATA_RANGE = 'A5:O50'

MONTHS = {
    'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
    'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
}


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


def _aggregate_by_settlement(deposits):
    fpx_by_settlement = {}
    ewallet_by_settlement = {}
    
    for dep in deposits:
        if dep.fpx_settlement_date and dep.fpx_amount:
            if dep.fpx_settlement_date not in fpx_by_settlement:
                fpx_by_settlement[dep.fpx_settlement_date] = []
            fpx_by_settlement[dep.fpx_settlement_date].append(dep)
        if dep.ewallet_settlement_date and dep.ewallet_amount:
            if dep.ewallet_settlement_date not in ewallet_by_settlement:
                ewallet_by_settlement[dep.ewallet_settlement_date] = []
            ewallet_by_settlement[dep.ewallet_settlement_date].append(dep)
    
    return fpx_by_settlement, ewallet_by_settlement


def _recalculate_balances(session, merchant: str, fpx_by_settlement: dict, ewallet_by_settlement: dict):
    rows = session.query(AgentLedger).filter(
        AgentLedger.merchant == merchant
    ).order_by(AgentLedger.transaction_date).all()
    
    prev_balance = 0
    
    for row in rows:
        date = row.transaction_date
        rate_fpx = row.commission_rate_fpx or 0
        rate_ewallet = row.commission_rate_ewallet or 0
        
        avail_fpx = 0
        if date in fpx_by_settlement and rate_fpx:
            fpx_sum = sum(d.fpx_amount or 0 for d in fpx_by_settlement[date])
            avail_fpx = r(fpx_sum * rate_fpx / 1000) or 0
        
        avail_ewallet = 0
        if date in ewallet_by_settlement and rate_ewallet:
            ewallet_sum = sum(d.ewallet_amount or 0 for d in ewallet_by_settlement[date])
            avail_ewallet = r(ewallet_sum * rate_ewallet / 1000) or 0
        
        available_total = avail_fpx + avail_ewallet
        commission_amount = row.commission_amount or 0
        
        has_activity = available_total > 0 or commission_amount > 0 or prev_balance != 0
        
        if has_activity:
            row.balance = r(prev_balance + available_total + commission_amount)
        else:
            row.balance = None
        
        prev_balance = row.balance if row.balance is not None else prev_balance


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
        
        merchant_value = client.read_data(AGENT_LEDGER_SHEET, 'B1')
        if not merchant_value or not merchant_value[0]:
            raise ValueError("Merchant not selected")
        merchant = merchant_value[0][0]
        
        period_value = client.read_data(AGENT_LEDGER_SHEET, 'B2')
        if not period_value or not period_value[0]:
            raise ValueError("Period not selected")
        
        year, month = cls._parse_period(period_value[0][0])
        if not year or not month:
            raise ValueError("Invalid period format")
        
        session = get_session()
        
        try:
            manual_inputs = cls._read_manual_inputs()
            cls._apply_manual_inputs(session, manual_inputs)
            
            deposits = session.query(Deposit).filter(Deposit.merchant == merchant).all()
            fpx_by_settlement, ewallet_by_settlement = _aggregate_by_settlement(deposits)
            
            _recalculate_balances(session, merchant, fpx_by_settlement, ewallet_by_settlement)
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
        data = client.read_data(AGENT_LEDGER_SHEET, DATA_RANGE)
        
        manual_inputs = []
        for row in data:
            if len(row) < 1 or not row[0]:
                continue
            
            record_id = row[0]
            commission_rate_fpx = row[2] if len(row) > 2 else ''
            commission_rate_ewallet = row[4] if len(row) > 4 else ''
            volume = row[10] if len(row) > 10 else ''
            commission_rate = row[11] if len(row) > 11 else ''
            
            manual_inputs.append({
                'id': int(record_id),
                'commission_rate_fpx': to_float(commission_rate_fpx) if commission_rate_fpx else None,
                'commission_rate_ewallet': to_float(commission_rate_ewallet) if commission_rate_ewallet else None,
                'volume': to_float(volume) if volume else None,
                'commission_rate': to_float(commission_rate) if commission_rate else None,
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
            
            if record.volume and record.commission_rate:
                record.commission_amount = r(record.volume * record.commission_rate)
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
        
        ledger_map = {lg.transaction_date: lg for lg in ledgers}
        
        result = []
        for deposit in deposits:
            date = deposit.transaction_date
            ledger = ledger_map.get(date)
            
            kira_fpx = deposit.fpx_amount or 0
            kira_ewallet = deposit.ewallet_amount or 0
            
            rate_fpx = ledger.commission_rate_fpx if ledger else None
            rate_ewallet = ledger.commission_rate_ewallet if ledger else None
            
            fpx_commission = r(kira_fpx * rate_fpx / 1000) if rate_fpx else None
            ewallet_commission = r(kira_ewallet * rate_ewallet / 1000) if rate_ewallet else None
            
            gross = None
            if fpx_commission is not None or ewallet_commission is not None:
                gross = r((fpx_commission or 0) + (ewallet_commission or 0))
            
            available_fpx = 0
            if date in fpx_by_settlement and rate_fpx:
                fpx_sum = sum(d.fpx_amount or 0 for d in fpx_by_settlement[date])
                available_fpx = r(fpx_sum * rate_fpx / 1000) or 0
            
            available_ewallet = 0
            if date in ewallet_by_settlement and rate_ewallet:
                ewallet_sum = sum(d.ewallet_amount or 0 for d in ewallet_by_settlement[date])
                available_ewallet = r(ewallet_sum * rate_ewallet / 1000) or 0
            
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
                'available_total': r(available_fpx + available_ewallet),
                'volume': ledger.volume if ledger else None,
                'commission_rate': ledger.commission_rate if ledger else None,
                'commission_amount': ledger.commission_amount if ledger else None,
                'balance': ledger.balance if ledger else None,
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
                rec.get('commission_rate_fpx') or '',
                rec.get('fpx_commission') if rec.get('fpx_commission') is not None else 0,
                rec.get('commission_rate_ewallet') or '',
                rec.get('ewallet_commission') if rec.get('ewallet_commission') is not None else 0,
                rec.get('gross_amount') if rec.get('gross_amount') is not None else 0,
                rec.get('available_fpx') if rec.get('available_fpx') is not None else 0,
                rec.get('available_ewallet') if rec.get('available_ewallet') is not None else 0,
                rec.get('available_total') if rec.get('available_total') is not None else 0,
                rec.get('volume') or '',
                rec.get('commission_rate') or '',
                rec.get('commission_amount') or '',
                rec.get('balance') or '',
                '',
            ])
        
        worksheet = client.spreadsheet.worksheet(AGENT_LEDGER_SHEET)
        worksheet.batch_clear([DATA_RANGE])
        
        if rows:
            client.write_data(AGENT_LEDGER_SHEET, rows, f'A{DATA_START_ROW}')
        
        logger.info(f"Wrote {len(rows)} rows to Agent Ledger sheet")
