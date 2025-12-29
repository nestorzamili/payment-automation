from typing import List, Dict, Any, Set
from calendar import monthrange

from sqlalchemy import and_, func

from src.core.database import get_session
from src.core.models import Deposit, KiraTransaction, Parameter
from src.core.logger import get_logger
from src.utils.helpers import normalize_channel, r, to_float, calculate_fee
from src.utils.holiday import load_malaysia_holidays, calculate_settlement_date

logger = get_logger(__name__)


def init_deposit():
    session = get_session()
    
    try:
        settlement_rules, add_on_holidays = _load_parameters()
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
                    
                    fpx_data = tx_map.get((date_str, 'FPX'), {'amount': 0, 'volume': 0, 'settlement_amount': 0})
                    ewallet_data = tx_map.get((date_str, 'EWALLET'), {'amount': 0, 'volume': 0, 'settlement_amount': 0})
                    
                    existing_record = existing_map.get((merchant, date_str))
                    
                    fpx_fee_type = existing_record.fpx_fee_type if existing_record else None
                    fpx_fee_rate = existing_record.fpx_fee_rate if existing_record else None
                    ewallet_fee_type = existing_record.ewallet_fee_type if existing_record else None
                    ewallet_fee_rate = existing_record.ewallet_fee_rate if existing_record else None
                    remarks = existing_record.remarks if existing_record else None
                    
                    fpx_rule = _get_settlement_rule(settlement_rules, 'FPX')
                    ewallet_rule = _get_settlement_rule(settlement_rules, 'EWALLET')
                    
                    fpx_settlement_date = calculate_settlement_date(
                        date_str, fpx_rule, public_holidays, add_on_holidays
                    )
                    ewallet_settlement_date = calculate_settlement_date(
                        date_str, ewallet_rule, public_holidays, add_on_holidays
                    )
                    
                    fpx_fee_amount = calculate_fee(
                        fpx_fee_type, fpx_fee_rate, fpx_data['amount'], fpx_data['volume']
                    )
                    ewallet_fee_amount = calculate_fee(
                        ewallet_fee_type, ewallet_fee_rate, ewallet_data['amount'], ewallet_data['volume']
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
                        'fpx_settlement_date': fpx_settlement_date,
                        'ewallet_amount': r(ewallet_data['amount']),
                        'ewallet_volume': ewallet_data['volume'],
                        'ewallet_fee_type': ewallet_fee_type,
                        'ewallet_fee_rate': ewallet_fee_rate,
                        'ewallet_fee_amount': ewallet_fee_amount,
                        'ewallet_gross': r(ewallet_data['amount'] - (ewallet_fee_amount or 0)),
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
                    session, merchant, year, month, 
                    settlement_rules, public_holidays, add_on_holidays
                )
        
        session.commit()
        logger.info(f"Initialized {count} deposit records")
        
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to init deposit: {e}")
        raise
    finally:
        session.close()


def _load_parameters() -> tuple[Dict[str, str], Set[str]]:
    session = get_session()
    try:
        params = session.query(Parameter).all()
        
        settlement_rules = {}
        add_on_holidays = set()
        
        for p in params:
            if p.type == 'SETTLEMENT_RULES':
                settlement_rules[p.key.lower()] = p.value
            elif p.type == 'ADD_ON_HOLIDAYS':
                add_on_holidays.add(p.key)
        
        return settlement_rules, add_on_holidays
    finally:
        session.close()


def _get_settlement_rule(rules: Dict[str, str], channel: str) -> str:
    return rules.get(channel.lower(), 'T+1')


def _calculate_available_settlements(
    session, merchant: str, year: int, month: int,
    settlement_rules: Dict[str, str], public_holidays: Set[str], add_on_holidays: Set[str]
):
    date_prefix = f"{year}-{month:02d}"
    
    prev_month = month - 1
    prev_year = year
    if prev_month == 0:
        prev_month = 12
        prev_year = year - 1
    prev_date_prefix = f"{prev_year}-{prev_month:02d}"
    
    prev_kira = session.query(
        func.substr(KiraTransaction.transaction_date, 1, 10).label('tx_date'),
        KiraTransaction.payment_method,
        func.sum(KiraTransaction.amount).label('amount'),
    ).filter(
        KiraTransaction.merchant == merchant,
        KiraTransaction.transaction_date.like(f"{prev_date_prefix}%")
    ).group_by(
        func.substr(KiraTransaction.transaction_date, 1, 10),
        KiraTransaction.payment_method
    ).all()
    
    current_kira = session.query(
        func.substr(KiraTransaction.transaction_date, 1, 10).label('tx_date'),
        KiraTransaction.payment_method,
        func.sum(KiraTransaction.amount).label('amount'),
    ).filter(
        KiraTransaction.merchant == merchant,
        KiraTransaction.transaction_date.like(f"{date_prefix}%")
    ).group_by(
        func.substr(KiraTransaction.transaction_date, 1, 10),
        KiraTransaction.payment_method
    ).all()
    
    fpx_rule = _get_settlement_rule(settlement_rules, 'FPX')
    ewallet_rule = _get_settlement_rule(settlement_rules, 'EWALLET')
    
    fpx_settlement: Dict[str, float] = {}
    ewallet_settlement: Dict[str, float] = {}
    
    for row in list(prev_kira) + list(current_kira):
        channel = normalize_channel(row.payment_method)
        tx_date = row.tx_date
        amount = row.amount or 0
        
        if channel == 'FPX':
            settlement_date = calculate_settlement_date(tx_date, fpx_rule, public_holidays, add_on_holidays)
            if settlement_date and settlement_date.startswith(date_prefix):
                fpx_settlement[settlement_date] = fpx_settlement.get(settlement_date, 0) + amount
        else:
            settlement_date = calculate_settlement_date(tx_date, ewallet_rule, public_holidays, add_on_holidays)
            if settlement_date and settlement_date.startswith(date_prefix):
                ewallet_settlement[settlement_date] = ewallet_settlement.get(settlement_date, 0) + amount
    
    deposits = session.query(Deposit).filter(
        Deposit.merchant == merchant,
        Deposit.transaction_date.like(f"{date_prefix}%")
    ).all()
    
    for deposit in deposits:
        deposit.available_fpx = r(fpx_settlement.get(deposit.transaction_date, 0))
        deposit.available_ewallet = r(ewallet_settlement.get(deposit.transaction_date, 0))
        deposit.available_total = r((deposit.available_fpx or 0) + (deposit.available_ewallet or 0))


class DepositService:
    
    def __init__(self):
        pass
    
    def get_deposit_data(self, merchant: str, year: int, month: int) -> List[Dict[str, Any]]:
        session = get_session()
        
        try:
            date_prefix = f"{year}-{month:02d}"
            
            records = session.query(Deposit).filter(
                and_(
                    Deposit.merchant == merchant,
                    Deposit.transaction_date.like(f"{date_prefix}%")
                )
            ).order_by(Deposit.transaction_date).all()
            
            return [self._to_response_dict(rec) for rec in records]
            
        finally:
            session.close()
    
    def _to_response_dict(self, record: Deposit) -> Dict[str, Any]:
        return {
            'id': record.id,
            'transaction_date': record.transaction_date,
            'fpx_amount': record.fpx_amount,
            'fpx_volume': record.fpx_volume,
            'fpx_fee_type': record.fpx_fee_type,
            'fpx_fee_rate': record.fpx_fee_rate,
            'fpx_fee_amount': record.fpx_fee_amount,
            'fpx_gross': record.fpx_gross,
            'fpx_settlement_date': record.fpx_settlement_date,
            'ewallet_amount': record.ewallet_amount,
            'ewallet_volume': record.ewallet_volume,
            'ewallet_fee_type': record.ewallet_fee_type,
            'ewallet_fee_rate': record.ewallet_fee_rate,
            'ewallet_fee_amount': record.ewallet_fee_amount,
            'ewallet_gross': record.ewallet_gross,
            'ewallet_settlement_date': record.ewallet_settlement_date,
            'total_amount': record.total_amount,
            'total_fees': record.total_fees,
            'available_fpx': record.available_fpx,
            'available_ewallet': record.available_ewallet,
            'available_total': record.available_total,
            'remarks': record.remarks,
        }
    
    def save_fee_inputs(self, fee_data: List[Dict[str, Any]]) -> int:
        session = get_session()
        count = 0
        
        try:
            grouped = {}
            for row in fee_data:
                deposit_id = row.get('id')
                channel = row.get('channel')
                
                if not deposit_id or not channel:
                    continue
                
                deposit_id = int(deposit_id)
                if deposit_id not in grouped:
                    grouped[deposit_id] = {'FPX': {}, 'EWALLET': {}}
                
                grouped[deposit_id][channel] = {
                    'fee_type': row.get('fee_type'),
                    'fee_rate': to_float(row.get('fee_rate')),
                    'remarks': row.get('remarks'),
                }
            
            if not grouped:
                return 0
            
            records = session.query(Deposit).filter(
                Deposit.id.in_(grouped.keys())
            ).all()
            
            records_by_id = {r.id: r for r in records}
            
            for deposit_id, channel_data in grouped.items():
                existing = records_by_id.get(deposit_id)
                if not existing:
                    continue
                
                fpx_data = channel_data.get('FPX', {})
                if fpx_data.get('fee_type') is not None or fpx_data.get('fee_rate') is not None:
                    existing.fpx_fee_type = fpx_data.get('fee_type')
                    existing.fpx_fee_rate = fpx_data.get('fee_rate')
                    existing.fpx_fee_amount = calculate_fee(
                        existing.fpx_fee_type, existing.fpx_fee_rate,
                        existing.fpx_amount or 0, existing.fpx_volume or 0
                    )
                    existing.fpx_gross = r((existing.fpx_amount or 0) - (existing.fpx_fee_amount or 0))
                
                ewallet_data = channel_data.get('EWALLET', {})
                if ewallet_data.get('fee_type') is not None or ewallet_data.get('fee_rate') is not None:
                    existing.ewallet_fee_type = ewallet_data.get('fee_type')
                    existing.ewallet_fee_rate = ewallet_data.get('fee_rate')
                    existing.ewallet_fee_amount = calculate_fee(
                        existing.ewallet_fee_type, existing.ewallet_fee_rate,
                        existing.ewallet_amount or 0, existing.ewallet_volume or 0
                    )
                    existing.ewallet_gross = r((existing.ewallet_amount or 0) - (existing.ewallet_fee_amount or 0))
                
                remarks = fpx_data.get('remarks') or ewallet_data.get('remarks')
                if remarks:
                    existing.remarks = remarks
                
                existing.total_fees = r((existing.fpx_fee_amount or 0) + (existing.ewallet_fee_amount or 0))
                
                count += 1
            
            session.commit()
            logger.info(f"Saved {count} fee inputs")
            return count
            
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to save fee inputs: {e}")
            raise
        finally:
            session.close()

