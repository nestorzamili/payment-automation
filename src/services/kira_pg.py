from typing import List, Dict, Any, Optional, Set

from sqlalchemy import and_, func

from src.core.database import get_session
from src.core.models import KiraPG, KiraTransaction, PGTransaction, Parameter
from src.core.logger import get_logger
from src.utils.helpers import normalize_channel, r, to_float
from src.utils.holiday import load_malaysia_holidays, calculate_settlement_date

logger = get_logger(__name__)


def init_kira_pg():
    session = get_session()
    
    try:
        settlement_rules, add_on_holidays = _load_parameters()
        public_holidays = load_malaysia_holidays()
        
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
            channel = normalize_channel(row.payment_method)
            key = (row.pg_account_label, row.tx_date, channel)
            if key not in kira_map:
                kira_map[key] = {'kira_amount': 0, 'mdr': 0, 'kira_settlement_amount': 0}
            kira_map[key]['kira_amount'] += row.kira_amount or 0
            kira_map[key]['mdr'] += row.mdr or 0
            kira_map[key]['kira_settlement_amount'] += row.kira_settlement_amount or 0
        
        pg_map: Dict[tuple, Dict] = {}
        for row in pg_agg:
            channel = normalize_channel(row.channel)
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
            
            settlement_rule = _get_settlement_rule(settlement_rules, channel)
            settlement_date = calculate_settlement_date(
                tx_date, settlement_rule, public_holidays, add_on_holidays
            )
            
            daily_variance = r(kira_data['kira_amount'] - pg_data['pg_amount'])
            
            existing_record = existing_map.get(key)
            fee_rate = existing_record.fee_rate if existing_record else None
            remarks = existing_record.remarks if existing_record else None
            
            fees = r(pg_data['pg_amount'] * fee_rate / 100) if fee_rate else None
            settlement_amount = r(pg_data['pg_amount'] - fees) if fees else None
            
            records.append({
                'key': key,
                'pg_account_label': pg_account_label,
                'transaction_date': tx_date,
                'channel': channel,
                'kira_amount': r(kira_data['kira_amount']),
                'mdr': r(kira_data['mdr']),
                'kira_settlement_amount': r(kira_data['kira_settlement_amount']),
                'pg_amount': r(pg_data['pg_amount']),
                'volume': pg_data['volume'],
                'settlement_rule': settlement_rule,
                'settlement_date': settlement_date,
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
            record['cumulative_variance'] = r(cumulative)
        
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


class KiraPGService:
    
    def __init__(self):
        pass
    
    def get_kira_pg_data(self, year: int = None, month: int = None) -> List[Dict[str, Any]]:
        session = get_session()
        
        try:
            query = session.query(KiraPG)
            
            if year and month:
                date_prefix = f"{year}-{month:02d}"
                query = query.filter(KiraPG.transaction_date.like(f"{date_prefix}%"))
            
            records = query.order_by(
                KiraPG.transaction_date,
                KiraPG.pg_account_label,
                KiraPG.channel
            ).all()
            
            return [self._to_response_dict(rec) for rec in records]
            
        finally:
            session.close()
    
    def _to_response_dict(self, record: KiraPG) -> Dict[str, Any]:
        return {
            'pg_merchant': record.pg_account_label,
            'channel': record.channel,
            'kira_amount': record.kira_amount,
            'mdr': record.mdr,
            'kira_settlement_amount': record.kira_settlement_amount,
            'pg_date': record.transaction_date,
            'amount_pg': record.pg_amount,
            'transaction_count': record.volume,
            'settlement_rule': record.settlement_rule,
            'settlement_date': record.settlement_date,
            'fee_rate': record.fee_rate,
            'fees': record.fees,
            'settlement_amount': record.settlement_amount,
            'daily_variance': record.daily_variance,
            'cumulative_variance': record.cumulative_variance,
            'remarks': record.remarks,
        }
    
    def save_manual_data(self, manual_data: List[Dict[str, Any]]) -> int:
        session = get_session()
        count = 0
        affected_dates = set()
        
        try:
            for row in manual_data:
                pg_merchant = row.get('pg_merchant')
                date = row.get('pg_date')
                channel = normalize_channel(row.get('channel'))
                
                if not pg_merchant or not date:
                    continue
                
                existing = session.query(KiraPG).filter(
                    and_(
                        KiraPG.pg_account_label == pg_merchant,
                        KiraPG.transaction_date == date,
                        KiraPG.channel == channel
                    )
                ).first()
                
                if not existing:
                    continue
                
                fee_rate_raw = row.get('fee_rate')
                remarks_raw = row.get('remarks')
                
                if fee_rate_raw == 'CLEAR':
                    existing.fee_rate = None
                elif fee_rate_raw is not None:
                    existing.fee_rate = to_float(fee_rate_raw)
                
                if remarks_raw == 'CLEAR':
                    existing.remarks = None
                elif remarks_raw is not None and str(remarks_raw).strip():
                    existing.remarks = str(remarks_raw).strip()
                
                if existing.fee_rate is not None and existing.pg_amount:
                    existing.fees = r(existing.pg_amount * existing.fee_rate / 100)
                    existing.settlement_amount = r(existing.pg_amount - existing.fees)
                else:
                    existing.fees = None
                    existing.settlement_amount = None
                
                affected_dates.add(date[:7])
                count += 1
            
            for ym in affected_dates:
                self._recalculate_cumulative_variance(session, ym)
            
            session.commit()
            logger.info(f"Saved {count} Kira PG manual inputs")
            return count
            
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to save manual data: {e}")
            raise
        finally:
            session.close()
    
    def _recalculate_cumulative_variance(self, session, year_month: str):
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
            record.cumulative_variance = r(cumulative)
