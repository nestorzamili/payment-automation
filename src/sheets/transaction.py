from typing import List, Dict, Any, Set, Optional
from calendar import monthrange

from sqlalchemy import and_, func

from src.core.database import get_session
from src.core.models import KiraTransaction, DepositLedger
from src.core.logger import get_logger
from src.sheets.client import SheetsClient
from src.sheets.parameters import ParameterLoader
from src.utils.holiday import load_malaysia_holidays, calculate_settlement_date

logger = get_logger(__name__)


class TransactionService:
    
    def __init__(
        self, 
        add_on_holidays: Optional[Set[str]] = None, 
        param_loader: Optional[ParameterLoader] = None
    ):
        self.public_holidays = load_malaysia_holidays()
        self.add_on_holidays = add_on_holidays or set()
        self.param_loader = param_loader
    
    def _normalize_channel(self, payment_method: str) -> str:
        if not payment_method:
            return 'EWALLET'
        pm_upper = payment_method.upper().strip()
        if pm_upper in ('FPX', 'FPXC') or 'FPX' in pm_upper:
            return 'FPX'
        return 'EWALLET'
    
    def get_monthly_data(self, merchant: str, year: int, month: int) -> List[Dict[str, Any]]:
        session = get_session()
        
        try:
            date_prefix = f"{year}-{month:02d}"
            _, last_day = monthrange(year, month)
            
            kira_agg = session.query(
                func.substr(KiraTransaction.transaction_date, 1, 10).label('tx_date'),
                KiraTransaction.payment_method,
                func.sum(KiraTransaction.amount).label('kira_amount'),
                func.sum(KiraTransaction.settlement_amount).label('settlement_amount'),
                func.count().label('volume'),
            ).filter(
                and_(
                    KiraTransaction.merchant == merchant,
                    KiraTransaction.transaction_date.like(f"{date_prefix}%")
                )
            ).group_by(
                func.substr(KiraTransaction.transaction_date, 1, 10),
                KiraTransaction.payment_method
            ).all()
            
            tx_map: Dict[tuple, Dict[str, Any]] = {}
            for row in kira_agg:
                channel = self._normalize_channel(row.payment_method)
                key = (row.tx_date, channel)
                if key not in tx_map:
                    tx_map[key] = {'kira_amount': 0, 'settlement_amount': 0, 'volume': 0}
                tx_map[key]['kira_amount'] += row.kira_amount or 0
                tx_map[key]['settlement_amount'] += row.settlement_amount or 0
                tx_map[key]['volume'] += row.volume or 0
            
            fees = session.query(DepositLedger).filter(
                and_(
                    DepositLedger.merchant == merchant,
                    DepositLedger.transaction_date.like(f"{date_prefix}%")
                )
            ).all()
            
            fee_map = {}
            for fee in fees:
                key = (fee.transaction_date, fee.channel)
                fee_map[key] = fee
            
            result = []
            for day in range(1, last_day + 1):
                date_str = f"{year}-{month:02d}-{day:02d}"
                
                fpx_settlement_rule = self.param_loader.get_settlement_rule('FPX') if self.param_loader else 'T+1'
                ewallet_settlement_rule = self.param_loader.get_settlement_rule('EWALLET') if self.param_loader else 'T+2'
                
                fpx_settlement_date = calculate_settlement_date(
                    date_str, fpx_settlement_rule, self.public_holidays, self.add_on_holidays
                )
                ewallet_settlement_date = calculate_settlement_date(
                    date_str, ewallet_settlement_rule, self.public_holidays, self.add_on_holidays
                )
                
                row = {
                    'transaction_date': date_str,
                    'fpx_amount': 0, 'fpx_volume': 0, 'fpx_settlement_date': fpx_settlement_date,
                    'fpx_fee_type': '', 'fpx_fee_rate': None, 'fpx_fee_amount': 0, 'fpx_gross': 0,
                    'fpx_kira_settlement': 0,
                    'ewallet_amount': 0, 'ewallet_volume': 0, 'ewallet_settlement_date': ewallet_settlement_date,
                    'ewallet_fee_type': '', 'ewallet_fee_rate': None, 'ewallet_fee_amount': 0, 'ewallet_gross': 0,
                    'ewallet_kira_settlement': 0,
                    'total_amount': 0, 'total_fees': 0,
                    'available_fpx': 0, 'available_ewallet': 0, 'available_total': 0,
                    'remarks': None
                }
                
                fpx_tx = tx_map.get((date_str, 'FPX'))
                fpx_fee = fee_map.get((date_str, 'FPX'))
                if fpx_tx:
                    row['fpx_amount'] = round(fpx_tx['kira_amount'] or 0, 2)
                    row['fpx_volume'] = fpx_tx['volume'] or 0
                    row['fpx_kira_settlement'] = round(fpx_tx['settlement_amount'] or 0, 2)
                if fpx_fee:
                    row['fpx_fee_type'] = fpx_fee.fee_type
                    row['fpx_fee_rate'] = fpx_fee.fee_rate
                    row['fpx_fee_amount'] = fpx_fee.calculate_fee(row['fpx_amount'], row['fpx_volume'])
                    if fpx_fee.remarks:
                        row['remarks'] = fpx_fee.remarks
                row['fpx_gross'] = round(row['fpx_amount'] - row['fpx_fee_amount'], 2)
                
                ewallet_tx = tx_map.get((date_str, 'EWALLET'))
                ewallet_fee = fee_map.get((date_str, 'EWALLET'))
                if ewallet_tx:
                    row['ewallet_amount'] = round(ewallet_tx['kira_amount'] or 0, 2)
                    row['ewallet_volume'] = ewallet_tx['volume'] or 0
                    row['ewallet_kira_settlement'] = round(ewallet_tx['settlement_amount'] or 0, 2)
                if ewallet_fee:
                    row['ewallet_fee_type'] = ewallet_fee.fee_type
                    row['ewallet_fee_rate'] = ewallet_fee.fee_rate
                    row['ewallet_fee_amount'] = ewallet_fee.calculate_fee(row['ewallet_amount'], row['ewallet_volume'])
                    if ewallet_fee.remarks and not row['remarks']:
                        row['remarks'] = ewallet_fee.remarks
                row['ewallet_gross'] = round(row['ewallet_amount'] - row['ewallet_fee_amount'], 2)
                
                row['total_amount'] = round(row['fpx_amount'] + row['ewallet_amount'], 2)
                row['total_fees'] = round(row['fpx_fee_amount'] + row['ewallet_fee_amount'], 2)
                
                result.append(row)
            
            result = self._calculate_available_settlements(result, merchant, year, month, session)
            
            return result
            
        finally:
            session.close()
    
    def _calculate_available_settlements(
        self, rows: List[Dict], merchant: str, year: int, month: int, session
    ) -> List[Dict]:
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
            func.sum(KiraTransaction.settlement_amount).label('settlement_amount'),
        ).filter(
            and_(
                KiraTransaction.merchant == merchant,
                KiraTransaction.transaction_date.like(f"{prev_date_prefix}%")
            )
        ).group_by(
            func.substr(KiraTransaction.transaction_date, 1, 10),
            KiraTransaction.payment_method
        ).all()
        
        fpx_settlement_rule = self.param_loader.get_settlement_rule('FPX') if self.param_loader else 'T+1'
        ewallet_settlement_rule = self.param_loader.get_settlement_rule('EWALLET') if self.param_loader else 'T+2'
        
        fpx_settlement: Dict[str, float] = {}
        ewallet_settlement: Dict[str, float] = {}
        
        for row in prev_kira:
            channel = self._normalize_channel(row.payment_method)
            tx_date = row.tx_date
            settlement_amount = row.settlement_amount or 0
            
            if channel == 'FPX':
                settlement_date = calculate_settlement_date(
                    tx_date, fpx_settlement_rule, self.public_holidays, self.add_on_holidays
                )
                if settlement_date and settlement_date.startswith(date_prefix):
                    fpx_settlement[settlement_date] = fpx_settlement.get(settlement_date, 0) + settlement_amount
            else:
                settlement_date = calculate_settlement_date(
                    tx_date, ewallet_settlement_rule, self.public_holidays, self.add_on_holidays
                )
                if settlement_date and settlement_date.startswith(date_prefix):
                    ewallet_settlement[settlement_date] = ewallet_settlement.get(settlement_date, 0) + settlement_amount
        
        for row in rows:
            if row['fpx_settlement_date']:
                sd = row['fpx_settlement_date']
                fpx_settlement[sd] = fpx_settlement.get(sd, 0) + row['fpx_kira_settlement']
            if row['ewallet_settlement_date']:
                sd = row['ewallet_settlement_date']
                ewallet_settlement[sd] = ewallet_settlement.get(sd, 0) + row['ewallet_kira_settlement']
        
        for row in rows:
            date = row['transaction_date']
            row['available_fpx'] = round(fpx_settlement.get(date, 0), 2)
            row['available_ewallet'] = round(ewallet_settlement.get(date, 0), 2)
            row['available_total'] = round(row['available_fpx'] + row['available_ewallet'], 2)
        
        return rows
