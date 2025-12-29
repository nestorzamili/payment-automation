from typing import Dict, Any
from sqlalchemy import func

from src.core.database import get_session
from src.core.models import MerchantLedger, KiraTransaction
from src.core.logger import get_logger

logger = get_logger(__name__)


class LedgerSummaryService:
    
    def get_summary(self, year: int, view_type: str) -> Dict[str, Any]:
        if view_type == 'merchants':
            return self._get_merchants_summary(year)
        elif view_type == 'agents':
            return self._get_agents_summary(year)
        elif view_type == 'payout_pool':
            return self._get_payout_pool_summary(year)
        else:
            return {'merchants': [], 'data': {}, 'monthly_totals': {}}
    
    def _get_merchants_summary(self, year: int) -> Dict[str, Any]:
        session = get_session()
        
        try:
            date_prefix = f"{year}-"
            
            results = session.query(
                KiraTransaction.merchant,
                func.substr(KiraTransaction.transaction_date, 6, 2).label('month'),
                func.sum(func.coalesce(KiraTransaction.amount, 0)).label('total')
            ).filter(
                KiraTransaction.transaction_date.like(f"{date_prefix}%")
            ).group_by(
                KiraTransaction.merchant,
                func.substr(KiraTransaction.transaction_date, 6, 2)
            ).all()
            
            return self._format_results(results)
            
        finally:
            session.close()
    
    def _get_agents_summary(self, year: int) -> Dict[str, Any]:
        session = get_session()
        
        try:
            from src.core.models import AgentLedger, Deposit
            
            date_prefix = f"{year}-"
            
            ledgers = session.query(AgentLedger).filter(
                AgentLedger.transaction_date.like(f"{date_prefix}%")
            ).all()
            
            deposits = session.query(Deposit).filter(
                Deposit.transaction_date.like(f"{date_prefix}%")
            ).all()
            
            deposit_map = {(d.merchant, d.transaction_date): d for d in deposits}
            
            results = []
            for ledger in ledgers:
                deposit = deposit_map.get((ledger.merchant, ledger.transaction_date))
                
                available_total = 0
                if deposit:
                    rate_fpx = ledger.commission_rate_fpx or 0
                    rate_ewallet = ledger.commission_rate_ewallet or 0
                    avail_fpx = (deposit.available_fpx or 0) * rate_fpx / 100 if rate_fpx else 0
                    avail_ewallet = (deposit.available_ewallet or 0) * rate_ewallet / 100 if rate_ewallet else 0
                    available_total = avail_fpx + avail_ewallet
                
                month = ledger.transaction_date[5:7]
                results.append({
                    'merchant': ledger.merchant,
                    'month': month,
                    'total': available_total
                })
            
            return self._format_agent_results(results)
            
        finally:
            session.close()
    
    def _format_agent_results(self, results) -> Dict[str, Any]:
        merchants = set()
        data = {}
        monthly_totals = {str(m): 0 for m in range(1, 13)}
        monthly_totals['grand_total'] = 0
        
        for row in results:
            merchant = row['merchant']
            month = str(int(row['month']))
            total = float(row['total']) if row['total'] else 0
            
            merchants.add(merchant)
            
            if merchant not in data:
                data[merchant] = {str(m): 0 for m in range(1, 13)}
                data[merchant]['total'] = 0
            
            data[merchant][month] = round(data[merchant][month] + total, 2)
            data[merchant]['total'] = round(data[merchant]['total'] + total, 2)
            
            monthly_totals[month] = round(monthly_totals[month] + total, 2)
            monthly_totals['grand_total'] = round(monthly_totals['grand_total'] + total, 2)
        
        return {
            'merchants': sorted(list(merchants)),
            'data': data,
            'monthly_totals': monthly_totals
        }
    
    def _get_payout_pool_summary(self, year: int) -> Dict[str, Any]:
        session = get_session()
        
        try:
            date_prefix = f"{year}-"
            
            results = session.query(
                MerchantLedger.merchant,
                func.substr(MerchantLedger.transaction_date, 6, 2).label('month'),
                func.max(MerchantLedger.payout_pool_balance).label('total')
            ).filter(
                MerchantLedger.transaction_date.like(f"{date_prefix}%")
            ).group_by(
                MerchantLedger.merchant,
                func.substr(MerchantLedger.transaction_date, 6, 2)
            ).all()
            
            return self._format_results(results)
            
        finally:
            session.close()
    
    def _format_results(self, results) -> Dict[str, Any]:
        merchants = set()
        data = {}
        monthly_totals = {str(m): 0 for m in range(1, 13)}
        monthly_totals['grand_total'] = 0
        
        for row in results:
            merchant = row.merchant
            month = str(int(row.month))
            total = float(row.total) if row.total else 0
            
            merchants.add(merchant)
            
            if merchant not in data:
                data[merchant] = {str(m): 0 for m in range(1, 13)}
                data[merchant]['total'] = 0
            
            data[merchant][month] = round(total, 2)
            data[merchant]['total'] = round(data[merchant]['total'] + total, 2)
            
            monthly_totals[month] = round(monthly_totals[month] + total, 2)
            monthly_totals['grand_total'] = round(monthly_totals['grand_total'] + total, 2)
        
        return {
            'merchants': sorted(list(merchants)),
            'data': data,
            'monthly_totals': monthly_totals
        }
