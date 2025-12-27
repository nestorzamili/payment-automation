from typing import Dict, Any, List
from sqlalchemy import func, and_

from src.core.database import get_session
from src.core.models import MerchantBalance, AgentBalance, Transaction
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
                Transaction.merchant,
                func.substr(Transaction.transaction_date, 6, 2).label('month'),
                func.sum(func.coalesce(Transaction.amount, 0)).label('total')
            ).filter(
                Transaction.transaction_date.like(f"{date_prefix}%")
            ).group_by(
                Transaction.merchant,
                func.substr(Transaction.transaction_date, 6, 2)
            ).all()
            
            return self._format_results(results)
            
        finally:
            session.close()
    
    def _get_agents_summary(self, year: int) -> Dict[str, Any]:
        session = get_session()
        
        try:
            date_prefix = f"{year}-"
            
            results = session.query(
                Transaction.merchant,
                func.substr(Transaction.transaction_date, 6, 2).label('month'),
                func.sum(func.coalesce(Transaction.amount, 0)).label('total')
            ).filter(
                Transaction.transaction_date.like(f"{date_prefix}%")
            ).group_by(
                Transaction.merchant,
                func.substr(Transaction.transaction_date, 6, 2)
            ).all()
            
            return self._format_results(results)
            
        finally:
            session.close()
    
    def _get_payout_pool_summary(self, year: int) -> Dict[str, Any]:
        session = get_session()
        
        try:
            date_prefix = f"{year}-"
            
            results = session.query(
                MerchantBalance.merchant,
                func.substr(MerchantBalance.transaction_date, 6, 2).label('month'),
                func.max(MerchantBalance.payout_pool_balance).label('total')
            ).filter(
                MerchantBalance.transaction_date.like(f"{date_prefix}%")
            ).group_by(
                MerchantBalance.merchant,
                func.substr(MerchantBalance.transaction_date, 6, 2)
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
