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
            from src.core.models import AgentLedger
            from src.sheets.transaction import TransactionService
            
            date_prefix = f"{year}-"
            
            merchants = session.query(AgentLedger.merchant).filter(
                AgentLedger.transaction_date.like(f"{date_prefix}%")
            ).distinct().all()
            
            merchant_list = [m[0] for m in merchants]
            
            tx_service = TransactionService()
            results = []
            
            for merchant in merchant_list:
                for month in range(1, 13):
                    month_prefix = f"{year}-{month:02d}"
                    ledgers = session.query(AgentLedger).filter(
                        AgentLedger.merchant == merchant,
                        AgentLedger.transaction_date.like(f"{month_prefix}%")
                    ).all()
                    
                    if not ledgers:
                        continue
                    
                    ledger_map = {lg.transaction_date: lg for lg in ledgers}
                    
                    tx_data = tx_service.get_monthly_data(merchant, year, month)
                    
                    month_total = 0
                    for tx_row in tx_data:
                        date = tx_row['transaction_date']
                        ledger = ledger_map.get(date)
                        
                        if not ledger:
                            continue
                        
                        rate_fpx = ledger.commission_rate_fpx or 0
                        rate_ewallet = ledger.commission_rate_ewallet or 0
                        
                        available_fpx = round((tx_row['available_fpx'] or 0) * rate_fpx / 100, 2)
                        available_ewallet = round((tx_row['available_ewallet'] or 0) * rate_ewallet / 100, 2)
                        available_total = round(available_fpx + available_ewallet, 2)
                        
                        month_total += available_total
                    
                    class ResultRow:
                        pass
                    row = ResultRow()
                    row.merchant = merchant
                    row.month = f"{month:02d}"
                    row.total = round(month_total, 2)
                    results.append(row)
            
            return self._format_results(results)
            
        finally:
            session.close()
    
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
