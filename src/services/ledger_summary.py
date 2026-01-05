from typing import Dict, Any, List, Optional
from sqlalchemy import func
import re

from src.core.database import get_session
from src.core.models import MerchantLedger, KiraTransaction, AgentLedger, Deposit
from src.core.logger import get_logger
from src.services.client import SheetsClient

logger = get_logger(__name__)

SUMMARY_SHEET = 'Summary'
DATA_START_ROW = 5
DATA_RANGE = 'A5:N200'

MONTHS = {
    'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
    'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
}

VIEW_TYPE_MAP = {
    'Merchants': 'merchants',
    'Agents': 'agents',
    'Payout Pool Balance': 'payout_pool'
}


class SummarySheetService:
    _client: Optional[SheetsClient] = None
    
    @classmethod
    def get_client(cls) -> SheetsClient:
        if cls._client is None:
            cls._client = SheetsClient()
        return cls._client
    
    @classmethod
    def sync_sheet(cls) -> int:
        client = cls.get_client()
        
        year_value = client.read_data(SUMMARY_SHEET, 'B1')
        if not year_value or not year_value[0]:
            raise ValueError("Year not selected")
        year = int(year_value[0][0])
        
        view_type_value = client.read_data(SUMMARY_SHEET, 'B2')
        if not view_type_value or not view_type_value[0]:
            raise ValueError("View Type not selected")
        view_type_raw = view_type_value[0][0]
        view_type = VIEW_TYPE_MAP.get(view_type_raw, view_type_raw.lower())
        
        session = get_session()
        
        try:
            if view_type == 'merchants':
                data = cls._get_merchants_summary(session, year)
            elif view_type == 'agents':
                data = cls._get_agents_summary(session, year)
            elif view_type == 'payout_pool':
                data = cls._get_payout_pool_summary(session, year)
            else:
                data = {'merchants': [], 'data': {}, 'monthly_totals': {}}
            
            cls._write_to_sheet(data)
            
            return len(data.get('merchants', []))
            
        except Exception as e:
            logger.error(f"Failed to sync Summary sheet: {e}")
            raise
        finally:
            session.close()
    
    @classmethod
    def _get_merchants_summary(cls, session, year: int) -> Dict[str, Any]:
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
        
        return cls._format_results(results)
    
    @classmethod
    def _get_agents_summary(cls, session, year: int) -> Dict[str, Any]:
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
                avail_fpx = (deposit.fpx_amount or 0) * rate_fpx / 1000 if rate_fpx else 0
                avail_ewallet = (deposit.ewallet_amount or 0) * rate_ewallet / 1000 if rate_ewallet else 0
                available_total = avail_fpx + avail_ewallet
            
            available_total += (ledger.commission_amount or 0)
            
            month = ledger.transaction_date[5:7]
            results.append({
                'merchant': ledger.merchant,
                'month': month,
                'total': available_total
            })
        
        return cls._format_agent_results(results)
    
    @classmethod
    def _get_payout_pool_summary(cls, session, year: int) -> Dict[str, Any]:
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
        
        return cls._format_results(results)
    
    @classmethod
    def _format_results(cls, results) -> Dict[str, Any]:
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
    
    @classmethod
    def _format_agent_results(cls, results) -> Dict[str, Any]:
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
    
    @classmethod
    def _write_to_sheet(cls, data: Dict[str, Any]):
        client = cls.get_client()
        
        merchants = data.get('merchants', [])
        merchant_data = data.get('data', {})
        monthly_totals = data.get('monthly_totals', {})
        
        rows = []
        for merchant in merchants:
            m_data = merchant_data.get(merchant, {})
            rows.append([
                merchant,
                m_data.get('1', 0) or '',
                m_data.get('2', 0) or '',
                m_data.get('3', 0) or '',
                m_data.get('4', 0) or '',
                m_data.get('5', 0) or '',
                m_data.get('6', 0) or '',
                m_data.get('7', 0) or '',
                m_data.get('8', 0) or '',
                m_data.get('9', 0) or '',
                m_data.get('10', 0) or '',
                m_data.get('11', 0) or '',
                m_data.get('12', 0) or '',
                m_data.get('total', 0) or '',
            ])
        
        grand_total = monthly_totals.get('grand_total', 0)
        if grand_total != 0:
            rows.append([
                'Total Deposit',
                monthly_totals.get('1', 0) or '',
                monthly_totals.get('2', 0) or '',
                monthly_totals.get('3', 0) or '',
                monthly_totals.get('4', 0) or '',
                monthly_totals.get('5', 0) or '',
                monthly_totals.get('6', 0) or '',
                monthly_totals.get('7', 0) or '',
                monthly_totals.get('8', 0) or '',
                monthly_totals.get('9', 0) or '',
                monthly_totals.get('10', 0) or '',
                monthly_totals.get('11', 0) or '',
                monthly_totals.get('12', 0) or '',
                grand_total,
            ])
        
        worksheet = client.spreadsheet.worksheet(SUMMARY_SHEET)
        worksheet.batch_clear([DATA_RANGE])
        
        if rows:
            client.write_data(SUMMARY_SHEET, rows, f'A{DATA_START_ROW}')
        
        logger.info(f"Wrote {len(rows)} rows to Summary sheet")
