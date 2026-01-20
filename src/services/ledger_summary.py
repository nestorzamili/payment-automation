from typing import Dict, Any, List, Optional
from sqlalchemy import func

from src.core.database import get_session
from src.core.models import MerchantLedger, AgentLedger
from src.core.logger import get_logger
from src.services.client import SheetsClient
from src.utils.helpers import MONTHS, round_decimal, to_float

logger = get_logger(__name__)

SUMMARY_SHEET = 'Summary'
DATA_START_ROW = 5
DATA_RANGE = 'A5:N200'

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

        header_data = client.read_data(SUMMARY_SHEET, 'B1:B2')
        if not header_data or len(header_data) < 2:
            raise ValueError("Year or View Type not selected")

        year_str = header_data[0][0] if header_data[0] else None
        view_type_raw = header_data[1][0] if header_data[1] else None

        if not year_str:
            raise ValueError("Year not selected")
        if not view_type_raw:
            raise ValueError("View Type not selected")

        year = int(year_str)
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
            MerchantLedger.merchant,
            func.substr(MerchantLedger.transaction_date, 6, 2).label('month'),
            func.sum(func.coalesce(MerchantLedger.available_total, 0)).label('total')
        ).filter(
            MerchantLedger.transaction_date.like(f"{date_prefix}%")
        ).group_by(
            MerchantLedger.merchant,
            func.substr(MerchantLedger.transaction_date, 6, 2)
        ).all()
        
        return cls._format_results(results)
    
    @classmethod
    def _get_agents_summary(cls, session, year: int) -> Dict[str, Any]:
        date_prefix = f"{year}-"
        
        results = session.query(
            AgentLedger.merchant,
            func.substr(AgentLedger.transaction_date, 6, 2).label('month'),
            func.sum(func.coalesce(AgentLedger.available_total, 0)).label('total')
        ).filter(
            AgentLedger.transaction_date.like(f"{date_prefix}%")
        ).group_by(
            AgentLedger.merchant,
            func.substr(AgentLedger.transaction_date, 6, 2)
        ).all()
        
        return cls._format_results(results)
    
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
            total = to_float(row.total) or 0
            
            merchants.add(merchant)
            
            if merchant not in data:
                data[merchant] = {str(m): 0 for m in range(1, 13)}
                data[merchant]['total'] = 0
            
            data[merchant][month] = round_decimal(total)
            data[merchant]['total'] = round_decimal(data[merchant]['total'] + total)
            
            monthly_totals[month] = round_decimal(monthly_totals[month] + total)
            monthly_totals['grand_total'] = round_decimal(monthly_totals['grand_total'] + total)
        
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
                m_data.get('1', 0),
                m_data.get('2', 0),
                m_data.get('3', 0),
                m_data.get('4', 0),
                m_data.get('5', 0),
                m_data.get('6', 0),
                m_data.get('7', 0),
                m_data.get('8', 0),
                m_data.get('9', 0),
                m_data.get('10', 0),
                m_data.get('11', 0),
                m_data.get('12', 0),
                m_data.get('total', 0),
            ])
        
        grand_total = monthly_totals.get('grand_total', 0)
        rows.append([
            'Total Deposit',
            monthly_totals.get('1', 0),
            monthly_totals.get('2', 0),
            monthly_totals.get('3', 0),
            monthly_totals.get('4', 0),
            monthly_totals.get('5', 0),
            monthly_totals.get('6', 0),
            monthly_totals.get('7', 0),
            monthly_totals.get('8', 0),
            monthly_totals.get('9', 0),
            monthly_totals.get('10', 0),
            monthly_totals.get('11', 0),
            monthly_totals.get('12', 0),
            grand_total,
        ])
        
        worksheet = client.spreadsheet.worksheet(SUMMARY_SHEET)
        client.clear_row_backgrounds(SUMMARY_SHEET, DATA_START_ROW, 200, 1, 14)
        worksheet.batch_clear([DATA_RANGE])

        if rows:
            client.write_data(SUMMARY_SHEET, rows, f'A{DATA_START_ROW}')
            
            total_row = DATA_START_ROW + len(rows) - 1
            client.set_row_background(SUMMARY_SHEET, total_row, 1, 14)
        
        logger.info(f"Wrote {len(rows)} rows to Summary sheet")
