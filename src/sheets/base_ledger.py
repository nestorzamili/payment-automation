from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from calendar import monthrange
from typing import List, Dict, Any, Set, Tuple

from sqlalchemy import and_

from src.core.database import get_session
from src.core.logger import get_logger
from src.sheets.client import SheetsClient

logger = get_logger(__name__)


class BaseLedgerService(ABC):
    """Base class for ledger services with shared logic."""
    
    def __init__(self, sheets_client: SheetsClient = None):
        self.sheets_client = sheets_client or SheetsClient()
    
    def _r(self, value):
        """Round to 2 decimal places."""
        return round(value, 2) if value is not None else None
    
    def _to_float(self, value):
        """Convert value to float, handling various input types."""
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            value = value.strip()
            if not value:
                return None
            try:
                return float(value)
            except ValueError:
                return None
        return None
    
    def init_from_deposit(self, deposit_rows: List[Dict[str, Any]]) -> int:
        """Initialize ledger from deposit data."""
        if not deposit_rows:
            return 0
        
        aggregated = self._aggregate_deposit_data(deposit_rows)
        settlement_map = self._build_settlement_map(deposit_rows)
        count = self._upsert_ledger_rows(aggregated, settlement_map)
        
        # Fill missing dates for entire month
        self._fill_missing_dates_for_all_merchants(deposit_rows)
        
        return count
    
    def _fill_missing_dates_for_all_merchants(self, deposit_rows: List[Dict[str, Any]]):
        """Fill missing dates for all merchants in the deposit data."""
        # Group by (merchant, year, month)
        merchant_months: Dict[Tuple[str, int, int], Set[str]] = {}
        
        for row in deposit_rows:
            merchant = row['Merchant']
            date_str = row['Transaction Date']
            year = int(date_str[:4])
            month = int(date_str[5:7])
            
            key = (merchant, year, month)
            if key not in merchant_months:
                merchant_months[key] = set()
            merchant_months[key].add(date_str)
        
        # For each merchant/month, fill the entire month
        for (merchant, year, month), existing_dates in merchant_months.items():
            self._fill_month_dates(merchant, year, month, existing_dates)
    
    def _fill_month_dates(self, merchant: str, year: int, month: int, existing_dates: Set[str]):
        """Fill all dates in a month for a merchant."""
        _, last_day = monthrange(year, month)
        
        all_dates = []
        for day in range(1, last_day + 1):
            date_str = f"{year}-{month:02d}-{day:02d}"
            all_dates.append(date_str)
        
        # Get already existing dates from database
        session = get_session()
        try:
            db_dates = self._get_existing_dates(session, merchant, year, month)
            existing_dates = existing_dates.union(db_dates)
            
            # Create rows for missing dates
            for date_str in all_dates:
                if date_str not in existing_dates:
                    self._create_zero_row(session, merchant, date_str)
            
            # Recalculate balances for this merchant
            self._recalculate_balances(session, merchant)
            
            session.commit()
            logger.debug(f"Filled missing dates for {merchant} {year}-{month:02d}")
            
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to fill missing dates: {e}")
            raise
        finally:
            session.close()
    
    @abstractmethod
    def _get_existing_dates(self, session, merchant: str, year: int, month: int) -> Set[str]:
        """Get existing dates from database for a merchant/month."""
        pass
    
    @abstractmethod
    def _create_zero_row(self, session, merchant: str, date_str: str):
        """Create a ledger row with zero values for a date."""
        pass
    
    @abstractmethod
    def _aggregate_deposit_data(self, deposit_rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """Aggregate deposit data by merchant and date."""
        pass
    
    def _build_settlement_map(self, deposit_rows: List[Dict[str, Any]]) -> Dict:
        """Build settlement map from deposit data (shared logic)."""
        settlement_map = {}
        
        for row in deposit_rows:
            merchant = row['Merchant']
            settlement_date = row['Settlement Date']
            channel = row['Channel'].upper()
            
            # Get appropriate amount based on ledger type
            amount = self._get_settlement_amount(row)
            
            channel_type = 'fpx' if channel in ('FPX', 'FPXC') else 'ewallet'
            key = (merchant, settlement_date, channel_type)
            
            if key not in settlement_map:
                settlement_map[key] = 0
            settlement_map[key] += amount
        
        return settlement_map
    
    @abstractmethod
    def _get_settlement_amount(self, row: Dict[str, Any]) -> float:
        """Get the settlement amount from a deposit row."""
        pass
    
    @abstractmethod
    def _upsert_ledger_rows(self, aggregated: Dict, settlement_map: Dict) -> int:
        """Upsert ledger rows from aggregated data."""
        pass
    
    @abstractmethod
    def get_ledger(self, merchant: str, year: int, month: int) -> List[Dict[str, Any]]:
        """Get ledger data for a merchant and month."""
        pass
    
    @abstractmethod
    def save_manual_data(self, manual_data: List[Dict[str, Any]]) -> int:
        """Save manual data updates."""
        pass
    
    @abstractmethod
    def _recalculate_balances(self, session, merchant: str):
        """Recalculate balances for a merchant."""
        pass
    
    @abstractmethod
    def upload_to_sheet(self, data: List[Dict[str, Any]], sheet_name: str = None) -> Dict[str, Any]:
        """Upload ledger data to Google Sheets."""
        pass
