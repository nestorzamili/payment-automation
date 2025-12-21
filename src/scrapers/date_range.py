from datetime import datetime, timedelta, date
from typing import Tuple
from zoneinfo import ZoneInfo

from sqlalchemy import func

from src.core.database import get_session
from src.core.models import KiraTransaction, PGTransaction
from src.core.logger import get_logger

logger = get_logger(__name__)
KL_TZ = ZoneInfo('Asia/Kuala_Lumpur')
MAX_RANGE_DAYS = 30
DEFAULT_START_DATE = date(2025, 10, 1)


class DateRangeService:
    
    def get_kira_date_range(self) -> Tuple[str, str]:
        last_date = self._get_last_transaction_date(KiraTransaction)
        return self._calculate_range(last_date, "Kira")
    
    def get_pg_date_range(self) -> Tuple[str, str]:
        last_date = self._get_last_transaction_date(PGTransaction)
        return self._calculate_range(last_date, "PG")
    
    def _get_last_transaction_date(self, model) -> datetime | None:
        session = get_session()
        try:
            result = session.query(func.max(model.transaction_date)).scalar()
            if result:
                return datetime.strptime(result[:10], '%Y-%m-%d')
            return None
        finally:
            session.close()
    
    def _calculate_range(self, last_date: datetime | None, source_name: str) -> Tuple[str, str]:
        today = datetime.now(KL_TZ).date()
        
        if last_date is None:
            from_date = DEFAULT_START_DATE
            logger.info(f"{source_name}: No existing data, starting from {from_date}")
        else:
            from_date = last_date.date()
            logger.info(f"{source_name}: Last transaction date {from_date}")
        
        gap = (today - from_date).days
        if gap > MAX_RANGE_DAYS:
            to_date = from_date + timedelta(days=MAX_RANGE_DAYS)
            logger.warning(f"{source_name}: Gap {gap} days, limiting to {MAX_RANGE_DAYS} days ({from_date} to {to_date})")
        else:
            to_date = today
        
        return from_date.strftime('%Y-%m-%d'), to_date.strftime('%Y-%m-%d')
