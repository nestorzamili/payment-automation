from datetime import datetime, timedelta, date
from typing import Tuple, Optional
from zoneinfo import ZoneInfo

from sqlalchemy import func

from src.core.database import get_session
from src.core.models import Job
from src.core.logger import get_logger

logger = get_logger(__name__)
KL_TZ = ZoneInfo('Asia/Kuala_Lumpur')
MAX_RANGE_DAYS = 30
DEFAULT_START_DATE = date(2025, 10, 1)


class DateRangeService:
    
    def get_date_range(self, platform: str) -> Optional[Tuple[str, str]]:
        last_job = self._get_last_completed_download(platform)
        today = datetime.now(KL_TZ).date()
        
        if last_job and last_job.to_date:
            last_to_date = datetime.strptime(last_job.to_date, '%Y-%m-%d').date()
            
            if last_to_date >= today:
                logger.info(f"{platform}: Already up to date (last_to_date={last_to_date}, today={today})")
                return None
            
            from_date = last_to_date
            logger.info(f"{platform}: Last download to_date {from_date}")
        else:
            from_date = DEFAULT_START_DATE
            logger.info(f"{platform}: No completed download, starting from {from_date}")
        
        return self._calculate_range(from_date, today, platform)
    
    def _get_last_completed_download(self, platform: str) -> Job | None:
        session = get_session()
        try:
            job = session.query(Job).filter(
                Job.job_type == 'download',
                Job.platform == platform,
                Job.status == 'completed'
            ).order_by(Job.to_date.desc()).first()
            return job
        finally:
            session.close()
    
    def _calculate_range(self, from_date: date, today: date, platform: str) -> Tuple[str, str]:
        gap = (today - from_date).days
        if gap > MAX_RANGE_DAYS:
            to_date = from_date + timedelta(days=MAX_RANGE_DAYS)
            logger.warning(f"{platform}: Gap {gap} days, limiting to {MAX_RANGE_DAYS} days ({from_date} to {to_date})")
        else:
            to_date = today
        
        return from_date.strftime('%Y-%m-%d'), to_date.strftime('%Y-%m-%d')

