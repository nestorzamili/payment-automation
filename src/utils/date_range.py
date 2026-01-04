from __future__ import annotations

from datetime import datetime, timedelta, date
from typing import Tuple, Optional

from src.core.database import get_session
from src.core.loader import get_timezone, load_settings
from src.core.models import Job
from src.core.logger import get_logger

logger = get_logger(__name__)


class DateRangeService:
    
    def __init__(self):
        settings = load_settings()
        self.max_range_days = settings['download'].get('max_range_days', 30)
        default_start = settings['download'].get('default_start_date', '2025-12-01')
        self.default_start_date = datetime.strptime(default_start, '%Y-%m-%d').date()
    
    def get_date_range(self, platform: str) -> Optional[Tuple[str, str]]:
        last_job = self._get_last_completed_download(platform)
        today = datetime.now(get_timezone()).date()
        
        if last_job and last_job.to_date:
            last_to_date = datetime.strptime(last_job.to_date, '%Y-%m-%d').date()
            
            if last_to_date >= today:
                logger.info(f"{platform}: Already up to date (last_to_date={last_to_date}, today={today})")
                return None
            
            from_date = last_to_date
            logger.info(f"{platform}: Last download to_date {from_date}")
        else:
            from_date = self.default_start_date
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
        if gap > self.max_range_days:
            to_date = from_date + timedelta(days=self.max_range_days)
            logger.warning(f"{platform}: Gap {gap} days, limiting to {self.max_range_days} days ({from_date} to {to_date})")
        else:
            to_date = today
        
        return from_date.strftime('%Y-%m-%d'), to_date.strftime('%Y-%m-%d')
