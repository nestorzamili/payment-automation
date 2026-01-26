from __future__ import annotations

from datetime import datetime, timedelta, date
from typing import Tuple, Optional, Dict

from src.core.database import get_session
from src.core.loader import get_timezone, load_settings
from src.core.models import Job
from src.core.logger import get_logger

logger = get_logger(__name__)

PLATFORMS = ['kira', 'axai', 'm1', 'fiuu']


class DateRangeService:
    
    def __init__(self):
        settings = load_settings()
        self.max_range_days = settings['download'].get('max_range_days', 30)
        self.platform_max_range = {'kira': 15}
        default_start = settings['download'].get('default_start_date', '2025-12-01')
        self.default_start_date = datetime.strptime(default_start, '%Y-%m-%d').date()
    
    def get_platform_ranges(self) -> Dict[str, Optional[Tuple[str, str]]]:
        today = datetime.now(get_timezone()).date()
        
        if self.default_start_date >= today:
            logger.info(f"Default start date {self.default_start_date} not reached yet (today={today})")
            return {p: None for p in PLATFORMS}
        
        progress = self._get_all_progress()
        
        if not progress:
            ranges = {}
            for p in PLATFORMS:
                target_date = self._calculate_target_date(self.default_start_date, today, p)
                ranges[p] = (self.default_start_date.strftime('%Y-%m-%d'), target_date.strftime('%Y-%m-%d'))
            logger.info(f"No completed downloads, starting all platforms from {self.default_start_date}")
            return ranges
        
        max_progress = max(progress.values())
        all_synced = len(progress) == len(PLATFORMS) and all(d == max_progress for d in progress.values())
        
        if all_synced:
            ranges = {}
            for p in PLATFORMS:
                target_date = self._calculate_target_date(max_progress, today, p)
                if target_date <= max_progress:
                    ranges[p] = None
                else:
                    ranges[p] = (max_progress.strftime('%Y-%m-%d'), target_date.strftime('%Y-%m-%d'))
            if all(r is None for r in ranges.values()):
                logger.info(f"All platforms up to date at {max_progress}")
            else:
                logger.info(f"All platforms synced at {max_progress}, advancing")
            return ranges
        
        ranges = {}
        for platform in PLATFORMS:
            from_date = progress.get(platform, self.default_start_date)
            
            if from_date >= max_progress:
                logger.info(f"{platform}: Already at max progress ({from_date})")
                ranges[platform] = None
            else:
                to_date = self._calculate_target_date(from_date, max_progress, platform)
                logger.info(f"{platform}: Catch up {from_date} -> {to_date}")
                ranges[platform] = (from_date.strftime('%Y-%m-%d'), to_date.strftime('%Y-%m-%d'))
        
        return ranges
    
    def _get_all_progress(self) -> Dict[str, date]:
        from src.core.loader import load_accounts
        
        session = get_session()
        try:
            session.expire_all()
            accounts = load_accounts()
            progress = {}
            
            for platform in PLATFORMS:
                platform_accounts = [a['label'] for a in accounts if a['platform'] == platform]
                
                if not platform_accounts:
                    continue
                
                account_progress = {}
                for acc_label in platform_accounts:
                    job = session.query(Job).filter(
                        Job.job_type == 'download',
                        Job.platform == platform,
                        Job.account_label == acc_label,
                        Job.status == 'completed'
                    ).order_by(Job.to_date.desc()).first()
                    
                    if job and job.to_date:
                        account_progress[acc_label] = datetime.strptime(job.to_date, '%Y-%m-%d').date()
                
                if not account_progress:
                    continue
                
                if len(account_progress) < len(platform_accounts):
                    missing = set(platform_accounts) - set(account_progress.keys())
                    logger.info(f"{platform}: accounts without progress: {missing}")
                    continue
                
                min_progress = min(account_progress.values())
                progress[platform] = min_progress
                logger.debug(f"{platform} progress: {account_progress} -> min={min_progress}")
            
            return progress
        finally:
            session.close()
    
    def _calculate_target_date(self, from_date: date, target: date, platform: str = None) -> date:
        max_range = self.platform_max_range.get(platform, self.max_range_days) if platform else self.max_range_days
        gap = (target - from_date).days
        if gap > max_range:
            return from_date + timedelta(days=max_range)
        return target
