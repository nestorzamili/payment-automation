from datetime import datetime
from typing import Set, Optional

from sqlalchemy import and_

from src.core.database import get_session
from src.core.models import Job
from src.core.logger import get_logger

logger = get_logger(__name__)


def normalize_channel(channel: str) -> str:
    channel_lower = channel.lower().strip()
    
    if channel_lower in ['wallet', 'ewallet', 'e-wallet']:
        return 'ewallet'
    elif channel_lower in ['shopeepay', 'shopee pay', 'shopee']:
        return 'Shopee'
    elif channel_lower in ['touch n go', 'touchngo', 'tng', 'touch & go']:
        return 'TNG'
    elif channel_lower in ['boost']:
        return 'Boost'
    elif channel_lower in ['fpx']:
        return 'FPX'
    elif channel_lower in ['fpxc', 'fpx b2b']:
        return 'FPXC'
    
    return channel


def get_parsed_files(account_label: str = None, platform: str = None) -> Set[str]:
    session = get_session()
    parsed_files = set()
    
    try:
        query = session.query(Job).filter(
            and_(
                Job.job_type == 'parse',
                Job.status == 'completed'
            )
        )
        
        if account_label:
            query = query.filter(Job.account_label == account_label)
        
        if platform:
            query = query.filter(Job.platform == platform)
        
        jobs = query.all()
        
        for job in jobs:
            if job.files:
                for f in job.files:
                    parsed_files.add(f)
        
        return parsed_files
    finally:
        session.close()


def record_parsed_file(filename: str, account_label: str, platform: str, 
                       transactions_count: int = 0) -> Job:
    session = get_session()
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    try:
        job = Job(
            job_type='parse',
            platform=platform,
            account_label=account_label,
            status='completed',
            files=[filename],
            created_at=now,
            updated_at=now
        )
        session.add(job)
        session.commit()
        
        logger.debug(f"Recorded parsed file: {filename} ({platform})")
        return job
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to record parsed file: {e}")
        raise
    finally:
        session.close()
