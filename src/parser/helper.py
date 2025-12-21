from datetime import datetime
from typing import Set, Optional

from sqlalchemy import and_

from src.core.database import get_session
from src.core.models import Job
from src.core.logger import get_logger

logger = get_logger(__name__)


def get_parsed_files(account_label: str = None, platform: str = None) -> Set[str]:
    """
    Query jobs table untuk list files yang sudah berhasil di-parse.
    
    Args:
        account_label: Filter by account label (optional)
        platform: Filter by platform e.g. 'm1', 'axai', 'kira' (optional)
    
    Returns:
        Set of filenames that have been parsed
    """
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
        
        jobs = query.all()
        
        for job in jobs:
            if job.files:
                for f in job.files:
                    # Store with platform prefix if available
                    if platform and not f.startswith(f"{platform}:"):
                        continue
                    parsed_files.add(f.split(':', 1)[-1] if ':' in f else f)
        
        return parsed_files
    finally:
        session.close()


def record_parsed_file(filename: str, account_label: str, platform: str, 
                       transactions_count: int = 0) -> Job:
    """
    Create job record setelah file berhasil di-parse.
    
    Args:
        filename: Name of the parsed file
        account_label: Account label for PG, or None for Kira
        platform: Platform name ('m1', 'axai', 'kira')
        transactions_count: Number of transactions parsed
    
    Returns:
        Created Job object
    """
    session = get_session()
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    try:
        job = Job(
            job_type='parse',
            account_label=account_label,
            status='completed',
            files=[f"{platform}:{filename}"],
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
