import re
from datetime import datetime
from typing import Set, Optional, Tuple

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


def extract_date_range_from_filename(filename: str) -> Tuple[Optional[str], Optional[str]]:
    pattern = r'(\d{4}-\d{2}-\d{2})_(\d{4}-\d{2}-\d{2})'
    match = re.search(pattern, filename)
    if match:
        return match.group(1), match.group(2)
    return None, None


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
            if job.filename:
                parsed_files.add(job.filename)
        
        return parsed_files
    finally:
        session.close()


def start_parse_job(filename: str, account_label: str, platform: str, run_id: str = None) -> int:
    session = get_session()
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    from_date, to_date = extract_date_range_from_filename(filename)
    
    try:
        job = Job(
            run_id=run_id,
            job_type='parse',
            platform=platform,
            account_label=account_label,
            from_date=from_date,
            to_date=to_date,
            status='running',
            filename=filename,
            created_at=now,
            updated_at=now
        )
        session.add(job)
        session.commit()
        session.refresh(job)
        
        logger.debug(f"Started parse job: {filename} ({platform})")
        return job.job_id
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to start parse job: {e}")
        raise
    finally:
        session.close()


def complete_parse_job(job_id: int, transactions_count: int):
    session = get_session()
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    try:
        job = session.query(Job).filter_by(job_id=job_id).first()
        if job:
            job.status = 'completed'
            job.transactions_count = transactions_count
            job.updated_at = now
            session.commit()
            logger.debug(f"Completed parse job: {job_id} ({transactions_count} txn)")
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to complete parse job: {e}")
        raise
    finally:
        session.close()


def fail_parse_job(job_id: int, error: str):
    session = get_session()
    now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    try:
        job = session.query(Job).filter_by(job_id=job_id).first()
        if job:
            job.status = 'failed'
            job.desc = error
            job.updated_at = now
            session.commit()
            logger.debug(f"Failed parse job: {job_id}")
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to update parse job as failed: {e}")
        raise
    finally:
        session.close()



