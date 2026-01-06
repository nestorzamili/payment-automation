from __future__ import annotations

import threading
from datetime import datetime

from src.core.database import get_session
from src.core.loader import get_timezone
from src.core.models import Job

class JobManager:
    _instance = None
    _lock = threading.Lock()

    def __new__(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = super().__new__(cls)
        return cls._instance

    def create_job(
        self, 
        job_type: str,
        run_id: str = None,
        platform: str = None,
        account_label: str = None,
        source_type: str = None,
        from_date: str = None,
        to_date: str = None
    ) -> int:
        now = datetime.now(get_timezone()).strftime('%Y-%m-%d %H:%M:%S')
        
        session = get_session()
        try:
            job = Job(
                run_id=run_id,
                job_type=job_type,
                platform=platform,
                account_label=account_label,
                source_type=source_type,
                from_date=from_date,
                to_date=to_date,
                status='pending',
                created_at=now,
                updated_at=now,
            )
            session.add(job)
            session.commit()
            session.refresh(job)
            return job.job_id
        finally:
            session.close()

    def update_job(
        self, 
        job_id: int, 
        status: str, 
        error_message: str = None,
        fetched_count: int = None,
        stored_count: int = None
    ):
        session = get_session()
        try:
            job = session.query(Job).filter_by(job_id=job_id).first()
            if job:
                job.status = status
                job.updated_at = datetime.now(get_timezone()).strftime('%Y-%m-%d %H:%M:%S')
                if error_message is not None:
                    job.error_message = error_message
                if fetched_count is not None:
                    job.fetched_count = fetched_count
                if stored_count is not None:
                    job.stored_count = stored_count
                session.commit()
        finally:
            session.close()

    def get_job(self, job_id: int) -> dict | None:
        session = get_session()
        try:
            job = session.query(Job).filter_by(job_id=job_id).first()
            return job.to_dict() if job else None
        finally:
            session.close()


    def get_running_job_by_type(self, job_type: str) -> dict | None:
        session = get_session()
        try:
            job = session.query(Job).filter(
                Job.job_type == job_type,
                Job.status.in_(['pending', 'running'])
            ).first()
            return job.to_dict() if job else None
        finally:
            session.close()

job_manager = JobManager()



