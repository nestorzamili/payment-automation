from __future__ import annotations

import threading
from datetime import datetime
from typing import Callable, List

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

    def get_jobs_by_run_id(self, run_id: str) -> dict | None:
        session = get_session()
        try:
            jobs = session.query(Job).filter_by(run_id=run_id).all()
            if not jobs:
                return None
            
            running = sum(1 for j in jobs if j.status == 'running')
            completed = sum(1 for j in jobs if j.status == 'completed')
            failed = sum(1 for j in jobs if j.status == 'failed')
            total_stored = sum(j.stored_count or 0 for j in jobs)
            
            platforms = {}
            for j in jobs:
                if j.platform:
                    platforms[j.platform] = platforms.get(j.platform, 0) + (j.stored_count or 0)
            
            status = 'running' if running > 0 else 'completed'
            
            return {
                'status': status,
                'run_id': run_id,
                'total_jobs': len(jobs),
                'completed': completed,
                'failed': failed,
                'total_stored': total_stored,
                'platforms': platforms
            }
        finally:
            session.close()

    def list_jobs(self, run_id: str = None, limit: int = 50) -> List[dict]:
        session = get_session()
        try:
            query = session.query(Job).order_by(Job.created_at.desc())
            
            if run_id:
                query = query.filter(Job.run_id == run_id)
            
            jobs = query.limit(limit).all()
            return [j.to_dict() for j in jobs]
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

    def run_in_background(self, job_id: int, func: Callable, *args, **kwargs):
        def wrapper():
            self.update_job(job_id, 'running')
            try:
                func(*args, **kwargs)
                self.update_job(job_id, 'completed')
            except Exception as e:
                from src.core.logger import clean_error_msg
                error_msg = clean_error_msg(e)
                self.update_job(job_id, 'failed', error_message=error_msg)

        thread = threading.Thread(target=wrapper, daemon=True)
        thread.start()


job_manager = JobManager()



