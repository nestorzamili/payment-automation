import threading
from datetime import datetime
from typing import Callable, List
from zoneinfo import ZoneInfo

from src.core.database import get_session
from src.core.models import Job


KL_TZ = ZoneInfo('Asia/Kuala_Lumpur')


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
        from_date: str = None,
        to_date: str = None
    ) -> int:
        now = datetime.now(KL_TZ).strftime('%Y-%m-%d %H:%M:%S')
        
        session = get_session()
        try:
            job = Job(
                run_id=run_id,
                job_type=job_type,
                platform=platform,
                account_label=account_label,
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
        desc: str = None
    ):
        session = get_session()
        try:
            job = session.query(Job).filter_by(job_id=job_id).first()
            if job:
                job.status = status
                job.updated_at = datetime.now(KL_TZ).strftime('%Y-%m-%d %H:%M:%S')
                if desc is not None:
                    job.desc = desc
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
            total_transactions = sum(j.transactions_count or 0 for j in jobs)
            
            # Sum transactions per platform
            platforms = {}
            for j in jobs:
                if j.platform:
                    platforms[j.platform] = platforms.get(j.platform, 0) + (j.transactions_count or 0)
            
            # Status is 'running' if any job is still running
            status = 'running' if running > 0 else 'completed'
            
            return {
                'status': status,
                'run_id': run_id,
                'total_jobs': len(jobs),
                'completed': completed,
                'failed': failed,
                'total_transactions': total_transactions,
                'platforms': platforms
            }
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
                error_msg = str(e).split('Call log:')[0].strip()
                self.update_job(job_id, 'failed', desc=error_msg)

        thread = threading.Thread(target=wrapper, daemon=True)
        thread.start()


job_manager = JobManager()



