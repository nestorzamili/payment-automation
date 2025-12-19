import threading
from datetime import datetime
from typing import Any, Callable
from zoneinfo import ZoneInfo

from src.core.database import Job, get_session


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

    def create_job(self, job_type: str, metadata: dict = None) -> int:
        now = datetime.now(KL_TZ).isoformat()
        
        session = get_session()
        try:
            job = Job(
                type=job_type,
                status='pending',
                created_at=now,
                updated_at=now,
            )
            job.job_metadata = metadata or {}
            session.add(job)
            session.commit()
            session.refresh(job)
            return job.job_id
        finally:
            session.close()

    def update_job(self, job_id: int, status: str, result: Any = None, error: str = None):
        session = get_session()
        try:
            job = session.query(Job).filter_by(job_id=job_id).first()
            if job:
                job.status = status
                job.updated_at = datetime.now(KL_TZ).isoformat()
                if result is not None:
                    job.result = result
                if error is not None:
                    job.error = error
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
                Job.type == job_type,
                Job.status.in_(['pending', 'running'])
            ).first()
            return job.to_dict() if job else None
        finally:
            session.close()

    def run_in_background(self, job_id: int, func: Callable, *args, **kwargs):
        def wrapper():
            self.update_job(job_id, 'running')
            try:
                result = func(*args, **kwargs)
                self.update_job(job_id, 'completed', result=result)
            except Exception as e:
                error_msg = str(e).split('Call log:')[0].strip()
                self.update_job(job_id, 'failed', error=error_msg)

        thread = threading.Thread(target=wrapper, daemon=True)
        thread.start()


job_manager = JobManager()
