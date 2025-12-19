import threading
from datetime import datetime
from typing import Callable, List
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

    def create_job(
        self, 
        job_type: str, 
        account_label: str = None,
        from_date: str = None,
        to_date: str = None
    ) -> int:
        now = datetime.now(KL_TZ).isoformat()
        
        session = get_session()
        try:
            job = Job(
                job_type=job_type,
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
        files: List[str] = None,
        duration_seconds: float = None,
        error: str = None
    ):
        session = get_session()
        try:
            job = session.query(Job).filter_by(job_id=job_id).first()
            if job:
                job.status = status
                job.updated_at = datetime.now(KL_TZ).isoformat()
                if files is not None:
                    job.files = files
                if duration_seconds is not None:
                    job.duration_seconds = duration_seconds
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
                Job.job_type == job_type,
                Job.status.in_(['pending', 'running'])
            ).first()
            return job.to_dict() if job else None
        finally:
            session.close()

    def run_in_background(self, job_id: int, func: Callable, *args, **kwargs):
        def wrapper():
            self.update_job(job_id, 'running')
            start_time = datetime.now(KL_TZ)
            try:
                result = func(*args, **kwargs)
                duration = (datetime.now(KL_TZ) - start_time).total_seconds()
                self.update_job(
                    job_id, 
                    'completed', 
                    files=result.get('files', []),
                    duration_seconds=duration
                )
            except Exception as e:
                duration = (datetime.now(KL_TZ) - start_time).total_seconds()
                error_msg = str(e).split('Call log:')[0].strip()
                self.update_job(job_id, 'failed', error=error_msg, duration_seconds=duration)

        thread = threading.Thread(target=wrapper, daemon=True)
        thread.start()


job_manager = JobManager()
