from typing import List, Dict, Any, Optional

from src.core.loader import load_settings
from src.core.logger import get_logger
from src.services.client import SheetsClient

logger = get_logger(__name__)

DATA_START_ROW = 4
DATA_RANGE = 'A4:L500'


class JobSheetService:
    _client: Optional[SheetsClient] = None
    _jobs_sheet: Optional[str] = None
    _row_cache: Dict[int, int] = {}
    _next_row: int = DATA_START_ROW

    @classmethod
    def get_jobs_sheet_name(cls) -> str:
        if cls._jobs_sheet is None:
            settings = load_settings()
            cls._jobs_sheet = settings['google_sheets']['sheets'].get('jobs', 'Jobs')
        return cls._jobs_sheet

    @classmethod
    def get_client(cls) -> SheetsClient:
        if cls._client is None:
            cls._client = SheetsClient()
        return cls._client

    @classmethod
    def reset_cache(cls):
        cls._row_cache = {}
        cls._next_row = DATA_START_ROW

    @classmethod
    def clear_sheet(cls):
        try:
            client = cls.get_client()
            jobs_sheet = cls.get_jobs_sheet_name()
            worksheet = client.spreadsheet.worksheet(jobs_sheet)
            worksheet.batch_clear([DATA_RANGE])
            cls.reset_cache()
            logger.debug("Cleared Jobs sheet")
        except Exception as e:
            logger.error(f"Failed to clear Jobs sheet: {e}")

    @classmethod
    def _build_row(cls, job: Dict[str, Any]) -> List[Any]:
        date_range = f"{job.get('from_date', '')} - {job.get('to_date', '')}"
        return [
            job.get('job_id', ''),
            job.get('job_type', ''),
            job.get('platform', ''),
            job.get('account_label', ''),
            job.get('source_type', ''),
            date_range,
            job.get('status', ''),
            job.get('fetched_count', 0) or 0,
            job.get('stored_count', 0) or 0,
            job.get('error_message', ''),
            job.get('created_at', ''),
            job.get('updated_at', ''),
        ]

    @classmethod
    def update_job_by_id(cls, job: Dict[str, Any]) -> bool:
        job_id = job.get('job_id')
        if not job_id:
            logger.warning("Cannot update job: missing job id")
            return False

        row_index = cls._row_cache.get(job_id)
        if row_index is None:
            logger.debug(f"Job {job_id} not in cache, skipping sheet update")
            return False

        try:
            client = cls.get_client()
            jobs_sheet = cls.get_jobs_sheet_name()
            row = [cls._build_row(job)]
            client.write_data(jobs_sheet, row, f'A{row_index}')
            logger.debug(f"Updated job {job_id} at row {row_index}")
            return True

        except Exception as e:
            logger.error(f"Failed to update job {job_id}: {e}")
            return False

    @classmethod
    def append_job(cls, job: Dict[str, Any]):
        job_id = job.get('job_id')
        if not job_id:
            return

        try:
            client = cls.get_client()
            jobs_sheet = cls.get_jobs_sheet_name()

            row_index = cls._next_row
            cls._row_cache[job_id] = row_index
            cls._next_row += 1

            row = [cls._build_row(job)]
            client.write_data(jobs_sheet, row, f'A{row_index}')
            logger.debug(f"Appended job {job_id} at row {row_index}")

        except Exception as e:
            logger.error(f"Failed to append job: {e}")
