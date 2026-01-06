from typing import List, Dict, Any, Optional

from src.core.loader import load_settings
from src.core.logger import get_logger
from src.services.client import SheetsClient

logger = get_logger(__name__)

DATA_START_ROW = 4


class JobSheetService:
    _client: Optional[SheetsClient] = None
    _jobs_sheet: Optional[str] = None

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
    def _build_row(cls, job: Dict[str, Any]) -> List[Any]:
        date_range = f"{job.get('from_date', '')} - {job.get('to_date', '')}"
        return [
            job.get('id', ''),
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
    def _find_row_by_job_id(cls, worksheet, job_id: int) -> Optional[int]:
        job_ids = worksheet.col_values(1)
        
        for idx, cell_value in enumerate(job_ids):
            if idx < DATA_START_ROW - 1:
                continue
            if str(cell_value) == str(job_id):
                return idx + 1
        
        return None


    @classmethod
    def update_job_by_id(cls, job: Dict[str, Any]) -> bool:
        job_id = job.get('id')
        if not job_id:
            logger.warning("Cannot update job: missing job id")
            return False

        try:
            client = cls.get_client()
            jobs_sheet = cls.get_jobs_sheet_name()
            worksheet = client.spreadsheet.worksheet(jobs_sheet)

            row_index = cls._find_row_by_job_id(worksheet, job_id)
            if row_index is None:
                logger.warning(f"Job {job_id} not found in sheet")
                return False

            row = [cls._build_row(job)]
            client.write_data(jobs_sheet, row, f'A{row_index}')
            logger.debug(f"Updated job {job_id} at row {row_index}")
            return True

        except Exception as e:
            logger.error(f"Failed to update job {job_id}: {e}")
            return False

    @classmethod
    def append_job(cls, job: Dict[str, Any]):
        try:
            client = cls.get_client()
            jobs_sheet = cls.get_jobs_sheet_name()
            worksheet = client.spreadsheet.worksheet(jobs_sheet)

            job_ids = worksheet.col_values(1)
            next_row = max(len(job_ids) + 1, DATA_START_ROW)

            row = [cls._build_row(job)]
            client.write_data(jobs_sheet, row, f'A{next_row}')
            logger.debug(f"Appended job {job.get('id')} at row {next_row}")

        except Exception as e:
            logger.error(f"Failed to append job: {e}")
