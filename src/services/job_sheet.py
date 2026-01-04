from typing import List, Dict, Any

from src.core.logger import get_logger
from src.services.client import SheetsClient

logger = get_logger(__name__)

JOBS_SHEET = 'Jobs'
JOBS_HEADER_ROW = 3
JOBS_START_ROW = 4


class JobSheetService:
    _client = None
    
    @classmethod
    def get_client(cls) -> SheetsClient:
        if cls._client is None:
            cls._client = SheetsClient()
        return cls._client
    
    @classmethod
    def update_jobs_sheet(cls, jobs: List[Dict[str, Any]]):
        if not jobs:
            return
        
        try:
            client = cls.get_client()
            
            rows = []
            for j in jobs:
                date_range = f"{j.get('from_date', '')} - {j.get('to_date', '')}"
                rows.append([
                    j.get('run_id', ''),
                    j.get('job_type', ''),
                    j.get('platform', ''),
                    j.get('account_label', ''),
                    date_range,
                    j.get('status', ''),
                    j.get('transactions_count', 0) or 0,
                    j.get('desc', ''),
                    j.get('created_at', ''),
                    j.get('updated_at', ''),
                ])
            
            clear_range = f'A{JOBS_START_ROW}:J300'
            worksheet = client.spreadsheet.worksheet(JOBS_SHEET)
            worksheet.batch_clear([clear_range])
            
            client.write_data(JOBS_SHEET, rows, f'A{JOBS_START_ROW}')
            logger.debug(f"Updated Jobs sheet with {len(rows)} rows")
            
        except Exception as e:
            logger.error(f"Failed to update Jobs sheet: {e}")
    
    @classmethod
    def update_single_job(cls, job: Dict[str, Any], row_index: int):
        try:
            client = cls.get_client()
            
            date_range = f"{job.get('from_date', '')} - {job.get('to_date', '')}"
            row = [[
                job.get('run_id', ''),
                job.get('job_type', ''),
                job.get('platform', ''),
                job.get('account_label', ''),
                date_range,
                job.get('status', ''),
                job.get('transactions_count', 0) or 0,
                job.get('desc', ''),
                job.get('created_at', ''),
                job.get('updated_at', ''),
            ]]
            
            client.write_data(JOBS_SHEET, row, f'A{row_index}')
            
        except Exception as e:
            logger.error(f"Failed to update single job row: {e}")
