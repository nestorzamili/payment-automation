import re
from datetime import datetime
from pathlib import Path
from typing import List

import pandas as pd
from sqlalchemy.dialects.sqlite import insert

from src.core.database import get_session
from src.core.models import PGTransaction
from src.core.logger import get_logger
from src.parser.helper import get_parsed_date_ranges, extract_date_range_from_filename, create_pending_parse_job, start_running_parse_job, complete_parse_job, fail_parse_job, normalize_channel, _append_job_to_sheet

logger = get_logger(__name__)


class AxaiParser:
    
    COLUMNS = {
        'order number': 'transaction_id',
        'Payment Time': 'transaction_date',
        'Payment Amount': 'amount',
        'Payment channels': 'channel'
    }
    
    def parse_file(self, file_path: Path, account_label: str) -> List[dict]:
        df = pd.read_excel(file_path)
        transactions = []
        
        for _, row in df.iterrows():
            try:
                channel = self._extract_channel(row['Payment channels'])
                tx = {
                    'transaction_id': str(row['Order Number']),
                    'transaction_date': self._parse_date(row['Payment Time']),
                    'amount': float(row['Payment Amount']),
                    'channel': channel,
                    'account_label': account_label
                }
                transactions.append(tx)
            except Exception as e:
                logger.warning(f"Error parsing row: {e}")
                continue
        
        return transactions
    
    def _extract_channel(self, value: str) -> str:
        match = re.search(r'\s+(\w+)\s*\(', str(value))
        if match:
            raw_channel = match.group(1)
            return normalize_channel(raw_channel)
        return normalize_channel(str(value))
    
    def _parse_date(self, date_value) -> str:
        if isinstance(date_value, datetime):
            return date_value.strftime('%Y-%m-%d %H:%M:%S')
        elif isinstance(date_value, pd.Timestamp):
            return date_value.to_pydatetime().strftime('%Y-%m-%d %H:%M:%S')
        else:
            date_str = str(date_value).strip()
            formats = [
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%d %H:%M',
                '%d/%m/%Y %H:%M:%S',
            ]
            for fmt in formats:
                try:
                    dt = datetime.strptime(date_str, fmt)
                    return dt.strftime('%Y-%m-%d %H:%M:%S')
                except ValueError:
                    continue
            raise ValueError(f"Cannot parse date: {date_value}")
    
    def save_transactions(self, transactions: List[dict]) -> int:
        if not transactions:
            return 0
        
        session = get_session()
        inserted_count = 0
        
        try:
            for tx in transactions:
                stmt = insert(PGTransaction).values(
                    transaction_id=tx['transaction_id'],
                    transaction_date=tx['transaction_date'],
                    amount=tx['amount'],
                    platform='axai',
                    channel=tx['channel'],
                    account_label=tx['account_label']
                ).on_conflict_do_nothing(
                    index_elements=['transaction_id']
                )
                result = session.execute(stmt)
                inserted_count += result.rowcount
            
            session.commit()
            return inserted_count
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()
    
    def process_directory(self, directory: Path, account_label: str, run_id: str = None) -> dict:
        excel_files = list(directory.glob('*.xlsx'))
        excel_files = [f for f in excel_files if not f.name.startswith('~$')]
        
        parsed_ranges = get_parsed_date_ranges(account_label, 'axai')
        new_files = []
        for f in excel_files:
            from_date, to_date = extract_date_range_from_filename(f.name)
            if from_date and to_date and (from_date, to_date) not in parsed_ranges:
                new_files.append((f, from_date, to_date))
        
        result = {
            'account_label': account_label,
            'files_processed': 0,
            'files_skipped': len(excel_files) - len(new_files),
            'total_transactions': 0
        }
        
        pending_jobs = []
        for file_path, from_date, to_date in new_files:
            job_id = create_pending_parse_job(from_date, to_date, account_label, 'axai', run_id)
            pending_jobs.append((job_id, file_path, from_date, to_date))
            _append_job_to_sheet(job_id)
        
        for job_id, file_path, from_date, to_date in pending_jobs:
            start_running_parse_job(job_id, run_id)
            
            try:
                logger.info(f"Processing: {file_path.name}")
                transactions = self.parse_file(file_path, account_label)
                
                if transactions:
                    saved = self.save_transactions(transactions)
                    result['files_processed'] += 1
                    result['total_transactions'] += saved
                    complete_parse_job(job_id, len(transactions), saved)
                else:
                    complete_parse_job(job_id, 0, 0)
            except Exception as e:
                fail_parse_job(job_id, str(e))
                logger.error(f"Error processing {file_path.name}: {e}")
        
        return result

