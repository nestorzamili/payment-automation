import re
from datetime import datetime
from pathlib import Path
from typing import List

import pandas as pd
from sqlalchemy.dialects.sqlite import insert

from src.core.database import get_session
from src.core.models import PGTransaction
from src.core.logger import get_logger
from src.parser.helper import get_parsed_date_ranges, extract_date_range_from_filename, create_pending_parse_job, start_running_parse_job, complete_parse_job, fail_parse_job, normalize_channel, _update_jobs_sheet

logger = get_logger(__name__)


class M1Parser:
    
    FPX_COLUMNS = {
        'merchantOrderNo': 'transaction_id',
        'createdDate': 'transaction_date',
        'transactionAmount': 'amount'
    }
    
    EWALLET_COLUMNS = {
        'merchantOrderNo': 'transaction_id',
        'Date': 'transaction_date',
        'Amount': 'amount'
    }
    
    def parse_file(self, file_path: Path, account_label: str) -> List[dict]:
        filename = file_path.name.lower()
        
        if '_fpx_' in filename:
            return self._parse_fpx(file_path, account_label)
        elif '_ewallet_' in filename:
            channel = self._extract_channel(filename)
            return self._parse_ewallet(file_path, account_label, channel)
        else:
            logger.warning(f"Unknown file type: {filename}")
            return []
    
    def _extract_channel(self, filename: str) -> str:
        match = re.search(r'_ewallet_([a-z_]+)_\d{4}', filename)
        if match:
            raw_channel = match.group(1).replace('_', ' ').title()
            return normalize_channel(raw_channel)
        return "ewallet"
    
    def _parse_fpx(self, file_path: Path, account_label: str) -> List[dict]:
        df = pd.read_excel(file_path)
        transactions = []
        
        for _, row in df.iterrows():
            try:
                tx = {
                    'transaction_id': str(row['merchantOrderNo']),
                    'transaction_date': self._parse_date(row['createdDate']),
                    'amount': float(row['transactionAmount']),
                    'channel': 'FPX',
                    'account_label': account_label
                }
                transactions.append(tx)
            except Exception as e:
                logger.warning(f"Error parsing row: {e}")
                continue
        
        return transactions
    
    def _parse_ewallet(self, file_path: Path, account_label: str, channel: str) -> List[dict]:
        df = pd.read_excel(file_path)
        transactions = []
        
        for _, row in df.iterrows():
            try:
                tx = {
                    'transaction_id': str(row['merchantOrderNo']),
                    'transaction_date': self._parse_date(row['Date']),
                    'amount': float(row['Amount']),
                    'channel': channel,
                    'account_label': account_label
                }
                transactions.append(tx)
            except Exception as e:
                logger.warning(f"Error parsing row: {e}")
                continue
        
        return transactions
    
    def _parse_date(self, date_value) -> str:
        dt = None
        
        if isinstance(date_value, datetime):
            dt = date_value
        elif isinstance(date_value, pd.Timestamp):
            dt = date_value.to_pydatetime()
        else:
            date_str = str(date_value).strip()
            
            formats = [
                '%H:%M %Y-%m-%d',
                '%Y-%m-%d %H:%M:%S',
                '%Y-%m-%d %H:%M',
                '%d/%m/%Y %H:%M:%S',
            ]
            
            for fmt in formats:
                try:
                    dt = datetime.strptime(date_str, fmt)
                    break
                except ValueError:
                    continue
        
        if dt is None:
            raise ValueError(f"Cannot parse date: {date_value}")
        
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    
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
                    platform='m1',
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
        
        parsed_ranges = get_parsed_date_ranges(account_label, 'm1')
        new_files = []
        for f in excel_files:
            from_date, to_date = extract_date_range_from_filename(f.name)
            if from_date and to_date and (from_date, to_date) not in parsed_ranges:
                new_files.append((f, from_date, to_date))
        
        result = {
            'account_label': account_label,
            'files_processed': 0,
            'files_skipped': len(excel_files) - len(new_files),
            'total_transactions': 0,
            'by_type': {}
        }
        
        pending_jobs = []
        for file_path, from_date, to_date in new_files:
            job_id = create_pending_parse_job(from_date, to_date, account_label, 'm1', run_id)
            pending_jobs.append((job_id, file_path, from_date, to_date))
        
        if pending_jobs:
            _update_jobs_sheet(run_id)
        
        for job_id, file_path, from_date, to_date in pending_jobs:
            start_running_parse_job(job_id, run_id)
            
            try:
                logger.info(f"Processing: {file_path.name}")
                transactions = self.parse_file(file_path, account_label)
                
                if transactions:
                    saved = self.save_transactions(transactions)
                    result['files_processed'] += 1
                    result['total_transactions'] += saved
                    
                    channel = transactions[0]['channel']
                    result['by_type'][channel] = result['by_type'].get(channel, 0) + saved
                    
                    complete_parse_job(job_id, len(transactions), saved)
                else:
                    complete_parse_job(job_id, 0, 0)
            except Exception as e:
                fail_parse_job(job_id, str(e))
                logger.error(f"Error processing {file_path.name}: {e}")
        
        return result

