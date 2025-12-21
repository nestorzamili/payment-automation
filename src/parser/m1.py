import re
from datetime import datetime
from pathlib import Path
from typing import List

import pandas as pd
from sqlalchemy.dialects.sqlite import insert

from src.core.database import get_session
from src.core.models import PGTransaction
from src.core.logger import get_logger
from src.parser.helper import get_parsed_files, record_parsed_file

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
            return match.group(1).replace('_', ' ').title()
        return "Unknown"
    
    def _parse_fpx(self, file_path: Path, account_label: str) -> List[dict]:
        df = pd.read_excel(file_path)
        transactions = []
        
        for _, row in df.iterrows():
            try:
                tx = {
                    'transaction_id': str(row['merchantOrderNo']),
                    'transaction_date': self._parse_date(row['createdDate']),
                    'amount': float(row['transactionAmount']),
                    'transaction_type': 'FPX',
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
                    'transaction_type': 'EWALLET',
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
                    transaction_type=tx['transaction_type'],
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
    
    def process_directory(self, directory: Path, account_label: str) -> dict:
        excel_files = list(directory.glob('*.xlsx'))
        excel_files = [f for f in excel_files if not f.name.startswith('~$')]
        
        # Skip already-parsed files
        parsed_files = get_parsed_files(account_label, 'm1')
        new_files = [f for f in excel_files if f.name not in parsed_files]
        
        result = {
            'account_label': account_label,
            'files_processed': 0,
            'files_skipped': len(excel_files) - len(new_files),
            'total_transactions': 0,
            'by_type': {}
        }
        
        for file_path in new_files:
            logger.info(f"Processing: {file_path.name}")
            transactions = self.parse_file(file_path, account_label)
            
            if transactions:
                saved = self.save_transactions(transactions)
                result['files_processed'] += 1
                result['total_transactions'] += saved
                
                tx_type = transactions[0]['transaction_type']
                channel = transactions[0]['channel']
                key = f"{tx_type}:{channel}"
                result['by_type'][key] = result['by_type'].get(key, 0) + saved
                
                # Record the parsed file
                record_parsed_file(file_path.name, account_label, 'm1', saved)
        
        return result

