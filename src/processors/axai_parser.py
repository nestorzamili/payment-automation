import re
from datetime import datetime
from pathlib import Path
from typing import List

import pandas as pd
from sqlalchemy.dialects.sqlite import insert

from src.core.database import get_session
from src.core.models import AxaiTransaction
from src.core.logger import get_logger

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
                tx = {
                    'transaction_id': str(row['Order Number']),
                    'transaction_date': self._parse_date(row['Payment Time']),
                    'amount': float(row['Payment Amount']),
                    'channel': self._extract_channel(row['Payment channels']),
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
            return match.group(1)
        return str(value)
    
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
                stmt = insert(AxaiTransaction).values(
                    transaction_id=tx['transaction_id'],
                    transaction_date=tx['transaction_date'],
                    amount=tx['amount'],
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
        
        result = {
            'account_label': account_label,
            'files_processed': 0,
            'total_transactions': 0
        }
        
        for file_path in excel_files:
            logger.info(f"Processing: {file_path.name}")
            transactions = self.parse_file(file_path, account_label)
            
            if transactions:
                saved = self.save_transactions(transactions)
                result['files_processed'] += 1
                result['total_transactions'] += saved
        
        return result
