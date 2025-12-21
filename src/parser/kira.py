from datetime import datetime
from pathlib import Path
from typing import List

import pandas as pd
from sqlalchemy.dialects.sqlite import insert

from src.core.database import get_session
from src.core.models import KiraTransaction
from src.core.logger import get_logger
from src.parser.helper import get_parsed_files, record_parsed_file

logger = get_logger(__name__)


class KiraParser:
    
    def parse_file(self, file_path: Path) -> List[dict]:
        df = pd.read_excel(file_path)
        transactions = []
        
        for _, row in df.iterrows():
            try:
                tx = {
                    'transaction_id': str(row.get('Transaction ID', '')),
                    'transaction_date': self._parse_date(row.get('Created On')),
                    'amount': float(row.get('Transaction Amount', 0)),
                    'payment_method': self._normalize_payment_method(row.get('Payment Method'))
                }
                transactions.append(tx)
            except Exception as e:
                logger.warning(f"Error parsing row: {e}")
                continue
        
        return transactions
    
    def _normalize_payment_method(self, value) -> str:
        if not value:
            return 'ewallet'
        
        s = str(value).upper().strip()
        
        if s == 'FPX' or 'FPX B2C' in s or 'CASA' in s:
            return 'FPX'
        if s == 'FPXC' or 'FPX B2B' in s:
            return 'FPXC'
        if s == 'TNG' or 'TOUCH' in s or 'TOUCHNGO' in s:
            return 'TNG'
        if s == 'BOOST' or 'BOOST' in s:
            return 'BOOST'
        if s == 'SHOPEE' or 'SHOPEE' in s:
            return 'Shopee'
        
        return 'ewallet'
    
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
                '%d/%m/%Y %H:%M',
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
                stmt = insert(KiraTransaction).values(
                    transaction_id=tx['transaction_id'],
                    transaction_date=tx['transaction_date'],
                    amount=tx['amount'],
                    payment_method=tx['payment_method']
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
    
    def process_directory(self, directory: Path) -> dict:
        excel_files = list(directory.glob('*.xlsx'))
        excel_files = [f for f in excel_files if not f.name.startswith('~$')]
        
        # Skip already-parsed files
        parsed_files = get_parsed_files(platform='kira')
        new_files = [f for f in excel_files if f.name not in parsed_files]
        
        result = {
            'files_processed': 0,
            'files_skipped': len(excel_files) - len(new_files),
            'total_transactions': 0
        }
        
        for file_path in new_files:
            logger.info(f"Processing: {file_path.name}")
            transactions = self.parse_file(file_path)
            
            if transactions:
                saved = self.save_transactions(transactions)
                result['files_processed'] += 1
                result['total_transactions'] += saved
                
                # Record the parsed file
                record_parsed_file(file_path.name, None, 'kira', saved)
        
        return result

