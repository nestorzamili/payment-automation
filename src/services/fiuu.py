import hashlib
from datetime import datetime
from typing import List

import requests
from sqlalchemy.dialects.sqlite import insert

from src.core.database import get_session
from src.core.loader import load_settings
from src.core.logger import get_logger
from src.core.models import PGTransaction

logger = get_logger(__name__)


class FiuuAPIClient:
    
    ENDPOINT_DAILY = "/RMS/API/PSQ/psq-daily.php"
    VERSION = 4
    STATUS_SUCCESS = "00"
    
    def __init__(self, account: dict):
        self.account = account
        self.label = account['label']
        self.base_url = account['base_url']
        self.merchant_id = account['credentials']['merchant_id']
        self.private_key = account['credentials']['private_key']
        
        settings = load_settings()
        self.timeout = settings['browser'].get('timeout', 60000) // 1000
    
    def _generate_signature(self, rdate: str) -> str:
        data = f"{rdate}{self.merchant_id}{self.private_key}"
        return hashlib.md5(data.encode()).hexdigest()
    
    def _normalize_channel(self, channel: str) -> str:
        if 'fpx' in channel.lower():
            return 'FPX'
        return 'ewallet'
    
    def _calculate_duration_seconds(self, from_date: str, to_date: str) -> int:
        from_dt = datetime.strptime(from_date, '%Y-%m-%d')
        to_dt = datetime.strptime(to_date, '%Y-%m-%d')
        days_count = (to_dt - from_dt).days + 1
        return days_count * 86400

    def fetch_transactions(self, from_date: str, to_date: str) -> List[dict]:
        duration_seconds = self._calculate_duration_seconds(from_date, to_date)
        signature = self._generate_signature(from_date)
        
        params = {
            'rdate': from_date,
            'rduration': duration_seconds,
            'status': self.STATUS_SUCCESS,
            'version': self.VERSION,
            'response_type': 'json',
            'merchantID': self.merchant_id,
            'skey': signature
        }
        
        url = f"{self.base_url}{self.ENDPOINT_DAILY}"
        
        logger.info(f"Fetching Fiuu: {self.label} ({from_date} to {to_date}, {duration_seconds}s)")
        
        response = requests.get(url, params=params, timeout=self.timeout)
        response.raise_for_status()
        
        data = response.json()
        
        if isinstance(data, list):
            logger.info(f"Fetched {len(data)} transactions: {self.label}")
            return data
        elif isinstance(data, dict) and 'error' in data:
            logger.error(f"API error: {data.get('error')}")
            return []
        else:
            logger.warning(f"Unexpected response: {type(data)}")
            return []
    
    def save_transactions(self, transactions: List[dict]) -> int:
        if not transactions:
            return 0
        
        session = get_session()
        inserted_count = 0
        
        try:
            for tx in transactions:
                stmt = insert(PGTransaction).values(
                    transaction_id=tx['OrderID'],
                    transaction_date=tx['BillingDate'],
                    amount=float(tx['Amount']),
                    platform='fiuu',
                    channel=self._normalize_channel(tx.get('Channel', '')),
                    account_label=self.label
                ).on_conflict_do_nothing(
                    index_elements=['transaction_id']
                )
                result = session.execute(stmt)
                inserted_count += result.rowcount
            
            session.commit()
            logger.info(f"Saved {inserted_count} transactions: {self.label}")
            return inserted_count
        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.close()
    
    def fetch_and_store(self, from_date: str, to_date: str) -> tuple[int, int]:
        logger.info(f"Fetching Fiuu: {self.label} ({from_date} to {to_date})")
        
        transactions = self.fetch_transactions(from_date, to_date)
        stored = self.save_transactions(transactions)
        return len(transactions), stored
