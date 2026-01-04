import hashlib
from datetime import datetime, timedelta
from typing import List

import requests
from sqlalchemy import and_
from sqlalchemy.dialects.sqlite import insert

from src.core.database import get_session
from src.core.loader import load_settings
from src.core.logger import get_logger
from src.core.models import PGTransaction, Job

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
    
    def _get_fetched_dates(self) -> set:
        session = get_session()
        try:
            jobs = session.query(Job).filter(
                and_(
                    Job.job_type == 'download',
                    Job.platform == 'fiuu',
                    Job.account_label == self.label,
                    Job.status == 'completed'
                )
            ).all()
            
            fetched_dates = set()
            for job in jobs:
                if job.from_date and job.to_date:
                    from_dt = datetime.strptime(job.from_date, '%Y-%m-%d')
                    to_dt = datetime.strptime(job.to_date, '%Y-%m-%d')
                    current = from_dt
                    while current <= to_dt:
                        fetched_dates.add(current.strftime('%Y-%m-%d'))
                        current += timedelta(days=1)
            
            return fetched_dates
        finally:
            session.close()
    
    def _get_unfetched_dates(self, from_date: str, to_date: str) -> List[str]:
        fetched = self._get_fetched_dates()
        
        from_dt = datetime.strptime(from_date, '%Y-%m-%d')
        to_dt = datetime.strptime(to_date, '%Y-%m-%d')
        
        unfetched = []
        current = from_dt
        while current <= to_dt:
            date_str = current.strftime('%Y-%m-%d')
            if date_str not in fetched:
                unfetched.append(date_str)
            current += timedelta(days=1)
        
        return unfetched
    
    def fetch_transactions(self, from_date: str, to_date: str) -> List[dict]:
        from_dt = datetime.strptime(from_date, '%Y-%m-%d')
        to_dt = datetime.strptime(to_date, '%Y-%m-%d')
        
        duration_seconds = int((to_dt - from_dt).total_seconds()) + 86400
        
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
        
        logger.info(f"Fetching Fiuu: {self.label} ({from_date} to {to_date})")
        
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
    
    def fetch_and_store(self, from_date: str, to_date: str) -> int:
        unfetched = self._get_unfetched_dates(from_date, to_date)
        
        if not unfetched:
            logger.info(f"All dates already fetched: {self.label} ({from_date} to {to_date})")
            return 0
        
        logger.info(f"Unfetched dates: {len(unfetched)} of {self.label}")
        
        total_saved = 0
        for date in unfetched:
            transactions = self.fetch_transactions(date, date)
            saved = self.save_transactions(transactions)
            total_saved += saved
        
        return total_saved
