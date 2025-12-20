import json
from datetime import datetime
from typing import List

from sqlalchemy import Column, String, Float, DateTime, Text, Integer

from src.core.database import Base


class Job(Base):
    __tablename__ = 'jobs'

    job_id = Column(Integer, primary_key=True, autoincrement=True)
    job_type = Column(String(50), nullable=False)
    account_label = Column(String(100))
    from_date = Column(String(10))
    to_date = Column(String(10))
    status = Column(String(20), nullable=False, default='pending')
    error = Column(Text)
    files_json = Column(Text)
    file_count = Column(Integer, default=0)
    duration_seconds = Column(Float)
    created_at = Column(String(30), nullable=False)
    updated_at = Column(String(30), nullable=False)

    @property
    def files(self) -> List[str]:
        return json.loads(self.files_json) if self.files_json else []

    @files.setter
    def files(self, value: List[str]):
        self.files_json = json.dumps(value) if value else None
        self.file_count = len(value) if value else 0

    def to_dict(self) -> dict:
        result = {
            'job_id': self.job_id,
            'job_type': self.job_type,
            'status': self.status,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
        }
        
        if self.account_label:
            result['account_label'] = self.account_label
        if self.from_date:
            result['from_date'] = self.from_date
        if self.to_date:
            result['to_date'] = self.to_date
        if self.error:
            result['error'] = self.error
        if self.files_json:
            result['files'] = self.files
            result['file_count'] = self.file_count
        if self.duration_seconds is not None:
            result['duration_seconds'] = self.duration_seconds
            
        return result


class KiraTransaction(Base):
    __tablename__ = 'kira_transactions'

    transaction_id = Column(String(50), primary_key=True)
    transaction_date = Column(String(19), nullable=False, index=True)
    amount = Column(Float, nullable=False)
    payment_method = Column(String(20), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    def to_dict(self) -> dict:
        return {
            'transaction_id': self.transaction_id,
            'transaction_date': self.transaction_date,
            'amount': self.amount,
            'payment_method': self.payment_method
        }


class PGTransaction(Base):
    __tablename__ = 'pg_transactions'

    transaction_id = Column(String(50), primary_key=True)
    transaction_date = Column(String(19), nullable=False, index=True)
    amount = Column(Float, nullable=False)
    platform = Column(String(20), nullable=False)
    transaction_type = Column(String(20))
    channel = Column(String(50), nullable=False)
    account_label = Column(String(50), nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    def to_dict(self) -> dict:
        return {
            'transaction_id': self.transaction_id,
            'transaction_date': self.transaction_date,
            'amount': self.amount,
            'platform': self.platform,
            'transaction_type': self.transaction_type,
            'channel': self.channel,
            'account_label': self.account_label
        }
