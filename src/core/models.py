import json
from datetime import datetime
from typing import List
from zoneinfo import ZoneInfo

from sqlalchemy import Column, String, Float, DateTime, Text, Integer

from src.core.database import Base

KL_TZ = ZoneInfo('Asia/Kuala_Lumpur')

def _now_kl():
    return datetime.now(KL_TZ).strftime('%Y-%m-%d %H:%M:%S')


class Job(Base):
    __tablename__ = 'jobs'

    job_id = Column(Integer, primary_key=True, autoincrement=True)
    job_type = Column(String(50), nullable=False)
    platform = Column(String(20))  # 'kira', 'm1', 'axai', 'fiuu'
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
        
        if self.platform:
            result['platform'] = self.platform
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
    mdr = Column(Float)
    settlement_amount = Column(Float)
    merchant = Column(String(100))
    created_at = Column(String(19), default=_now_kl)

    def to_dict(self) -> dict:
        return {
            'transaction_id': self.transaction_id,
            'transaction_date': self.transaction_date,
            'amount': self.amount,
            'payment_method': self.payment_method,
            'mdr': self.mdr,
            'settlement_amount': self.settlement_amount,
            'merchant': self.merchant
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
    created_at = Column(String(19), default=_now_kl)

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


class MerchantLedger(Base):
    __tablename__ = 'merchant_ledger'

    merchant_ledger_id = Column(Integer, primary_key=True, autoincrement=True)
    merchant = Column(String(100), nullable=False, index=True)
    transaction_date = Column(String(10), nullable=False, index=True)
    
    fpx = Column(Float, default=0)
    fee_fpx = Column(Float, default=0)
    gross_fpx = Column(Float, default=0)
    ewallet = Column(Float, default=0)
    fee_ewallet = Column(Float, default=0)
    gross_ewallet = Column(Float, default=0)
    total_gross = Column(Float, default=0)
    total_fee = Column(Float, default=0)
    cum_fpx = Column(Float, default=0)
    cum_ewallet = Column(Float, default=0)
    cum_total = Column(Float, default=0)
    
    settlement_fund = Column(Float)
    settlement_charges = Column(Float)
    withdrawal_amount = Column(Float)
    withdrawal_charges = Column(Float)
    topup_payout_pool = Column(Float)
    payout_pool_balance = Column(Float)
    available_balance = Column(Float)
    total_balance = Column(Float)
    remarks = Column(Text)
    
    updated_at = Column(String(19), default=_now_kl, onupdate=_now_kl)

    __table_args__ = (
        {'sqlite_autoincrement': True},
    )

    def to_dict(self) -> dict:
        return {
            'merchant_ledger_id': self.merchant_ledger_id,
            'merchant': self.merchant,
            'transaction_date': self.transaction_date,
            'fpx': self.fpx,
            'fee_fpx': self.fee_fpx,
            'gross_fpx': self.gross_fpx,
            'ewallet': self.ewallet,
            'fee_ewallet': self.fee_ewallet,
            'gross_ewallet': self.gross_ewallet,
            'total_gross': self.total_gross,
            'total_fee': self.total_fee,
            'cum_fpx': self.cum_fpx,
            'cum_ewallet': self.cum_ewallet,
            'cum_total': self.cum_total,
            'settlement_fund': self.settlement_fund,
            'settlement_charges': self.settlement_charges,
            'withdrawal_amount': self.withdrawal_amount,
            'withdrawal_charges': self.withdrawal_charges,
            'topup_payout_pool': self.topup_payout_pool,
            'payout_pool_balance': self.payout_pool_balance,
            'available_balance': self.available_balance,
            'total_balance': self.total_balance,
            'remarks': self.remarks,
            'updated_at': self.updated_at
        }
