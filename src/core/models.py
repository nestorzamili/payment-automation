import json
from datetime import datetime
from typing import List
from zoneinfo import ZoneInfo

from sqlalchemy import Column, String, Float, Text, Integer

from src.core.database import Base

KL_TZ = ZoneInfo('Asia/Kuala_Lumpur')

def _now_kl():
    return datetime.now(KL_TZ).strftime('%Y-%m-%d %H:%M:%S')


class Job(Base):
    __tablename__ = 'jobs'

    job_id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String(36), index=True)
    job_type = Column(String(50), nullable=False)
    platform = Column(String(20))
    account_label = Column(String(100))
    from_date = Column(String(10))
    to_date = Column(String(10))
    status = Column(String(20), nullable=False, default='pending')
    filename = Column(String(255))
    transactions_count = Column(Integer, default=0)
    desc = Column(Text)
    created_at = Column(String(30), nullable=False)
    updated_at = Column(String(30), nullable=False)

    def to_dict(self) -> dict:
        return {
            'job_id': self.job_id,
            'run_id': self.run_id,
            'job_type': self.job_type,
            'platform': self.platform,
            'account_label': self.account_label,
            'from_date': self.from_date,
            'to_date': self.to_date,
            'status': self.status,
            'filename': self.filename,
            'transactions_count': self.transactions_count,
            'desc': self.desc,
            'created_at': self.created_at,
            'updated_at': self.updated_at,
        }


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


class Transaction(Base):
    __tablename__ = 'transactions'
    
    transaction_id = Column(Integer, primary_key=True, autoincrement=True)
    merchant = Column(String(100), nullable=False, index=True)
    pg_account_label = Column(String(100), index=True)
    transaction_date = Column(String(10), nullable=False, index=True)
    channel = Column(String(20), nullable=False)
    kira_amount = Column(Float, default=0)
    pg_amount = Column(Float, default=0)
    mdr = Column(Float, default=0)
    kira_settlement_amount = Column(Float, default=0)
    volume = Column(Integer, default=0)
    settlement_date = Column(String(10))
    created_at = Column(String(19), default=_now_kl)
    updated_at = Column(String(19), default=_now_kl, onupdate=_now_kl)
    
    __table_args__ = (
        {'sqlite_autoincrement': True},
    )
    
    def _round(self, value):
        return round(value, 2) if value is not None else None
    
    def to_dict(self) -> dict:
        return {
            'transaction_id': self.transaction_id,
            'merchant': self.merchant,
            'pg_account_label': self.pg_account_label,
            'transaction_date': self.transaction_date,
            'channel': self.channel,
            'kira_amount': self._round(self.kira_amount),
            'pg_amount': self._round(self.pg_amount),
            'mdr': self._round(self.mdr),
            'kira_settlement_amount': self._round(self.kira_settlement_amount),
            'volume': self.volume,
            'settlement_date': self.settlement_date,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }


class DepositFee(Base):
    __tablename__ = 'deposit_fees'
    
    deposit_fee_id = Column(Integer, primary_key=True, autoincrement=True)
    merchant = Column(String(100), nullable=False, index=True)
    transaction_date = Column(String(10), nullable=False, index=True)
    channel = Column(String(20), nullable=False)
    fee_type = Column(String(20))
    fee_rate = Column(Float)
    remarks = Column(Text)
    updated_at = Column(String(19), default=_now_kl, onupdate=_now_kl)
    
    __table_args__ = (
        {'sqlite_autoincrement': True},
    )
    
    def _round(self, value):
        return round(value, 2) if value is not None else None
    
    def calculate_fee(self, amount: float, volume: int) -> float:
        if not self.fee_type or self.fee_rate is None:
            return 0
        
        if self.fee_type == 'percentage':
            return round(amount * (self.fee_rate / 100), 2)
        elif self.fee_type == 'per_volume':
            return round(volume * self.fee_rate, 2)
        elif self.fee_type == 'flat':
            return round(self.fee_rate, 2)
        
        return 0
    
    def to_dict(self) -> dict:
        return {
            'deposit_fee_id': self.deposit_fee_id,
            'merchant': self.merchant,
            'transaction_date': self.transaction_date,
            'channel': self.channel,
            'fee_type': self.fee_type,
            'fee_rate': self._round(self.fee_rate),
            'remarks': self.remarks,
            'updated_at': self.updated_at
        }


class MerchantBalance(Base):
    __tablename__ = 'merchant_balances'

    merchant_balance_id = Column(Integer, primary_key=True, autoincrement=True)
    merchant = Column(String(100), nullable=False, index=True)
    transaction_date = Column(String(10), nullable=False, index=True)
    
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

    def _round(self, value):
        return round(value, 2) if value is not None else None

    def to_dict(self) -> dict:
        return {
            'merchant_balance_id': self.merchant_balance_id,
            'merchant': self.merchant,
            'transaction_date': self.transaction_date,
            'settlement_fund': self._round(self.settlement_fund),
            'settlement_charges': self._round(self.settlement_charges),
            'withdrawal_amount': self._round(self.withdrawal_amount),
            'withdrawal_charges': self._round(self.withdrawal_charges),
            'topup_payout_pool': self._round(self.topup_payout_pool),
            'payout_pool_balance': self._round(self.payout_pool_balance),
            'available_balance': self._round(self.available_balance),
            'total_balance': self._round(self.total_balance),
            'remarks': self.remarks,
            'updated_at': self.updated_at
        }


class AgentBalance(Base):
    __tablename__ = 'agent_balances'
    
    agent_balance_id = Column(Integer, primary_key=True, autoincrement=True)
    merchant = Column(String(100), nullable=False, index=True)
    transaction_date = Column(String(10), nullable=False, index=True)
    
    commission_rate_fpx = Column(Float)
    commission_rate_ewallet = Column(Float)
    withdrawal_amount = Column(Float)
    balance = Column(Float)
    
    updated_at = Column(String(30), default=_now_kl, onupdate=_now_kl)
    
    __table_args__ = (
        {'sqlite_autoincrement': True},
    )

    def _round(self, value):
        return round(value, 2) if value is not None else None

    def to_dict(self) -> dict:
        return {
            'agent_balance_id': self.agent_balance_id,
            'merchant': self.merchant,
            'transaction_date': self.transaction_date,
            'commission_rate_fpx': self._round(self.commission_rate_fpx),
            'commission_rate_ewallet': self._round(self.commission_rate_ewallet),
            'withdrawal_amount': self._round(self.withdrawal_amount),
            'balance': self._round(self.balance),
            'updated_at': self.updated_at
        }
