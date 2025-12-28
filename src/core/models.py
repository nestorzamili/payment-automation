from datetime import datetime
from zoneinfo import ZoneInfo

from sqlalchemy import Column, String, Float, Text, Integer, Index

from src.core.database import Base

KL_TZ = ZoneInfo('Asia/Kuala_Lumpur')

def _now_kl():
    return datetime.now(KL_TZ).strftime('%Y-%m-%d %H:%M:%S')


class Job(Base):
    __tablename__ = 'jobs'

    job_id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String(36), index=True)
    job_type = Column(String(50), nullable=False)
    platform = Column(String(20), nullable=False)
    account_label = Column(String(100), nullable=False)
    from_date = Column(String(10))
    to_date = Column(String(10))
    status = Column(String(20), nullable=False, default='pending')
    filename = Column(String(255))
    transactions_count = Column(Integer, default=0)
    desc = Column(Text)
    created_at = Column(String(30), nullable=False, default=_now_kl)
    updated_at = Column(String(30), nullable=False, default=_now_kl, onupdate=_now_kl)

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
    transaction_date = Column(String(19), nullable=False)
    amount = Column(Float, nullable=False)
    payment_method = Column(String(20), nullable=False)
    mdr = Column(Float)
    settlement_amount = Column(Float)
    merchant = Column(String(100), index=True)
    created_at = Column(String(19), default=_now_kl)

    __table_args__ = (
        Index('ix_kira_merchant_date', 'merchant', 'transaction_date'),
    )

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
    transaction_date = Column(String(19), nullable=False)
    amount = Column(Float, nullable=False)
    platform = Column(String(20), nullable=False)
    channel = Column(String(50), nullable=False)
    account_label = Column(String(50), nullable=False)
    created_at = Column(String(19), default=_now_kl)

    __table_args__ = (
        Index('ix_pg_account_date', 'account_label', 'transaction_date'),
        Index('ix_pg_account_date_channel', 'account_label', 'transaction_date', 'channel'),
    )

    def to_dict(self) -> dict:
        return {
            'transaction_id': self.transaction_id,
            'transaction_date': self.transaction_date,
            'amount': self.amount,
            'platform': self.platform,
            'channel': self.channel,
            'account_label': self.account_label
        }


class KiraPGFee(Base):
    __tablename__ = 'kira_pg_fees'

    id = Column(Integer, primary_key=True, autoincrement=True)
    pg_account_label = Column(String(100), nullable=False)
    transaction_date = Column(String(10), nullable=False)
    channel = Column(String(20), nullable=False)
    fee_rate = Column(Float)
    remarks = Column(Text)
    updated_at = Column(String(19), default=_now_kl, onupdate=_now_kl)

    __table_args__ = (
        Index('ix_kira_pg_fee_lookup', 'pg_account_label', 'transaction_date', 'channel', unique=True),
    )

    def to_dict(self) -> dict:
        return {
            'id': self.id,
            'pg_account_label': self.pg_account_label,
            'transaction_date': self.transaction_date,
            'channel': self.channel,
            'fee_rate': round(self.fee_rate, 2) if self.fee_rate else None,
            'remarks': self.remarks,
            'updated_at': self.updated_at
        }


class DepositLedger(Base):
    __tablename__ = 'deposit_ledger'

    id = Column(Integer, primary_key=True, autoincrement=True)
    merchant = Column(String(100), nullable=False)
    transaction_date = Column(String(10), nullable=False)
    channel = Column(String(20), nullable=False)
    fee_type = Column(String(20))
    fee_rate = Column(Float)
    remarks = Column(Text)
    updated_at = Column(String(19), default=_now_kl, onupdate=_now_kl)

    __table_args__ = (
        Index('ix_deposit_ledger_lookup', 'merchant', 'transaction_date', 'channel', unique=True),
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
            'id': self.id,
            'merchant': self.merchant,
            'transaction_date': self.transaction_date,
            'channel': self.channel,
            'fee_type': self.fee_type,
            'fee_rate': self._round(self.fee_rate),
            'remarks': self.remarks,
            'updated_at': self.updated_at
        }


class MerchantLedger(Base):
    __tablename__ = 'merchant_ledger'

    id = Column(Integer, primary_key=True, autoincrement=True)
    merchant = Column(String(100), nullable=False)
    transaction_date = Column(String(10), nullable=False)
    
    settlement_fund = Column(Float)
    settlement_charges = Column(Float)
    withdrawal_amount = Column(Float)
    withdrawal_rate = Column(Float)
    withdrawal_charges = Column(Float)
    topup_payout_pool = Column(Float)
    remarks = Column(Text)
    
    available_fpx = Column(Float)
    available_ewallet = Column(Float)
    available_total = Column(Float)
    
    payout_pool_balance = Column(Float)
    available_balance = Column(Float)
    total_balance = Column(Float)
    
    updated_at = Column(String(19), default=_now_kl, onupdate=_now_kl)

    __table_args__ = (
        Index('ix_merchant_ledger_lookup', 'merchant', 'transaction_date', unique=True),
    )

    def _round(self, value):
        return round(value, 2) if value is not None else None

    def to_dict(self) -> dict:
        return {
            'id': self.id,
            'merchant': self.merchant,
            'transaction_date': self.transaction_date,
            'settlement_fund': self._round(self.settlement_fund),
            'settlement_charges': self._round(self.settlement_charges),
            'withdrawal_amount': self._round(self.withdrawal_amount),
            'withdrawal_rate': self._round(self.withdrawal_rate),
            'withdrawal_charges': self._round(self.withdrawal_charges),
            'topup_payout_pool': self._round(self.topup_payout_pool),
            'payout_pool_balance': self._round(self.payout_pool_balance),
            'available_balance': self._round(self.available_balance),
            'total_balance': self._round(self.total_balance),
            'remarks': self.remarks,
            'updated_at': self.updated_at
        }


class AgentLedger(Base):
    __tablename__ = 'agent_ledger'

    id = Column(Integer, primary_key=True, autoincrement=True)
    merchant = Column(String(100), nullable=False)
    transaction_date = Column(String(10), nullable=False)
    
    commission_rate_fpx = Column(Float)
    commission_rate_ewallet = Column(Float)
    withdrawal_amount = Column(Float)
    
    available_fpx = Column(Float)
    available_ewallet = Column(Float)
    available_total = Column(Float)
    
    balance = Column(Float)
    
    updated_at = Column(String(30), default=_now_kl, onupdate=_now_kl)

    __table_args__ = (
        Index('ix_agent_ledger_lookup', 'merchant', 'transaction_date', unique=True),
    )

    def _round(self, value):
        return round(value, 2) if value is not None else None

    def to_dict(self) -> dict:
        return {
            'id': self.id,
            'merchant': self.merchant,
            'transaction_date': self.transaction_date,
            'commission_rate_fpx': self._round(self.commission_rate_fpx),
            'commission_rate_ewallet': self._round(self.commission_rate_ewallet),
            'withdrawal_amount': self._round(self.withdrawal_amount),
            'balance': self._round(self.balance),
            'updated_at': self.updated_at
        }
