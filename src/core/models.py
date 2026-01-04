from datetime import datetime

from sqlalchemy import Column, String, Float, Text, Integer, Index

from src.core.database import Base
from src.core.loader import get_timezone

def _now_kl():
    return datetime.now(get_timezone()).strftime('%Y-%m-%d %H:%M:%S')


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


class KiraPG(Base):
    __tablename__ = 'kira_pg'

    id = Column(Integer, primary_key=True, autoincrement=True)
    pg_account_label = Column(String(100), nullable=False)
    transaction_date = Column(String(10), nullable=False)
    channel = Column(String(20), nullable=False)
    
    kira_amount = Column(Float)
    mdr = Column(Float)
    kira_settlement_amount = Column(Float)
    
    pg_amount = Column(Float)
    volume = Column(Integer)
    
    settlement_rule = Column(String(10))
    settlement_date = Column(String(10))
    
    fee_type = Column(String(20))
    fee_rate = Column(Float)
    fees = Column(Float)
    settlement_amount = Column(Float)
    
    daily_variance = Column(Float)
    cumulative_variance = Column(Float)
    
    remarks = Column(Text)
    updated_at = Column(String(19), default=_now_kl, onupdate=_now_kl)

    __table_args__ = (
        Index('ix_kira_pg_lookup', 'pg_account_label', 'transaction_date', 'channel', unique=True),
    )

    def _r(self, value):
        return round(value, 2) if value is not None else None

    def to_dict(self) -> dict:
        return {
            'id': self.id,
            'pg_account_label': self.pg_account_label,
            'transaction_date': self.transaction_date,
            'channel': self.channel,
            'kira_amount': self._r(self.kira_amount),
            'mdr': self._r(self.mdr),
            'kira_settlement_amount': self._r(self.kira_settlement_amount),
            'pg_amount': self._r(self.pg_amount),
            'volume': self.volume,
            'settlement_rule': self.settlement_rule,
            'settlement_date': self.settlement_date,
            'fee_type': self.fee_type,
            'fee_rate': self._r(self.fee_rate),
            'fees': self._r(self.fees),
            'settlement_amount': self._r(self.settlement_amount),
            'daily_variance': self._r(self.daily_variance),
            'cumulative_variance': self._r(self.cumulative_variance),
            'remarks': self.remarks,
            'updated_at': self.updated_at
        }


class Deposit(Base):
    __tablename__ = 'deposit'

    id = Column(Integer, primary_key=True, autoincrement=True)
    merchant = Column(String(100), nullable=False)
    transaction_date = Column(String(10), nullable=False)
    
    fpx_amount = Column(Float)
    fpx_volume = Column(Integer)
    fpx_fee_type = Column(String(20))
    fpx_fee_rate = Column(Float)
    fpx_fee_amount = Column(Float)
    fpx_gross = Column(Float)
    fpx_settlement_date = Column(String(10))
    
    ewallet_amount = Column(Float)
    ewallet_volume = Column(Integer)
    ewallet_fee_type = Column(String(20))
    ewallet_fee_rate = Column(Float)
    ewallet_fee_amount = Column(Float)
    ewallet_gross = Column(Float)
    ewallet_settlement_date = Column(String(10))
    
    total_amount = Column(Float)
    total_fees = Column(Float)
    available_fpx = Column(Float)
    available_ewallet = Column(Float)
    available_total = Column(Float)
    
    remarks = Column(Text)
    updated_at = Column(String(19), default=_now_kl, onupdate=_now_kl)

    __table_args__ = (
        Index('ix_deposit_lookup', 'merchant', 'transaction_date', unique=True),
    )

    def _r(self, value):
        return round(value, 2) if value is not None else None

    def calculate_fee(self, channel: str, amount: float, volume: int) -> float:
        if channel == 'FPX':
            fee_type = self.fpx_fee_type
            fee_rate = self.fpx_fee_rate
        else:
            fee_type = self.ewallet_fee_type
            fee_rate = self.ewallet_fee_rate
        
        if not fee_type or fee_rate is None:
            return 0
        
        if fee_type == 'percentage':
            return round(amount * (fee_rate / 100), 2)
        elif fee_type == 'per_volume':
            return round(volume * fee_rate, 2)
        elif fee_type == 'flat':
            return round(fee_rate, 2)
        
        return 0

    def to_dict(self) -> dict:
        return {
            'id': self.id,
            'merchant': self.merchant,
            'transaction_date': self.transaction_date,
            'fpx_amount': self._r(self.fpx_amount),
            'fpx_volume': self.fpx_volume,
            'fpx_fee_type': self.fpx_fee_type,
            'fpx_fee_rate': self._r(self.fpx_fee_rate),
            'fpx_fee_amount': self._r(self.fpx_fee_amount),
            'fpx_gross': self._r(self.fpx_gross),
            'fpx_settlement_date': self.fpx_settlement_date,
            'ewallet_amount': self._r(self.ewallet_amount),
            'ewallet_volume': self.ewallet_volume,
            'ewallet_fee_type': self.ewallet_fee_type,
            'ewallet_fee_rate': self._r(self.ewallet_fee_rate),
            'ewallet_fee_amount': self._r(self.ewallet_fee_amount),
            'ewallet_gross': self._r(self.ewallet_gross),
            'ewallet_settlement_date': self.ewallet_settlement_date,
            'total_amount': self._r(self.total_amount),
            'total_fees': self._r(self.total_fees),
            'available_fpx': self._r(self.available_fpx),
            'available_ewallet': self._r(self.available_ewallet),
            'available_total': self._r(self.available_total),
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
    
    available_fpx = Column(Float)
    available_ewallet = Column(Float)
    available_total = Column(Float)
    
    volume = Column(Float)
    commission_rate = Column(Float)
    commission_amount = Column(Float)
    
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
            'volume': self._round(self.volume),
            'commission_rate': self._round(self.commission_rate),
            'commission_amount': self._round(self.commission_amount),
            'balance': self._round(self.balance),
            'updated_at': self.updated_at
        }


class Parameter(Base):
    __tablename__ = 'parameter'

    id = Column(Integer, primary_key=True, autoincrement=True)
    type = Column(String(50), nullable=False)
    key = Column(String(100), nullable=False)
    value = Column(String(255))
    description = Column(Text)
    updated_at = Column(String(19), default=_now_kl, onupdate=_now_kl)

    __table_args__ = (
        Index('ix_parameter_lookup', 'type', 'key', unique=True),
    )

    def to_dict(self) -> dict:
        return {
            'id': self.id,
            'type': self.type,
            'key': self.key,
            'value': self.value,
            'description': self.description,
            'updated_at': self.updated_at
        }
