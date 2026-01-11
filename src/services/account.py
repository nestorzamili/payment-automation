from typing import Dict, List, Optional, Any

from src.core.database import get_session
from src.core.models import Account
from src.core.logger import get_logger

logger = get_logger(__name__)


def get_all_accounts() -> List[Account]:
    session = get_session()
    try:
        return session.query(Account).order_by(Account.account_id).all()
    finally:
        session.close()


def get_active_accounts() -> List[Account]:
    session = get_session()
    try:
        return session.query(Account).filter(Account.is_active == 1).order_by(Account.account_id).all()
    finally:
        session.close()


def get_account_by_id(account_id: int) -> Optional[Account]:
    session = get_session()
    try:
        return session.query(Account).filter(Account.account_id == account_id).first()
    finally:
        session.close()


def get_account_by_label(label: str) -> Optional[Account]:
    session = get_session()
    try:
        return session.query(Account).filter(Account.label == label).first()
    finally:
        session.close()


def get_accounts_by_platform(platform: str) -> List[Account]:
    session = get_session()
    try:
        return session.query(Account).filter(
            Account.platform == platform,
            Account.is_active == 1
        ).order_by(Account.account_id).all()
    finally:
        session.close()


PLATFORM_BASE_URLS = {
    'kira': 'https://bo.kira.asia/backoffice',
    'axai': 'https://user.ragnaroksys.com',
    'm1': 'https://m1pay.com.my/merchant',
    'fiuu': 'https://api.fiuu.com'
}


def create_account(data: Dict[str, Any]) -> Account:
    session = get_session()
    try:
        existing = session.query(Account).filter(Account.label == data['label']).first()
        if existing:
            raise ValueError(f"Account dengan label '{data['label']}' sudah ada. Gunakan label yang berbeda.")
        
        platform = data['platform']
        base_url = data.get('base_url') or PLATFORM_BASE_URLS.get(platform)
        
        account = Account(
            label=data['label'],
            platform=platform,
            base_url=base_url,
            need_captcha=1 if data.get('need_captcha') else 0,
            is_active=1 if data.get('is_active', True) else 0,
            cred_username=data.get('cred_username'),
            cred_password=data.get('cred_password')
        )
        session.add(account)
        session.commit()
        session.refresh(account)
        logger.info(f"Created account: {account.label}")
        return account
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to create account: {e}")
        raise
    finally:
        session.close()


def update_account(account_id: int, data: Dict[str, Any]) -> Optional[Account]:
    session = get_session()
    try:
        account = session.query(Account).filter(Account.account_id == account_id).first()
        if not account:
            return None
        
        if 'label' in data:
            account.label = data['label']
        if 'platform' in data:
            account.platform = data['platform']
        if 'base_url' in data:
            account.base_url = data['base_url']
        if 'need_captcha' in data:
            account.need_captcha = 1 if data['need_captcha'] else 0
        if 'is_active' in data:
            account.is_active = 1 if data['is_active'] else 0
        if 'cred_username' in data:
            account.cred_username = data['cred_username']
        if 'cred_password' in data:
            account.cred_password = data['cred_password']
        
        session.commit()
        session.refresh(account)
        logger.info(f"Updated account: {account.label}")
        return account
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to update account: {e}")
        raise
    finally:
        session.close()


def delete_account(account_id: int) -> bool:
    session = get_session()
    try:
        account = session.query(Account).filter(Account.account_id == account_id).first()
        if not account:
            return False
        
        session.delete(account)
        session.commit()
        logger.info(f"Deleted account: {account.label}")
        return True
    except Exception as e:
        session.rollback()
        logger.error(f"Failed to delete account: {e}")
        raise
    finally:
        session.close()
