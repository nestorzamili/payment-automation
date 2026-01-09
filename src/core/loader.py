import json
from functools import lru_cache
from pathlib import Path
from typing import Dict, Any, List
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).parent.parent.parent


@lru_cache(maxsize=1)
def get_timezone() -> ZoneInfo:
    """Get timezone from settings. Cached for performance."""
    settings = load_settings()
    return ZoneInfo(settings['timezone'])


def load_settings() -> Dict[str, Any]:
    settings_path = PROJECT_ROOT / 'config' / 'settings.json'
    
    if not settings_path.exists():
        raise FileNotFoundError(f"Settings file not found: {settings_path}")
    
    with open(settings_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def load_accounts() -> List[Dict[str, Any]]:
    from src.services.account import get_active_accounts
    
    accounts = get_active_accounts()
    result = []
    
    for acc in accounts:
        credentials = {}
        if acc.platform in ['kira', 'm1']:
            credentials = {'username': acc.cred_username, 'password': acc.cred_password}
        elif acc.platform == 'axai':
            credentials = {'email': acc.cred_username, 'password': acc.cred_password}
        elif acc.platform == 'fiuu':
            credentials = {'merchant_id': acc.cred_username, 'private_key': acc.cred_password}
        
        result.append({
            'label': acc.label,
            'platform': acc.platform,
            'credentials': credentials,
            'base_url': acc.base_url,
            'need_captcha': acc.need_captcha == 1
        })
    
    return result


def get_service_account_path() -> Path:
    settings = load_settings()
    sa_file = settings['google_sheets']['service_account_file']
    sa_path = PROJECT_ROOT / sa_file
    
    if not sa_path.exists():
        raise FileNotFoundError(
            f"Service account file not found: {sa_path}\n"
            f"Please create it from service-account.json.example"
        )
    
    return sa_path


def get_spreadsheet_id() -> str:
    settings = load_settings()
    return settings['google_sheets']['spreadsheet_id']


def get_session_path(label: str) -> Path:
    settings = load_settings()
    return PROJECT_ROOT / settings['sessions']['path'] / f"{label}.json"


def get_download_path(label: str) -> Path:
    settings = load_settings()
    base_path = PROJECT_ROOT / settings['download']['base_path']
    return base_path / label
