import json
from pathlib import Path
from typing import Dict, Any, List

PROJECT_ROOT = Path(__file__).parent.parent.parent


def load_settings() -> Dict[str, Any]:
    settings_path = PROJECT_ROOT / 'config' / 'settings.json'
    
    if not settings_path.exists():
        raise FileNotFoundError(f"Settings file not found: {settings_path}")
    
    with open(settings_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def load_accounts() -> List[Dict[str, Any]]:
    accounts_path = PROJECT_ROOT / 'config' / 'accounts.json'
    
    if not accounts_path.exists():
        raise FileNotFoundError(f"Accounts file not found: {accounts_path}")
    
    with open(accounts_path, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    return data['accounts']


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
    return PROJECT_ROOT / 'sessions' / f"{label}.json"


def get_download_path(label: str) -> Path:
    settings = load_settings()
    base_path = PROJECT_ROOT / settings['download']['base_path']
    return base_path / label
