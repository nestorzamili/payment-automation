from __future__ import annotations

from flask import Blueprint

from src.core import load_accounts
from src.core.logger import get_logger
from src.services.download import (
    get_date_range,
    start_platform_download,
    start_account_download,
    check_running_download
)
from src.utils import jsend_success, jsend_fail

bp = Blueprint('download', __name__)
logger = get_logger(__name__)


@bp.route('/download/<platform>', methods=['POST'])
def download_platform(platform: str):
    all_accounts = load_accounts()
    
    if platform == 'kira':
        accounts = [a for a in all_accounts if a['platform'] == 'kira']
    elif platform in ('m1', 'axai', 'fiuu'):
        accounts = [a for a in all_accounts if a['platform'] == platform]
    elif platform == 'pg':
        accounts = [a for a in all_accounts if a['platform'] in ('m1', 'axai')]
    else:
        return jsend_fail(f'Unknown platform: {platform}', 400)
    
    if not accounts:
        return jsend_fail(f'No accounts found for platform: {platform}', 404)
    
    date_range = get_date_range(platform)
    if date_range is None:
        return jsend_success({
            'platform': platform,
            'status': 'skipped',
            'message': f'{platform}: Already up to date, no download needed'
        }, 200)
    
    from_date, to_date = date_range
    
    existing = check_running_download()
    if existing:
        return jsend_fail(f"Download job already running for {platform} (job: {existing['job_id']})", 409)
    
    result = start_platform_download(platform, accounts, from_date, to_date)
    result['message'] = 'Download jobs queued'
    
    return jsend_success(result, 202)


@bp.route('/download/<platform>/<label>', methods=['POST'])
def download_account(platform: str, label: str):
    all_accounts = load_accounts()
    account = next((a for a in all_accounts if a['label'] == label), None)
    
    if not account:
        return jsend_fail(f'Account not found: {label}', 404)
    
    if account['platform'] != platform:
        return jsend_fail(f'Account {label} is not a {platform} account', 400)
    
    date_range = get_date_range(platform)
    if date_range is None:
        return jsend_success({
            'label': label,
            'platform': platform,
            'status': 'skipped',
            'message': f'{label}: Already up to date, no download needed'
        }, 200)
    
    from_date, to_date = date_range
    
    existing = check_running_download()
    if existing:
        return jsend_fail(f"Download job already running for {label} (job: {existing['job_id']})", 409)
    
    result = start_account_download(account, from_date, to_date)
    result['message'] = 'Download job queued'
    
    return jsend_success(result, 202)
