import asyncio

from flask import Blueprint

from src.core import load_accounts
from src.core.loader import PROJECT_ROOT
from src.core.logger import get_logger
from src.core.jobs import job_manager
from src.scrapers import BrowserManager
from src.scrapers.date_range import DateRangeService
from src.utils import jsend_success, jsend_fail

bp = Blueprint('download', __name__)
logger = get_logger(__name__)
date_service = DateRangeService()


def get_date_range(platform: str) -> tuple[str, str]:
    return date_service.get_date_range(platform)


def run_download_job(accounts: list, from_date: str, to_date: str):
    async def run():
        results = []
        async with BrowserManager() as browser_manager:
            from src.scrapers import get_scraper_class
            for account in accounts:
                try:
                    scraper_class = get_scraper_class(account['platform'])
                    scraper = scraper_class(account)
                    files = await scraper.download_data(browser_manager, from_date, to_date)
                    results.append({
                        'label': account['label'],
                        'status': 'success',
                        'files': [str(f.relative_to(PROJECT_ROOT)) for f in files],
                        'file_count': len(files)
                    })
                except Exception as e:
                    results.append({
                        'label': account['label'],
                        'status': 'error',
                        'error': str(e)
                    })
        return results
    
    results = asyncio.run(run())
    
    # Collect all files from successful downloads for Job record
    all_files = []
    for r in results:
        if r['status'] == 'success':
            all_files.extend(r.get('files', []))
    
    return {
        'from_date': from_date,
        'to_date': to_date,
        'accounts': results,
        'files': all_files,  # For JobManager
        'successful': len([r for r in results if r['status'] == 'success']),
        'failed': len([r for r in results if r['status'] == 'error'])
    }


@bp.route('/download/<platform>', methods=['POST'])
def download_platform(platform: str):
    all_accounts = load_accounts()
    
    if platform == 'kira':
        accounts = [a for a in all_accounts if a['platform'] == 'kira']
    elif platform in ('m1', 'axai', 'fiuu'):
        accounts = [a for a in all_accounts if a['platform'] == platform]
    elif platform == 'pg':
        accounts = [a for a in all_accounts if a['platform'] in ('m1', 'axai', 'fiuu')]
    else:
        return jsend_fail(f'Unknown platform: {platform}', 400)
    
    if not accounts:
        return jsend_fail(f'No accounts found for platform: {platform}', 404)
    
    from_date, to_date = get_date_range(platform)
    
    job_type = "download"
    account_labels = ','.join([a['label'] for a in accounts])
    existing = job_manager.get_running_job_by_type(job_type)
    if existing:
        return jsend_fail(f"Download job already running for {platform} (job: {existing['job_id']})", 409)
    
    job_id = job_manager.create_job(job_type=job_type, platform=platform, account_label=account_labels, from_date=from_date, to_date=to_date)
    job_manager.run_in_background(job_id, run_download_job, accounts, from_date, to_date)
    
    logger.info(f"Download job queued: {job_id} ({platform}, {len(accounts)} accounts, {from_date} to {to_date})")
    return jsend_success({
        'job_id': job_id,
        'platform': platform,
        'accounts': [a['label'] for a in accounts],
        'from_date': from_date,
        'to_date': to_date,
        'message': 'Download job queued'
    }, 202)


@bp.route('/download/<platform>/<label>', methods=['POST'])
def download_account(platform: str, label: str):
    all_accounts = load_accounts()
    account = next((a for a in all_accounts if a['label'] == label), None)
    
    if not account:
        return jsend_fail(f'Account not found: {label}', 404)
    
    if account['platform'] != platform:
        return jsend_fail(f'Account {label} is not a {platform} account', 400)
    
    from_date, to_date = get_date_range(platform)
    
    job_type = "download"
    existing = job_manager.get_running_job_by_type(job_type)
    if existing:
        return jsend_fail(f"Download job already running for {label} (job: {existing['job_id']})", 409)
    
    job_id = job_manager.create_job(job_type=job_type, platform=platform, account_label=label, from_date=from_date, to_date=to_date)
    job_manager.run_in_background(job_id, run_download_job, [account], from_date, to_date)
    
    logger.info(f"Download job queued: {job_id} ({label}, {from_date} to {to_date})")
    return jsend_success({
        'job_id': job_id,
        'label': label,
        'platform': platform,
        'from_date': from_date,
        'to_date': to_date,
        'message': 'Download job queued'
    }, 202)
