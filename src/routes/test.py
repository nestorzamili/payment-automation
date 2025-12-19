import asyncio

from flask import Blueprint, request

from src.core import BrowserManager, load_accounts
from src.core.database import get_session
from src.core.loader import PROJECT_ROOT
from src.core.logger import get_logger
from src.core.models import Job
from src.utils import jsend_success, jsend_fail, job_manager

bp = Blueprint('test', __name__)
logger = get_logger(__name__)


def run_test_job(account: dict, from_date: str, to_date: str):
    async def run_download():
        async with BrowserManager() as browser_manager:
            from src.scrapers import get_scraper_class
            scraper_class = get_scraper_class(account['platform'])
            scraper = scraper_class(account)
            downloaded_files = await scraper.download_data(browser_manager, from_date, to_date)
            return [str(f.relative_to(PROJECT_ROOT)) for f in downloaded_files]

    files = asyncio.run(run_download())
    return {
        'label': account['label'],
        'platform': account['platform'],
        'from_date': from_date,
        'to_date': to_date,
        'files': files,
        'file_count': len(files)
    }


@bp.route('/test/<label>', methods=['POST'])
def test_account(label: str):
    accounts = load_accounts()
    account = next((a for a in accounts if a['label'] == label), None)
    
    if not account:
        return jsend_fail(f'Account not found: {label}', 404)
    
    from_date = request.args.get('from_date')
    to_date = request.args.get('to_date')
    
    if not from_date or not to_date:
        return jsend_fail('from_date and to_date query params are required', 400)
    
    session = get_session()
    try:
        covered_job = session.query(Job).filter(
            Job.account_label == label,
            Job.status == 'completed',
            Job.from_date <= from_date,
            Job.to_date >= to_date
        ).first()
        if covered_job:
            logger.info(f"Range {from_date} to {to_date} already covered by job {covered_job.job_id}")
            return jsend_success({
                'label': label,
                'message': f'Range already covered by job {covered_job.job_id}',
                'covered_by': covered_job.to_dict()
            })
    finally:
        session.close()
    
    job_type = f"test:{label}"
    existing = job_manager.get_running_job_by_type(job_type)
    if existing:
        return jsend_fail(f"Test job already running for {label} (job: {existing['job_id']})", 409)
    
    job_id = job_manager.create_job(
        job_type=job_type,
        account_label=label,
        from_date=from_date,
        to_date=to_date
    )
    job_manager.run_in_background(job_id, run_test_job, account, from_date, to_date)
    logger.info(f"Test job queued: {job_id} ({label})")
    return jsend_success({'job_id': job_id, 'label': label, 'message': 'Test job queued'}, 202)
