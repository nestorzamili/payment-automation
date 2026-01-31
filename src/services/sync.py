from __future__ import annotations

import threading
import time
import uuid
from typing import List, Tuple, Optional

from src.core import load_accounts
from src.core.jobs import job_manager
from src.core.logger import get_logger
from src.scrapers import BrowserManager, get_scraper_class
from src.services.parser import run_parse_job
from src.services.job_sheet import JobSheetService
from src.utils.date_range import DateRangeService

logger = get_logger(__name__)

def _get_date_service():
    return DateRangeService()

_sync_running = False
_current_run_id: Optional[str] = None


def is_sync_running() -> bool:
    return _sync_running


def get_current_run_id() -> Optional[str]:
    return _current_run_id


def start_full_sync() -> dict:
    global _sync_running, _current_run_id
    
    if _sync_running:
        return {
            'status': 'already_running',
            'run_id': _current_run_id,
            'message': 'Sync already in progress'
        }
    
    run_id = str(uuid.uuid4())
    _current_run_id = run_id
    _sync_running = True
    
    thread = threading.Thread(target=_run_full_sync, args=(run_id,), daemon=True)
    thread.start()
    
    logger.info(f"Full sync started: {run_id}")
    
    return {
        'status': 'started',
        'run_id': run_id,
        'message': 'Sync started'
    }


def start_platform_sync(platform: str) -> dict:
    global _sync_running, _current_run_id
    
    if _sync_running:
        return {
            'status': 'already_running',
            'run_id': _current_run_id,
            'message': 'Sync already in progress'
        }
    
    run_id = str(uuid.uuid4())
    _current_run_id = run_id
    _sync_running = True
    
    thread = threading.Thread(target=_run_platform_sync, args=(run_id, platform), daemon=True)
    thread.start()
    
    logger.info(f"Platform sync started: {platform} ({run_id})")
    
    return {
        'status': 'started',
        'run_id': run_id,
        'platform': platform,
        'message': f'{platform} download started'
    }


def start_parse_only() -> dict:
    global _sync_running, _current_run_id
    
    if _sync_running:
        return {
            'status': 'already_running',
            'run_id': _current_run_id,
            'message': 'Sync already in progress'
        }
    
    run_id = str(uuid.uuid4())
    _current_run_id = run_id
    _sync_running = True
    
    thread = threading.Thread(target=_run_parse_only, args=(run_id,), daemon=True)
    thread.start()
    
    logger.info(f"Parse only started: {run_id}")
    
    return {
        'status': 'started',
        'run_id': run_id,
        'message': 'Parse started'
    }


def _run_full_sync(run_id: str):
    global _sync_running, _current_run_id
    
    try:
        JobSheetService.clear_sheet()
        accounts = load_accounts()
        platform_ranges = _get_date_service().get_platform_ranges()
        all_jobs = []
        
        for platform in ['kira', 'axai', 'm1', 'fiuu']:
            platform_accounts = [a for a in accounts if a['platform'] == platform]
            date_range = platform_ranges.get(platform)
            
            if date_range and platform_accounts:
                from_date, to_date = date_range
                jobs = _create_download_jobs(run_id, platform_accounts, from_date, to_date, platform)
                all_jobs.extend(jobs)
        
        if all_jobs:
            _run_download_jobs(all_jobs)

        run_parse_job(run_id)
        logger.info(f"Full sync completed: {run_id}")
        
    except Exception as e:
        logger.error(f"Full sync failed: {e}")
    finally:
        _sync_running = False
        _current_run_id = None


def _run_platform_sync(run_id: str, platform: str):
    global _sync_running, _current_run_id
    
    try:
        JobSheetService.clear_sheet()
        accounts = load_accounts()
        target_accounts = [a for a in accounts if a['platform'] == platform]
        
        if not target_accounts:
            logger.warning(f"No accounts found for platform: {platform}")
            return
        
        platform_ranges = _get_date_service().get_platform_ranges()
        date_range = platform_ranges.get(platform)
        
        if not date_range:
            logger.info(f"No date range for {platform}, already up to date")
            return
        
        from_date, to_date = date_range
        jobs = _create_download_jobs(run_id, target_accounts, from_date, to_date, platform)
        if jobs:
            _run_download_jobs(jobs)
        
        logger.info(f"Platform sync completed: {platform} ({run_id})")
        
    except Exception as e:
        logger.error(f"Platform sync failed: {e}")
    finally:
        _sync_running = False
        _current_run_id = None


def _run_parse_only(run_id: str):
    global _sync_running, _current_run_id
    
    try:
        run_parse_job(run_id)
        logger.info(f"Parse only completed: {run_id}")
        
    except Exception as e:
        logger.error(f"Parse only failed: {e}")
    finally:
        _sync_running = False
        _current_run_id = None


def _is_account_completed(account_label: str, from_date: str, to_date: str) -> bool:
    from src.core.database import get_session
    from src.core.models import Job
    
    session = get_session()
    try:
        job = session.query(Job).filter(
            Job.job_type == 'download',
            Job.account_label == account_label,
            Job.from_date == from_date,
            Job.to_date == to_date,
            Job.status == 'completed'
        ).first()
        return job is not None
    finally:
        session.close()


def _create_download_jobs(run_id: str, accounts: list, from_date: str, to_date: str, platform_group: str) -> List[Tuple[int, dict, str, str]]:
    jobs = []
    for account in accounts:
        if _is_account_completed(account['label'], from_date, to_date):
            logger.info(f"Skipping {account['label']}: already completed for {from_date} - {to_date}")
            continue
        
        source_type = 'api' if account['platform'] == 'fiuu' else 'browser'
        job_id = job_manager.create_job(
            job_type='download',
            run_id=run_id,
            platform=platform_group,
            account_label=account['label'],
            source_type=source_type,
            from_date=from_date,
            to_date=to_date
        )
        _append_job_to_sheet(job_id)
        jobs.append((job_id, account, from_date, to_date))
    return jobs


def _run_download_jobs(jobs: List[Tuple[int, dict, str, str]]):
    import asyncio

    async def run():
        api_jobs = [job for job in jobs if job[1]['platform'] == 'fiuu']
        browser_jobs = [job for job in jobs if job[1]['platform'] != 'fiuu']

        for job_id, account, from_date, to_date in api_jobs:
            job_manager.update_job(job_id, 'running')
            _update_job_sheet(job_id)
            try:
                from src.services.fiuu import FiuuAPIClient
                client = FiuuAPIClient(account)
                fetched, stored = client.fetch_and_store(from_date, to_date)
                job_manager.update_job(job_id, 'completed', fetched_count=fetched, stored_count=stored)
            except Exception as e:
                from src.core.logger import clean_error_msg
                error_msg = clean_error_msg(e)
                logger.error(f"Download failed: {account['label']} - {error_msg}")
                job_manager.update_job(job_id, 'failed', error_message=error_msg)
            _update_job_sheet(job_id)

        if browser_jobs:
            for job_id, account, from_date, to_date in browser_jobs:
                job_manager.update_job(job_id, 'running')
                _update_job_sheet(job_id)
                try:
                    async with BrowserManager() as browser_manager:
                        scraper_class = get_scraper_class(account['platform'])
                        scraper = scraper_class(account)
                        files = await scraper.download_data(browser_manager, from_date, to_date, job_id)
                        job_manager.update_job(job_id, 'completed', fetched_count=len(files), stored_count=len(files))
                except Exception as e:
                    from src.core.logger import clean_error_msg
                    error_msg = clean_error_msg(e)
                    logger.error(f"Download failed: {account['label']} - {error_msg}")
                    job_manager.update_job(job_id, 'failed', error_message=error_msg)
                _update_job_sheet(job_id)

    asyncio.run(run())


def _update_job_sheet(job_id: int):
    job = job_manager.get_job(job_id)
    if job:
        JobSheetService.update_job_by_id(job)


def _append_job_to_sheet(job_id: int):
    job = job_manager.get_job(job_id)
    if job:
        JobSheetService.append_job(job)
