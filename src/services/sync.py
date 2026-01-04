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
date_service = DateRangeService()

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
        accounts = load_accounts()
        
        kira_accounts = [a for a in accounts if a['platform'] == 'kira']
        pg_accounts = [a for a in accounts if a['platform'] in ('m1', 'axai')]
        fiuu_accounts = [a for a in accounts if a['platform'] == 'fiuu']
        
        all_jobs = []
        
        kira_range = date_service.get_date_range('kira')
        if kira_range and kira_accounts:
            from_date, to_date = kira_range
            jobs = _create_download_jobs(run_id, kira_accounts, from_date, to_date)
            all_jobs.extend(jobs)
        
        pg_range = date_service.get_date_range('pg')
        if pg_range and pg_accounts:
            from_date, to_date = pg_range
            jobs = _create_download_jobs(run_id, pg_accounts, from_date, to_date)
            all_jobs.extend(jobs)
        
        fiuu_range = date_service.get_date_range('fiuu')
        if fiuu_range and fiuu_accounts:
            from_date, to_date = fiuu_range
            jobs = _create_download_jobs(run_id, fiuu_accounts, from_date, to_date)
            all_jobs.extend(jobs)
        
        _update_sheet(run_id)
        
        if all_jobs:
            _run_download_jobs(all_jobs, run_id)
        
        run_parse_job(run_id)
        _update_sheet(run_id)
        logger.info(f"Full sync completed: {run_id}")
        
    except Exception as e:
        logger.error(f"Full sync failed: {e}")
    finally:
        _sync_running = False
        _current_run_id = None


def _run_platform_sync(run_id: str, platform: str):
    global _sync_running, _current_run_id
    
    try:
        accounts = load_accounts()
        
        if platform == 'pg':
            target_accounts = [a for a in accounts if a['platform'] in ('m1', 'axai')]
        elif platform in ('m1', 'axai'):
            target_accounts = [a for a in accounts if a['platform'] == platform]
        else:
            target_accounts = [a for a in accounts if a['platform'] == platform]
        
        if not target_accounts:
            logger.warning(f"No accounts found for platform: {platform}")
            return
        
        date_key = 'pg' if platform in ('m1', 'axai', 'pg') else platform
        date_range = date_service.get_date_range(date_key)
        
        if not date_range:
            logger.info(f"No date range for {platform}, already up to date")
            return
        
        from_date, to_date = date_range
        jobs = _create_download_jobs(run_id, target_accounts, from_date, to_date)
        _update_sheet(run_id)
        
        if jobs:
            _run_download_jobs(jobs, run_id)
        
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
        _update_sheet(run_id)
        logger.info(f"Parse only completed: {run_id}")
        
    except Exception as e:
        logger.error(f"Parse only failed: {e}")
    finally:
        _sync_running = False
        _current_run_id = None


def _create_download_jobs(run_id: str, accounts: list, from_date: str, to_date: str) -> List[Tuple[int, dict, str, str]]:
    jobs = []
    for account in accounts:
        job_id = job_manager.create_job(
            job_type='download',
            run_id=run_id,
            platform=account['platform'],
            account_label=account['label'],
            from_date=from_date,
            to_date=to_date
        )
        jobs.append((job_id, account, from_date, to_date))
    return jobs


def _run_download_jobs(jobs: List[Tuple[int, dict, str, str]], run_id: str):
    import asyncio
    
    async def run():
        fiuu_jobs = [(j, a, f, t) for j, a, f, t in jobs if a['platform'] == 'fiuu']
        playwright_jobs = [(j, a, f, t) for j, a, f, t in jobs if a['platform'] != 'fiuu']
        
        for job_id, account, from_date, to_date in fiuu_jobs:
            job_manager.update_job(job_id, 'running')
            _update_sheet(run_id)
            try:
                from src.services.fiuu import FiuuAPIClient
                client = FiuuAPIClient(account)
                count = client.fetch_and_store(from_date, to_date)
                job_manager.update_job(job_id, 'completed', transactions_count=count)
            except Exception as e:
                error_msg = str(e).split('Call log:')[0].strip()
                logger.error(f"Download failed: {account['label']} - {error_msg}")
                job_manager.update_job(job_id, 'failed', desc=error_msg)
            _update_sheet(run_id)
        
        if playwright_jobs:
            async with BrowserManager() as browser_manager:
                for job_id, account, from_date, to_date in playwright_jobs:
                    job_manager.update_job(job_id, 'running')
                    _update_sheet(run_id)
                    try:
                        scraper_class = get_scraper_class(account['platform'])
                        scraper = scraper_class(account)
                        files = await scraper.download_data(browser_manager, from_date, to_date)
                        filenames = [f.name for f in files]
                        filename = ','.join(filenames) if filenames else None
                        job_manager.update_job(job_id, 'completed', filename=filename, transactions_count=len(files))
                    except Exception as e:
                        error_msg = str(e).split('Call log:')[0].strip()
                        logger.error(f"Download failed: {account['label']} - {error_msg}")
                        job_manager.update_job(job_id, 'failed', desc=error_msg)
                    _update_sheet(run_id)
    
    asyncio.run(run())


def _update_sheet(run_id: str):
    jobs = job_manager.list_jobs(run_id=run_id)
    JobSheetService.update_jobs_sheet(jobs)
