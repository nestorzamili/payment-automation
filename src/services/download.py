from __future__ import annotations

import asyncio
import threading
import uuid
from pathlib import Path
from typing import List, Tuple

from src.core.jobs import job_manager
from src.core.logger import get_logger
from src.scrapers import BrowserManager, get_scraper_class
from src.utils.date_range import DateRangeService

logger = get_logger(__name__)
date_service = DateRangeService()


def get_date_range(platform: str) -> tuple[str, str] | None:
    return date_service.get_date_range(platform)


def run_download_jobs(jobs: List[Tuple[int, dict]], from_date: str, to_date: str):
    async def run():
        async with BrowserManager() as browser_manager:
            for job_id, account in jobs:
                job_manager.update_job(job_id, 'running')   
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
    
    asyncio.run(run())


def start_platform_download(platform: str, accounts: list, from_date: str, to_date: str) -> dict:
    job_type = "download"
    run_id = str(uuid.uuid4())
    
    jobs = []
    for account in accounts:
        job_id = job_manager.create_job(
            job_type=job_type,
            run_id=run_id,
            platform=platform,
            account_label=account['label'],
            from_date=from_date,
            to_date=to_date
        )
        jobs.append((job_id, account))
    
    def run_jobs():
        run_download_jobs(jobs, from_date, to_date)
    
    thread = threading.Thread(target=run_jobs, daemon=True)
    thread.start()
    
    logger.info(f"Download jobs started: run_id={run_id} ({platform}, {len(accounts)} accounts, {from_date} to {to_date})")
    
    return {
        'run_id': run_id,
        'platform': platform,
        'accounts': [a['label'] for a in accounts],
        'from_date': from_date,
        'to_date': to_date
    }


def start_account_download(account: dict, from_date: str, to_date: str) -> dict:
    job_type = "download"
    run_id = str(uuid.uuid4())
    label = account['label']
    platform = account['platform']
    
    job_id = job_manager.create_job(
        job_type=job_type,
        run_id=run_id,
        platform=platform,
        account_label=label,
        from_date=from_date,
        to_date=to_date
    )
    
    def run_jobs():
        run_download_jobs([(job_id, account)], from_date, to_date)
    
    thread = threading.Thread(target=run_jobs, daemon=True)
    thread.start()
    
    logger.info(f"Download job started: {job_id} ({label}, {from_date} to {to_date})")
    
    return {
        'run_id': run_id,
        'job_id': job_id,
        'label': label,
        'platform': platform,
        'from_date': from_date,
        'to_date': to_date
    }


def check_running_download() -> dict | None:
    return job_manager.get_running_job_by_type("download")
