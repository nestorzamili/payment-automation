import asyncio
from datetime import datetime
from pathlib import Path
from typing import List

from playwright.async_api import Page

from src.core.logger import get_logger
from src.scrapers.base import BaseScraper

logger = get_logger(__name__)


class M1Scraper(BaseScraper):
    
    LOGIN_PATH = "/user/login"
    TARGET_PATH = "/transaction/fpx-list"
    
    async def check_if_logged_in(self, page: Page) -> bool:
        return self.TARGET_PATH in page.url
    
    async def fill_login_credentials(self, page: Page):
        await page.get_by_label("Username").fill(self.credentials['username'])
        await page.get_by_label("Password").fill(self.credentials['password'])
    
    async def submit_login(self, page: Page):
        await page.get_by_role("button", name="Login").click()
    
    async def wait_for_login_success(self, page: Page):
        await page.wait_for_url(lambda url: '/user/login' not in url, timeout=30000)
    
    async def download_files(self, page: Page, download_dir: Path, from_date: str, to_date: str) -> List[Path]:
        logger.info(f"Downloading M1: {from_date} to {to_date}")
        download_dir.mkdir(parents=True, exist_ok=True)
        
        from_dt = datetime.strptime(from_date, '%Y-%m-%d')
        to_dt = datetime.strptime(to_date, '%Y-%m-%d')
        formatted_from = from_dt.strftime('%m/%d/%Y')
        formatted_to = to_dt.strftime('%m/%d/%Y')
        
        logger.info(f"Filling From Date: {formatted_from}")
        await page.get_by_label("From Date").clear()
        await page.get_by_label("From Date").fill(formatted_from)
        
        logger.info(f"Filling To Date: {formatted_to}")
        await page.get_by_label("To Date").clear()
        await page.get_by_label("To Date").fill(formatted_to)
        
        logger.info("Selecting Transaction Status: SUCCESS")
        await page.get_by_role("combobox").first.dispatch_event('click')
        await asyncio.sleep(0.5)
        await page.get_by_role("option", name="SUCCESS").click()
        await asyncio.sleep(0.5)
        
        logger.info("Clicking Search button")
        await page.get_by_role("button", name="search").click()
        await page.wait_for_load_state('networkidle')
        
        logger.info("Clicking Export All button")
        async with page.expect_download() as download_info:
            await page.get_by_role("button", name="Export All", exact=True).click()
        
        download = await download_info.value
        
        filename = f"{self.label}_{from_date}_{to_date}.xlsx"
        file_path = download_dir / filename
        await download.save_as(file_path)
        
        logger.info(f"Downloaded: {filename}")
        return [file_path]

