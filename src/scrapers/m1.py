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
    FPX_PATH = "/transaction/fpx-list"
    EWALLET_PATH = "/transaction/e-wallet"
    
    async def check_if_logged_in(self, page: Page) -> bool:
        return '/user/login' not in page.url
    
    async def fill_login_credentials(self, page: Page):
        await page.get_by_label("Username").fill(self.credentials['username'])
        await page.get_by_label("Password").fill(self.credentials['password'])
    
    async def submit_login(self, page: Page):
        await page.get_by_role("button", name="Login").click()
    
    async def wait_for_login_success(self, page: Page):
        await page.wait_for_url(lambda url: '/user/login' not in url, timeout=self.timeout)
    
    async def download_files(self, page: Page, download_dir: Path, from_date: str, to_date: str) -> List[Path]:
        logger.info(f"Downloading M1: {from_date} to {to_date}")
        download_dir.mkdir(parents=True, exist_ok=True)
        
        downloaded_files = []
        
        fpx_files = await self._download_fpx(page, download_dir, from_date, to_date)
        downloaded_files.extend(fpx_files)
        
        ewallet_files = await self._download_ewallet(page, download_dir, from_date, to_date)
        downloaded_files.extend(ewallet_files)
        
        return downloaded_files
    
    async def _download_fpx(self, page: Page, download_dir: Path, from_date: str, to_date: str) -> List[Path]:
        logger.info("Downloading FPX transactions")
        
        await page.goto(f"{self.base_url}{self.FPX_PATH}")
        await page.wait_for_load_state('networkidle')
        
        file_path = await self._search_and_export(
            page, download_dir, from_date, to_date,
            filename=f"{self.label}_fpx_{from_date}_{to_date}.xlsx"
        )
        
        return [file_path] if file_path else []
    
    async def _download_ewallet(self, page: Page, download_dir: Path, from_date: str, to_date: str) -> List[Path]:
        logger.info("Downloading E-Wallet transactions")
        
        await page.goto(f"{self.base_url}{self.EWALLET_PATH}")
        await page.wait_for_load_state('networkidle')
        
        channels = await self._get_ewallet_channels(page)
        logger.info(f"Found {len(channels)} channels: {channels}")
        
        downloaded_files = []
        for channel in channels:
            logger.info(f"Processing channel: {channel}")
            
            await page.locator('#mat-select-2').click()
            await asyncio.sleep(0.3)
            await page.get_by_role('option', name=channel, exact=True).click()
            await asyncio.sleep(0.3)
            
            channel_slug = channel.lower().replace("'", "").replace(" ", "_")
            filename = f"{self.label}_ewallet_{channel_slug}_{from_date}_{to_date}.xlsx"
            
            file_path = await self._search_and_export(page, download_dir, from_date, to_date, filename)
            if file_path:
                downloaded_files.append(file_path)
        
        return downloaded_files
    
    async def _get_ewallet_channels(self, page: Page) -> List[str]:
        await page.locator('#mat-select-2').click()
        await asyncio.sleep(0.5)
        
        options = await page.locator('mat-option .mdc-list-item__primary-text').all_text_contents()
        
        await page.keyboard.press('Escape')
        await asyncio.sleep(0.3)
        
        return [opt.strip() for opt in options if opt.strip()]
    
    async def _search_and_export(self, page: Page, download_dir: Path, from_date: str, to_date: str, filename: str) -> Path | None:
        from_dt = datetime.strptime(from_date, '%Y-%m-%d')
        to_dt = datetime.strptime(to_date, '%Y-%m-%d')
        formatted_from = from_dt.strftime('%m/%d/%Y')
        formatted_to = to_dt.strftime('%m/%d/%Y')
        
        await page.get_by_label("From Date").clear()
        await page.get_by_label("From Date").fill(formatted_from)
        
        await page.get_by_label("To Date").clear()
        await page.get_by_label("To Date").fill(formatted_to)
        
        status_dropdown = page.get_by_role("combobox").first
        await status_dropdown.dispatch_event('click')
        await asyncio.sleep(0.5)
        await page.get_by_role("option", name="SUCCESS").click()
        await asyncio.sleep(0.5)
        
        await page.get_by_role("button", name="search").click()
        await page.wait_for_load_state('networkidle')
        await asyncio.sleep(1)
        
        no_data_msg = page.locator('.message:has-text("There is no data")')
        if await no_data_msg.count() > 0:
            logger.info(f"No data found for {filename}")
            return None
        
        export_button = page.get_by_role("button", name="Export All", exact=True)
        
        try:
            await export_button.wait_for(state='visible', timeout=self.timeout)
        except:
            logger.warning(f"Export button not visible for {filename}")
            return None
        
        try:
            async with page.expect_download(timeout=self.download_timeout) as download_info:
                await export_button.click()
            
            download = await download_info.value
            file_path = download_dir / filename
            await download.save_as(file_path)
            logger.info(f"Downloaded: {filename}")
            return file_path
        except Exception as e:
            logger.warning(f"No data to export for {filename}: {e}")
            return None
