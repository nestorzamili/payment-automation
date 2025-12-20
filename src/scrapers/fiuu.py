import asyncio
from datetime import datetime
from pathlib import Path
from typing import List

from playwright.async_api import Page

from src.core.logger import get_logger
from src.scrapers.base import BaseScraper

logger = get_logger(__name__)


class FiuuScraper(BaseScraper):
    
    LOGIN_PATH = "/index.php?mod=authentication&opt=login"
    TARGET_PATH = "/index.php?mod=reports&opt=transaction"
    
    async def check_if_logged_in(self, page: Page) -> bool:
        return 'mod=home' in page.url or 'mod=reports' in page.url
    
    async def fill_login_credentials(self, page: Page):
        await page.locator('#btnSbmt').wait_for(state='visible', timeout=30000)
        await asyncio.sleep(0.5)
        await page.fill('#txt_merchant', self.credentials['username'])
        await asyncio.sleep(0.3)
        await page.fill('#txt_username', self.credentials['email'])
        await asyncio.sleep(0.3)
        await page.locator('input[type="password"]').fill(self.credentials['password'])
        await asyncio.sleep(0.3)
    
    async def submit_login(self, page: Page):
        await page.click('#btnSbmt')
        await asyncio.sleep(1)
    
    async def wait_for_login_success(self, page: Page):
        await page.wait_for_url('**/mod=home**', timeout=30000)
    
    async def download_files(self, page: Page, download_dir: Path, from_date: str, to_date: str) -> List[Path]:
        logger.info(f"Downloading FIUU: {from_date} to {to_date}")
        download_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info("Navigating to Transaction Reports")
        await page.goto(self.base_url + self.TARGET_PATH, wait_until='networkidle')
        await asyncio.sleep(1)
        
        logger.info(f"Selecting From Date: {from_date}")
        await self._select_date(page, '#txt_daterange_start', from_date)
        
        logger.info(f"Selecting To Date: {to_date}")
        await self._select_date(page, '#txt_daterange_end', to_date)
        
        logger.info("Selecting Status: Settled")
        await page.select_option('#txt_daily_status', 'settled')
        await asyncio.sleep(0.3)
        
        logger.info("Selecting Channel: FPX")
        await page.select_option('#txt_daterange_channel', 'FPX-TPA')
        await asyncio.sleep(0.3)
        
        logger.info("Selecting Export Type: Excel")
        await page.locator('#txt_export_type_xlsx').click()
        await asyncio.sleep(0.3)
        
        logger.info("Clicking Send Request")
        await page.locator('#btnSbmtDateRange').click()
        await page.wait_for_load_state('networkidle')
        await asyncio.sleep(2)
        
        logger.info("Waiting for report table")
        await page.locator('#tbl_report').wait_for(state='visible', timeout=30000)
        await asyncio.sleep(1)
        
        logger.info("Clicking Download on first row")
        download_link = page.locator('#tbl_report tbody tr').first.locator('td:last-child a')
        
        async with page.expect_download() as download_info:
            await download_link.click()
        
        download = await download_info.value
        
        filename = f"{self.label}_{from_date}_{to_date}.xlsx"
        file_path = download_dir / filename
        await download.save_as(file_path)
        
        logger.info(f"Downloaded: {filename}")
        return [file_path]
    
    async def _select_date(self, page: Page, input_selector: str, date_str: str):
        target_date = datetime.strptime(date_str, '%Y-%m-%d')
        target_year = target_date.year
        target_month = target_date.month
        target_day = target_date.day
        
        await page.locator(input_selector).click()
        await asyncio.sleep(0.5)
        
        datepicker = page.locator('.datepicker-dropdown')
        await datepicker.wait_for(state='visible')
        
        month_names = ['January', 'February', 'March', 'April', 'May', 'June',
                       'July', 'August', 'September', 'October', 'November', 'December']
        
        while True:
            title = await datepicker.locator('.datepicker-switch').text_content()
            parts = title.strip().split()
            current_month_name = parts[0]
            current_year = int(parts[1])
            current_month = month_names.index(current_month_name) + 1
            
            if current_year == target_year and current_month == target_month:
                break
            
            if (current_year > target_year) or (current_year == target_year and current_month > target_month):
                await datepicker.locator('th.prev').click()
            else:
                await datepicker.locator('th.next').click()
            
            await asyncio.sleep(0.3)
        
        day_cells = datepicker.locator('.datepicker-days td.day:not(.old):not(.new):not(.disabled)')
        count = await day_cells.count()
        
        for i in range(count):
            cell = day_cells.nth(i)
            text = await cell.text_content()
            if text.strip() == str(target_day):
                await cell.click()
                break
        
        await asyncio.sleep(0.3)
