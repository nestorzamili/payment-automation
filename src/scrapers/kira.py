import asyncio
from datetime import datetime
from pathlib import Path
from typing import List

from playwright.async_api import Page

from src.core.logger import get_logger
from src.scrapers.base import BaseScraper

logger = get_logger(__name__)


class KiraScraper(BaseScraper):
    
    LOGIN_PATH = "/mms/login"
    TARGET_PATH = "/mms/home#!/dashboard"
    
    async def check_if_logged_in(self, page: Page) -> bool:
        return '/mms/login' not in page.url
    
    async def fill_login_credentials(self, page: Page):
        await page.fill('#loginID', self.credentials['username'])
        await page.fill('#loginPassword', self.credentials['password'])
    
    async def submit_login(self, page: Page):
        await page.click('.login-button button')
    
    async def wait_for_login_success(self, page: Page):
        await page.wait_for_url('**/home**', timeout=30000)
    
    async def download_files(self, page: Page, download_dir: Path, from_date: str, to_date: str) -> List[Path]:
        logger.info(f"Downloading KIRA: {from_date} to {to_date}")
        download_dir.mkdir(parents=True, exist_ok=True)
        
        logger.info("Clicking Transactions on sidebar")
        await page.locator('a[href="#!transactions"]').click()
        await page.wait_for_load_state('networkidle')
        await asyncio.sleep(1)
        
        logger.info(f"Selecting From Date: {from_date}")
        await self._select_date(page, 'input[ng-model="selectedStartDate"]', from_date)
        
        logger.info(f"Selecting To Date: {to_date}")
        await self._select_date(page, 'input[ng-model="selectedEndDate"]', to_date)
        
        logger.info("Selecting Transaction Status: SUCCESS")
        await page.locator('button[data-id="selectedTransactionStatus"]').click()
        await asyncio.sleep(0.5)
        
        await page.locator('.dropdown-menu.show .bs-deselect-all').click()
        await asyncio.sleep(0.3)
        
        await page.locator('.dropdown-menu.show .dropdown-item').filter(has_text="SUCCESS").click()
        
        await page.keyboard.press('Escape')
        await asyncio.sleep(0.3)
        
        logger.info("Clicking Search button")
        await page.get_by_role("button", name="Search").click()
        await page.wait_for_load_state('networkidle')
        await asyncio.sleep(1)
        
        logger.info("Clicking Export button")
        await page.get_by_role("button", name="Export").click()
        await asyncio.sleep(1)
        
        logger.info("Clicking Yes on modal to go to Export History")
        await page.locator('#dialogModalButton2').click()
        await page.wait_for_load_state('networkidle')
        await asyncio.sleep(2)
        
        logger.info("Clicking Search on Export History")
        await page.locator('button[ng-click="refreshDisplay()"]').click()
        await page.wait_for_load_state('networkidle')
        await asyncio.sleep(2)
        
        first_row = page.locator('#reportListTable tbody tr').first
        status = await first_row.locator('td:nth-child(10)').text_content()
        status = status.strip() if status else ""
        
        if status != "Completed":
            logger.info(f"Export status: {status} - file not ready yet")
            return []
        
        logger.info("Export completed, clicking Download button")
        download_btn = first_row.locator('button[ng-click^="downloadReport"]')
        
        async with page.expect_download() as download_info:
            await download_btn.click()
        
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
        
        date_input = page.locator(input_selector)
        await date_input.click()
        await asyncio.sleep(0.5)
        
        datepicker = page.locator('.uib-datepicker-popup')
        await datepicker.wait_for(state='visible')
        
        while True:
            title_btn = datepicker.locator('.uib-title strong')
            title_text = await title_btn.text_content()
            
            parts = title_text.strip().split()
            current_month_name = parts[0].upper()
            current_year = int(parts[1])
            
            month_names = ['JANUARY', 'FEBRUARY', 'MARCH', 'APRIL', 'MAY', 'JUNE',
                          'JULY', 'AUGUST', 'SEPTEMBER', 'OCTOBER', 'NOVEMBER', 'DECEMBER']
            current_month = month_names.index(current_month_name) + 1
            
            if current_year == target_year and current_month == target_month:
                break
            
            if (current_year > target_year) or (current_year == target_year and current_month > target_month):
                await datepicker.locator('.uib-left').click()
            else:
                await datepicker.locator('.uib-right').click()
            
            await asyncio.sleep(0.3)
        
        day_str = str(target_day).zfill(2)
        day_btn = datepicker.locator(f'.uib-day button:not([disabled]) span:not(.text-muted):text-is("{day_str}")')
        
        if await day_btn.count() == 0:
            day_btn = datepicker.locator(f'.uib-day button:not([disabled]) span:not(.text-muted):text-is("{target_day}")')
        
        await day_btn.click()
        await asyncio.sleep(0.3)
    

