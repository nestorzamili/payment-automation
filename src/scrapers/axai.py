import asyncio
import random
from pathlib import Path
from typing import List

from playwright.async_api import Page

from src.core.logger import get_logger
from src.scrapers.base import BaseScraper

logger = get_logger(__name__)


async def human_delay(min_sec: float = 0.5, max_sec: float = 1.5):
    await asyncio.sleep(random.uniform(min_sec, max_sec))


class AxaiScraper(BaseScraper):
    
    LOGIN_PATH = "/login.html"
    TARGET_PATH = ""
    
    async def check_if_logged_in(self, page: Page) -> bool:
        try:
            navbar = page.locator('.navbar-nav')
            return await navbar.is_visible(timeout=3000)
        except Exception:
            return False
    
    async def fill_login_credentials(self, page: Page):
        pass
    
    async def submit_login(self, page: Page):
        pass
    
    async def wait_for_login_success(self, page: Page):
        await page.wait_for_function(
            f"() => !window.location.href.includes('{self.LOGIN_PATH}')",
            timeout=0
        )
        await page.wait_for_load_state('networkidle')
        logger.info(f"Login successful, redirected to: {page.url}")
    
    async def navigate_to_payment_details(self, page: Page):
        logger.info("Navigating to Payment Details...")
        
        payment_dropdown = page.locator('#topnav-user-Interface5000')
        await payment_dropdown.hover()
        await human_delay(1.0, 2.0)
        
        payment_details = page.locator('a.dropdown-item:has-text("Payment Details")')
        await payment_details.click()
        
        await page.wait_for_load_state('networkidle')
        await human_delay(1.0, 2.0)
        
        search_button = page.locator('button:has-text("Search")')
        await search_button.wait_for(state='visible', timeout=self.timeout)
        
        logger.info("Successfully navigated to Payment Details page")
    
    async def select_transaction_status(self, page: Page):
        select2_container = page.locator('select[name="status[]"]').locator('xpath=..').locator('.select2-selection')
        await select2_container.click()
        await human_delay(1.0, 2.0)
        
        option = page.locator('.select2-results__option:has-text("Paid&Settlement Successful")')
        await option.click()
        await human_delay(0.5, 1.0)
        
        await page.keyboard.press('Escape')
        await human_delay(0.5, 1.0)
    
    async def fill_date_range(self, page: Page, from_date: str, to_date: str):
        start_input = page.locator('input[name="startDate"]')
        await start_input.click()
        await human_delay(0.8, 1.5)
        await start_input.clear()
        await human_delay(0.3, 0.6)
        await start_input.type(from_date, delay=100)
        await human_delay(1.0, 1.5)
        await page.keyboard.press('Tab')
        await human_delay(1.0, 2.0)
        
        end_input = page.locator('input[name="endDate"]')
        await end_input.click()
        await human_delay(0.8, 1.5)
        await end_input.clear()
        await human_delay(0.3, 0.6)
        await end_input.type(to_date, delay=100)
        await human_delay(1.0, 1.5)
        await page.keyboard.press('Tab')
        await human_delay(1.0, 2.0)
    
    async def search_and_export(self, page: Page, download_dir: Path, from_date: str, to_date: str) -> List[Path]:
        search_button = page.locator('button:has-text("Search")')
        await search_button.click()
        
        await page.wait_for_load_state('networkidle')
        await human_delay(2.0, 3.5)
        
        no_data = page.locator('tr.no-records-found')
        if await no_data.is_visible():
            logger.info("No data found for the selected date range")
            return []
        
        filename = f"{self.label}_{from_date}_{to_date}.xlsx"
        file_path = download_dir / filename
        
        await human_delay(0.5, 1.0)
        
        async with page.expect_download() as download_info:
            excel_button = page.locator('a[href*="exportExcel"]')
            await excel_button.click()
        
        download = await download_info.value
        await download.save_as(file_path)
        
        logger.info(f"Downloaded: {filename}")
        return [file_path]
    
    async def download_files(self, page: Page, download_dir: Path, from_date: str, to_date: str) -> List[Path]:
        logger.info(f"Downloading AXAI: {from_date} to {to_date}")
        download_dir.mkdir(parents=True, exist_ok=True)
        await self.navigate_to_payment_details(page)
        logger.info("Selecting transaction status")
        await self.select_transaction_status(page)
        logger.info(f"Filling date range: {from_date} to {to_date}")
        await self.fill_date_range(page, from_date, to_date)
        logger.info("Searching and exporting")
        return await self.search_and_export(page, download_dir, from_date, to_date)
