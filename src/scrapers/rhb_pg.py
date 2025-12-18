from pathlib import Path
from typing import List
from playwright.async_api import Page

from src.scrapers.base import BaseScraper
from src.core.logger import get_logger

logger = get_logger(__name__)


class PGRHBScraper(BaseScraper):
    
    def __init__(self, account: dict):
        super().__init__(account)
        self.merchant = account.get('merchant', 'Unknown')
        logger.info(f"RHB PG scraper for merchant: {self.merchant}")
    
    async def check_if_logged_in(self, page: Page) -> bool:
        try:
            dashboard_element = page.locator('div.dashboard')
            is_visible = await dashboard_element.is_visible(timeout=3000)
            return is_visible
        except Exception:
            return False
    
    async def fill_login_credentials(self, page: Page):
        logger.info("Filling RHB PG login credentials")
        
        email_selector = 'input[name="email"]'
        password_selector = 'input[name="password"]'
        
        await page.fill(email_selector, self.email)
        await page.fill(password_selector, self.password)
        
        logger.info("Credentials filled")
    
    async def submit_login(self, page: Page):
        logger.info("Submitting login form")
        
        login_button = 'button[type="submit"]'
        await page.click(login_button)
    
    async def wait_for_login_success(self, page: Page):
        logger.info("Waiting for login success")
        await page.wait_for_url('**/home', timeout=30000)
        logger.info("Login successful")
    
    async def download_files(self, page: Page, download_dir: Path, date_str: str) -> List[Path]:
        logger.info(f"Downloading RHB PG reports for {self.merchant} - Date: {date_str}")
        
        downloaded_files = []
        logger.warning("RHB PG scraper download_files() not fully implemented")
        
        return downloaded_files
