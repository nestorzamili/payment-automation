from pathlib import Path
from typing import List
from playwright.async_api import Page

from src.scrapers.base import BaseScraper
from src.core.logger import get_logger

logger = get_logger(__name__)


class KiraScraper(BaseScraper):
    
    async def check_if_logged_in(self, page: Page) -> bool:
        try:
            dashboard_element = page.locator('div.dashboard, #user-menu, .logout-button')
            is_visible = await dashboard_element.is_visible(timeout=3000)
            return is_visible
        except Exception:
            return False
    
    async def fill_login_credentials(self, page: Page):
        logger.info("Filling login credentials")
        
        email_selector = 'input[name="email"], input[type="email"], #email'
        password_selector = 'input[name="password"], input[type="password"], #password'
        
        await page.fill(email_selector, self.email)
        await page.fill(password_selector, self.password)
        
        logger.info("Credentials filled")
    
    async def submit_login(self, page: Page):
        logger.info("Submitting login form")
        
        login_button = 'button[type="submit"], button:has-text("Login"), #login-btn'
        
        await page.click(login_button)
    
    async def wait_for_login_success(self, page: Page):
        logger.info("Waiting for login success")
        await page.wait_for_url('**/dashboard', timeout=30000)
        logger.info("Login successful - dashboard loaded")
    
    async def download_files(self, page: Page, download_dir: Path, date_str: str) -> List[Path]:
        logger.info(f"Navigating to KIRA reports page for date: {date_str}")
        
        downloaded_files = []
        logger.warning("KIRA scraper download_files() not fully implemented - update with actual portal logic")
        
        return downloaded_files
