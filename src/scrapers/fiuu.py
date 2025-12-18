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
        try:
            dashboard = page.locator('div.dashboard, #main-content, .user-profile')
            return await dashboard.is_visible(timeout=3000)
        except Exception:
            return False
    
    async def fill_login_credentials(self, page: Page):
        await page.fill('input[name="email"], #email', self.credentials['email'])
        await page.fill('input[name="username"], #username', self.credentials['username'])
        await page.fill('input[name="password"], #password', self.credentials['password'])
    
    async def submit_login(self, page: Page):
        await page.click('button[type="submit"], button:has-text("Login")')
    
    async def wait_for_login_success(self, page: Page):
        await page.wait_for_url('**/home**', timeout=30000)
    
    async def download_files(self, page: Page, download_dir: Path, date_str: str) -> List[Path]:
        logger.warning("FIUU scraper download_files() not implemented")
        return []
