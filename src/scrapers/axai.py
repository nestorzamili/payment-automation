from pathlib import Path
from typing import List

from playwright.async_api import Page

from src.core.logger import get_logger
from src.scrapers.base import BaseScraper

logger = get_logger(__name__)


class AxaiScraper(BaseScraper):
    
    LOGIN_PATH = "/login.html"
    TARGET_PATH = "/pay/bill/find"
    
    async def check_if_logged_in(self, page: Page) -> bool:
        try:
            user_info = page.locator('.user-info, #main-menu, .logout')
            return await user_info.is_visible(timeout=3000)
        except Exception:
            return False
    
    async def fill_login_credentials(self, page: Page):
        await page.fill('input[name="username"], input[name="email"], #username', self.credentials['email'])
        await page.fill('input[name="password"], #password', self.credentials['password'])
    
    async def submit_login(self, page: Page):
        await page.click('button[type="submit"], button:has-text("Login")')
    
    async def wait_for_login_success(self, page: Page):
        await page.wait_for_url('**/pay/**', timeout=30000)
    
    async def download_files(self, page: Page, download_dir: Path, from_date: str, to_date: str) -> List[Path]:
        logger.warning("AXAI scraper download_files() not implemented")
        return []
