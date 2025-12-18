from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import List
from zoneinfo import ZoneInfo

from playwright.async_api import Page

from src.core.browser import BrowserManager, create_page_with_kl_settings
from src.core.loader import get_download_path, get_session_path
from src.core.logger import get_logger
from src.core.session import SessionManager

logger = get_logger(__name__)
KL_TZ = ZoneInfo('Asia/Kuala_Lumpur')


class BaseScraper(ABC):
    
    LOGIN_PATH = ""
    TARGET_PATH = ""
    
    def __init__(self, account: dict):
        self.account = account
        self.label = account['label']
        self.platform = account['platform']
        self.credentials = account['credentials']
        self.base_url = account['base_url']
        self.need_captcha = account.get('need_captcha', False)
        
        self.session_path = get_session_path(self.label)
        self.session_manager = SessionManager()
    
    @property
    def login_url(self) -> str:
        return self.base_url + self.LOGIN_PATH
    
    @property
    def target_url(self) -> str:
        return self.base_url + self.TARGET_PATH
    
    async def download_data(self, browser_manager: BrowserManager, from_date: str = None, to_date: str = None) -> List[Path]:
        logger.info(f"Starting download: {self.label}")
        
        has_session = self.session_manager.session_exists(self.session_path)
        
        context = await browser_manager.create_context(
            session_path=self.session_path if has_session else None
        )
        
        try:
            page = await create_page_with_kl_settings(context)
            
            await page.goto(self.target_url, wait_until='networkidle')
            
            needs_login = await self._check_needs_login(page)
            
            if needs_login:
                logger.info(f"Session expired, login required: {self.label}")
                
                if self.need_captcha:
                    await context.close()
                    return await self._login_with_visible_browser(from_date, to_date)
                
                await page.goto(self.login_url, wait_until='networkidle')
                await self.perform_login(page)
                await browser_manager.save_session(context, self.session_path)
                await page.goto(self.target_url, wait_until='networkidle')
            else:
                logger.info(f"Session valid: {self.label}")
            
            if not from_date:
                from_date = datetime.now(KL_TZ).strftime("%Y-%m-%d")
            if not to_date:
                to_date = datetime.now(KL_TZ).strftime("%Y-%m-%d")
            
            download_dir = get_download_path(self.label)
            
            downloaded_files = await self.download_files(page, download_dir, from_date, to_date)
            
            logger.info(f"Download completed: {self.label} ({len(downloaded_files)} files)")
            return downloaded_files
            
        except Exception as e:
            error_msg = str(e).split('Call log:')[0].strip()
            logger.error(f"Download failed: {self.label} - {error_msg}")
            raise
        finally:
            try:
                await context.close()
            except Exception:
                pass
    
    async def _check_needs_login(self, page: Page) -> bool:
        current_url = page.url
        
        if 'login' in current_url.lower():
            return True
        
        is_logged_in = await self.check_if_logged_in(page)
        return not is_logged_in
    
    async def _login_with_visible_browser(self, from_date: str = None, to_date: str = None) -> List[Path]:
        logger.info(f"Visible browser for CAPTCHA: {self.label}")
        
        async with BrowserManager(headless_override=False) as visible_browser:
            context = await visible_browser.create_context(session_path=None)
            page = await create_page_with_kl_settings(context)
            
            await page.goto(self.login_url, wait_until='networkidle')
            await self.perform_login(page)
            await visible_browser.save_session(context, self.session_path)
            
            await page.goto(self.target_url, wait_until='networkidle')
            
            if not from_date:
                from_date = datetime.now(KL_TZ).strftime("%Y-%m-%d")
            if not to_date:
                to_date = datetime.now(KL_TZ).strftime("%Y-%m-%d")
            
            download_dir = get_download_path(self.label)
            
            downloaded_files = await self.download_files(page, download_dir, from_date, to_date)
            
            logger.info(f"Download completed: {self.label} ({len(downloaded_files)} files)")
            return downloaded_files
    
    @abstractmethod
    async def check_if_logged_in(self, page: Page) -> bool:
        pass
    
    async def perform_login(self, page: Page):
        await self.fill_login_credentials(page)
        
        if self.need_captcha:
            print(f"\nCAPTCHA REQUIRED: {self.label}")
            print("Please solve the CAPTCHA then press ENTER...")
            input()
        
        await self.submit_login(page)
        await self.wait_for_login_success(page)
        
        logger.info(f"Login successful: {self.label}")
    
    @abstractmethod
    async def fill_login_credentials(self, page: Page):
        pass
    
    @abstractmethod
    async def submit_login(self, page: Page):
        pass
    
    @abstractmethod
    async def wait_for_login_success(self, page: Page):
        pass
    
    @abstractmethod
    async def download_files(self, page: Page, download_dir: Path, from_date: str, to_date: str) -> List[Path]:
        pass


def get_scraper_class(platform: str):
    from src.scrapers.axai import AxaiScraper
    from src.scrapers.fiuu import FiuuScraper
    from src.scrapers.kira import KiraScraper
    from src.scrapers.m1 import M1Scraper
    
    scraper_map = {
        'kira': KiraScraper,
        'axai': AxaiScraper,
        'm1': M1Scraper,
        'fiuu': FiuuScraper,
    }
    
    scraper_class = scraper_map.get(platform)
    if not scraper_class:
        raise ValueError(f"Unknown platform: {platform}")
    
    return scraper_class
