from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional, List
from datetime import datetime
from zoneinfo import ZoneInfo

from playwright.async_api import Page, BrowserContext
from src.core.browser import BrowserManager, create_page_with_kl_settings
from src.core.logger import get_logger
from src.core.session import SessionManager
from src.core.loader import get_session_path, get_download_path

logger = get_logger(__name__)
KL_TZ = ZoneInfo('Asia/Kuala_Lumpur')


class BaseScraper(ABC):
    
    def __init__(self, account: dict):
        self.account = account
        self.label = account['label']
        self.platform = account['platform']
        self.username = account['username']
        self.password = account['password']
        self.login_url = account['login_url']
        self.target_url = account['target_url']
        self.need_captcha = account.get('need_captcha', False)
        
        self.session_path = get_session_path(self.label)
        self.session_manager = SessionManager()
        
        logger.info(f"Initialized scraper for {self.label} ({self.platform})")
    
    async def download_today_data(self, browser_manager: BrowserManager) -> List[Path]:
        logger.info(f"Starting download for {self.label}")
        
        has_session = self.session_manager.session_exists(self.session_path)
        
        context = await browser_manager.create_context(
            session_path=self.session_path if has_session else None
        )
        
        try:
            page = await create_page_with_kl_settings(context)
            
            logger.info(f"Navigating to {self.login_url}")
            await page.goto(self.login_url, wait_until='networkidle')
            
            is_logged_in = await self.check_if_logged_in(page)
            
            if not is_logged_in:
                logger.info(f"Not logged in, performing login for {self.label}")
                await self.perform_login(page)
                
                await browser_manager.save_session(context, self.session_path)
            else:
                logger.info(f"Already logged in using saved session for {self.label}")
            
            logger.info(f"Navigating to target: {self.target_url}")
            await page.goto(self.target_url, wait_until='networkidle')
            
            today = datetime.now(KL_TZ).strftime("%Y-%m-%d")
            download_dir = get_download_path(self.platform, self.label, today)
            
            downloaded_files = await self.download_files(page, download_dir, today)
            
            logger.info(f"Download completed for {self.label}: {len(downloaded_files)} files")
            return downloaded_files
            
        except Exception as e:
            error_msg = str(e).split('Call log:')[0].strip()
            logger.error(f"Error during download for {self.label}: {error_msg}")
            raise
        finally:
            await context.close()
    
    @abstractmethod
    async def check_if_logged_in(self, page: Page) -> bool:
        pass
    
    async def perform_login(self, page: Page):
        logger.info(f"Starting login process for {self.label}")
        
        await self.fill_login_credentials(page)
        
        if self.need_captcha:
            logger.warning(f"CAPTCHA required for {self.label}")
            print(f"\nCAPTCHA REQUIRED FOR: {self.label}")
            print(f"Please solve the CAPTCHA in the browser window.")
            print(f"Then press ENTER to continue...\n")
            input()
            logger.info(f"User confirmed CAPTCHA solved for {self.label}")
        
        await self.submit_login(page)
        await self.wait_for_login_success(page)
        
        logger.info(f"Login successful for {self.label}")
    
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
    async def download_files(self, page: Page, download_dir: Path, date_str: str) -> List[Path]:
        pass


from src.scrapers.kira import KiraScraper
from src.scrapers.ragnarok import PGRagnarokScraper
from src.scrapers.m1pay import PGM1payScraper
from src.scrapers.rhb_pg import PGRHBScraper
from src.scrapers.rhb_bank import BankRHBScraper


def get_scraper_class(platform: str):
    scraper_map = {
        'kira': KiraScraper,
        'pg_ragnarok': PGRagnarokScraper,
        'pg_m1pay': PGM1payScraper,
        'pg_rhb': PGRHBScraper,
        'bank_rhb': BankRHBScraper,
    }
    
    scraper_class = scraper_map.get(platform)
    if not scraper_class:
        raise ValueError(f"Unknown platform: {platform}. Available: {list(scraper_map.keys())}")
    
    return scraper_class
