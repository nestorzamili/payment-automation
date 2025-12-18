from playwright.async_api import async_playwright, Browser, BrowserContext, Page
from typing import Optional
from pathlib import Path
import json

from src.core.logger import get_logger
from src.core.loader import load_settings

logger = get_logger(__name__)


class BrowserManager:
    
    def __init__(self):
        self.settings = load_settings()
        self.browser: Optional[Browser] = None
        self.playwright = None
        
    async def __aenter__(self):
        await self.initialize()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        
    async def initialize(self):
        logger.info("Initializing Playwright browser")
        
        self.playwright = await async_playwright().start()
        
        self.browser = await self.playwright.chromium.launch(
            headless=self.settings['browser']['headless'],
            slow_mo=self.settings['browser']['slow_mo']
        )
        
        logger.info("Browser launched successfully")
        
    async def create_context(self, session_path: Optional[Path] = None) -> BrowserContext:
        if not self.browser:
            raise RuntimeError("Browser not initialized")
        
        context = await self.browser.new_context(
            locale=self.settings['locale'],
            timezone_id=self.settings['timezone'],
            geolocation={
                'latitude': self.settings['geolocation']['latitude'],
                'longitude': self.settings['geolocation']['longitude']
            },
            permissions=['geolocation'],
            user_agent=self.settings['browser']['user_agent'],
            viewport={'width': 1920, 'height': 1080}
        )
        
        context.set_default_timeout(self.settings['browser']['timeout'])
        
        if session_path and session_path.exists():
            logger.info(f"Loading session from {session_path}")
            try:
                with open(session_path, 'r', encoding='utf-8') as f:
                    cookies = json.load(f)
                await context.add_cookies(cookies)
                logger.info(f"Session loaded: {len(cookies)} cookies")
            except Exception as e:
                logger.warning(f"Failed to load session: {e}")
        
        return context
        
    async def save_session(self, context: BrowserContext, session_path: Path):
        try:
            session_path.parent.mkdir(parents=True, exist_ok=True)
            cookies = await context.cookies()
            
            with open(session_path, 'w', encoding='utf-8') as f:
                json.dump(cookies, f, indent=2)
            
            logger.info(f"Session saved: {len(cookies)} cookies")
        except Exception as e:
            logger.error(f"Failed to save session: {e}")
            raise
    
    async def close(self):
        if self.browser:
            await self.browser.close()
            
        if self.playwright:
            await self.playwright.stop()


async def create_page_with_kl_settings(context: BrowserContext) -> Page:
    page = await context.new_page()
    return page


async def wait_for_download(page: Page, download_dir: Path, timeout: int = 120000) -> Path:
    async with page.expect_download(timeout=timeout) as download_info:
        download = await download_info.value
        
        filename = download.suggested_filename
        download_path = download_dir / filename
        
        download_dir.mkdir(parents=True, exist_ok=True)
        
        await download.save_as(download_path)
        
        logger.info(f"Download completed: {download_path}")
        return download_path
