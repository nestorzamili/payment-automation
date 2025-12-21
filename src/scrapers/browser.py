from pathlib import Path
from typing import Optional

from playwright.async_api import Browser, BrowserContext, Page, async_playwright

from src.core.loader import load_settings
from src.core.logger import get_logger

logger = get_logger(__name__)


class BrowserManager:
    
    def __init__(self, headless_override: Optional[bool] = None):
        self.settings = load_settings()
        self.headless_override = headless_override
        self.browser: Optional[Browser] = None
        self.playwright = None
        
    async def __aenter__(self):
        await self.initialize()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
        
    async def initialize(self):
        headless = self.headless_override if self.headless_override is not None \
                   else self.settings['browser']['headless']
        
        logger.info(f"Browser initialized (headless={headless})")
        
        launch_args = []
        if not headless:
            launch_args.append('--start-maximized')
        
        self.playwright = await async_playwright().start()
        self.browser = await self.playwright.chromium.launch(
            headless=headless,
            slow_mo=100,
            args=launch_args
        )
        self.headless = headless
        
    async def create_context(self, session_path: Optional[Path] = None) -> BrowserContext:
        if not self.browser:
            raise RuntimeError("Browser not initialized")
        
        context_options = {
            'locale': self.settings['locale'],
            'timezone_id': self.settings['timezone'],
            'geolocation': {
                'latitude': self.settings['geolocation']['latitude'],
                'longitude': self.settings['geolocation']['longitude']
            },
            'permissions': ['geolocation'],
            'user_agent': self.settings['browser']['user_agent'],
        }
        
        if self.headless:
            context_options['viewport'] = {'width': 1920, 'height': 1080}
        else:
            context_options['no_viewport'] = True
        
        if session_path and session_path.exists():
            context_options['storage_state'] = str(session_path)
            logger.info(f"Session loaded: {session_path}")
        
        context = await self.browser.new_context(**context_options)
        context.set_default_timeout(self.settings['browser']['timeout'])
        
        return context
        
    async def save_session(self, context: BrowserContext, session_path: Path):
        try:
            session_path.parent.mkdir(parents=True, exist_ok=True)
            await context.storage_state(path=str(session_path))
            logger.info(f"Session saved: {session_path}")
        except Exception as e:
            logger.error(f"Failed to save session: {e}")
            raise
    
    async def close(self):
        if self.browser:
            await self.browser.close()
            
        if self.playwright:
            await self.playwright.stop()


async def create_page_with_kl_settings(context: BrowserContext) -> Page:
    return await context.new_page()


async def wait_for_download(page: Page, download_dir: Path, timeout: int = 120000) -> Path:
    async with page.expect_download(timeout=timeout) as download_info:
        download = await download_info.value
        
        filename = download.suggested_filename
        download_path = download_dir / filename
        
        download_dir.mkdir(parents=True, exist_ok=True)
        
        await download.save_as(download_path)
        
        logger.info(f"Downloaded: {download_path.name}")
        return download_path
