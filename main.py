import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

from src.core import (
    get_logger,
    setup_logger,
    get_kl_timestamp,
    load_accounts,
    load_settings,
    PROJECT_ROOT,
    BrowserManager
)
from src.scrapers import get_scraper_class
from src.processors import (
    process_kira_folder,
    process_pg_folder,
    process_bank_folder,
    merge_data,
    load_malaysia_holidays
)
from src.sheets import SheetsClient

logger = get_logger(__name__)
KL_TZ = ZoneInfo('Asia/Kuala_Lumpur')


def clean_error(e: Exception) -> str:
    return str(e).split('Call log:')[0].strip()


class PaymentReconciliationPipeline:
    
    def __init__(self):
        setup_logger()
        self.settings = load_settings()
        self.accounts = load_accounts()
        self.today = datetime.now(KL_TZ).strftime("%Y-%m-%d")
        
        logger.info("Payment Reconciliation Automation Pipeline")
        logger.info(f"Date: {self.today} (Kuala Lumpur Time)")
        logger.info(f"Accounts configured: {len(self.accounts)}")
    
    async def download_all_data(self) -> dict:
        logger.info("Starting sequential download from all accounts")
        
        stats = {
            'total_accounts': len(self.accounts),
            'successful': 0,
            'failed': 0,
            'failed_accounts': []
        }
        
        async with BrowserManager() as browser_manager:
            for account in self.accounts:
                label = account['label']
                platform = account['platform']
                
                logger.info(f"Processing account: {label} ({platform})")
                
                try:
                    scraper_class = get_scraper_class(platform)
                    scraper = scraper_class(account)
                    
                    downloaded_files = await scraper.download_data(browser_manager)
                    
                    logger.info(f"Success: {label} - {len(downloaded_files)} files downloaded")
                    stats['successful'] += 1
                    
                except Exception as e:
                    error_msg = clean_error(e)
                    logger.error(f"Failed: {label} - {error_msg}")
                    stats['failed'] += 1
                    stats['failed_accounts'].append(label)
                    
                    logger.error(f"Stopping pipeline due to error in {label}")
                    raise RuntimeError(f"Download failed for {label}: {error_msg}")
        
        logger.info("Download Summary:")
        logger.info(f"Total accounts: {stats['total_accounts']}")
        logger.info(f"Successful: {stats['successful']}")
        logger.info(f"Failed: {stats['failed']}")
        if stats['failed_accounts']:
            logger.info(f"Failed accounts: {', '.join(stats['failed_accounts'])}")
        
        return stats
    
    def process_downloaded_data(self) -> dict:
        logger.info("Processing downloaded data")
        
        data_base = PROJECT_ROOT / self.settings['download']['base_path']
        
        logger.info(f"Processing KIRA data for {self.today}")
        kira_path = data_base / 'kira'
        kira_df = process_kira_folder(kira_path)
        
        logger.info(f"Processing PG data for {self.today}")
        pg_path = data_base / 'pg'
        pg_df = process_pg_folder(pg_path)
        
        logger.info(f"Processing Bank data for {self.today}")
        bank_path = data_base / 'bank'
        bank_df = process_bank_folder(bank_path)
        
        logger.info("Merging KIRA, PG, and Bank data")
        merged_df, merge_stats = merge_data(kira_df, pg_df, bank_df)
        
        holidays = load_malaysia_holidays()
        
        return {
            'merged_data': merged_df,
            'merge_stats': merge_stats,
            'holidays': holidays,
            'kira_count': len(kira_df),
            'pg_count': len(pg_df),
            'bank_count': len(bank_df)
        }
    
    def upload_to_sheets(self, processed_data: dict):
        logger.info("Uploading data to Google Sheets")
        
        merged_df = processed_data['merged_data']
        
        sheets_client = SheetsClient()
        
        logger.info("Uploading to Summary sheet")
        summary_sheet = self.settings['google_sheets']['sheets']['summary']
        sheets_client.upload_dataframe(summary_sheet, merged_df, include_header=True, clear_first=False)
        
        logger.info("Uploading to Deposit sheet")
        deposit_sheet = self.settings['google_sheets']['sheets']['deposit']
        
        logger.info("Uploading to Merchant Ledger")
        merchant_ledger_sheet = self.settings['google_sheets']['sheets']['merchant_ledger']
        
        logger.info("Uploading to Agent Ledger")
        agent_ledger_sheet = self.settings['google_sheets']['sheets']['agent_ledger']
        
        logger.info("Upload completed successfully")
    
    async def run(self) -> dict:
        start_time = datetime.now(KL_TZ)
        logger.info(f"Pipeline started at: {get_kl_timestamp()}")
        
        try:
            download_stats = await self.download_all_data()
            processed_data = self.process_downloaded_data()
            self.upload_to_sheets(processed_data)
            
            end_time = datetime.now(KL_TZ)
            duration = (end_time - start_time).total_seconds()
            
            logger.info("Pipeline completed successfully")
            logger.info(f"Duration: {duration:.2f} seconds")
            logger.info(f"Completed at: {get_kl_timestamp()}")
            
            return {
                'status': 'success',
                'duration_seconds': duration,
                'download_stats': download_stats,
                'process_stats': {
                    'kira_records': processed_data['kira_count'],
                    'pg_records': processed_data['pg_count'],
                    'bank_records': processed_data['bank_count'],
                    'merged_records': len(processed_data['merged_data'])
                },
                'merge_stats': processed_data['merge_stats']
            }
            
        except Exception as e:
            end_time = datetime.now(KL_TZ)
            duration = (end_time - start_time).total_seconds()
            
            error_msg = clean_error(e)
            logger.error(f"Pipeline failed: {error_msg}")
            logger.error(f"Duration before failure: {duration:.2f} seconds")
            
            return {
                'status': 'error',
                'error': error_msg,
                'duration_seconds': duration
            }
