import asyncio
from datetime import datetime
from pathlib import Path
from typing import List, Optional
from zoneinfo import ZoneInfo

from playwright.async_api import Page

from src.core.loader import load_settings
from src.core.logger import get_logger
from src.scrapers.base import BaseScraper

logger = get_logger(__name__)


class KiraScraper(BaseScraper):
    
    LOGIN_PATH = "/mms/login"
    TARGET_PATH = "/mms/home#!/transactions"
    EXPORT_PATH = "/mms_api/report_transactions/get_report_list"
    REPORT_LIST_PATH = "/mms_api/report_export_history/get_report_list"
    DOWNLOAD_PATH = "/mms_api/report_export_history/download_report"
    
    def __init__(self, account: dict):
        super().__init__(account)
        self.settings = load_settings()
        self.tz = ZoneInfo(self.settings['timezone'])
        self.tz_offset = self.settings['timezone_offset']
    
    async def check_if_logged_in(self, page: Page) -> bool:
        try:
            logout_btn = page.locator('text=Logout, .logout-btn, #logout')
            return await logout_btn.is_visible(timeout=3000)
        except Exception:
            return False
    
    async def fill_login_credentials(self, page: Page):
        await page.fill('#loginID', self.credentials['username'])
        await page.fill('#loginPassword', self.credentials['password'])
    
    async def submit_login(self, page: Page):
        await page.click('.login-button button')
    
    async def wait_for_login_success(self, page: Page):
        await page.wait_for_url('**/home**', timeout=30000)
    
    async def download_files(self, page: Page, download_dir: Path, date_str: str) -> List[Path]:
        logger.info(f"Downloading KIRA: {date_str}")
        download_dir.mkdir(parents=True, exist_ok=True)
        
        success = await self._create_export(page, date_str)
        if not success:
            logger.error("Failed to create export")
            return []
        
        await asyncio.sleep(2)
        
        report_id = await self._get_latest_report_id(page, date_str)
        if not report_id:
            logger.error("Failed to get report ID")
            return []
        
        file_path = await self._download_report(page, download_dir, report_id, date_str)
        if not file_path:
            return []
        
        return [file_path]
    
    async def _create_export(self, page: Page, date_str: str) -> bool:
        url = self.base_url + self.EXPORT_PATH
        start_date, end_date = self._get_date_range(date_str)
        form_data = self._build_export_form_data(start_date, end_date)
        
        response = await page.request.post(
            url,
            form=form_data,
            headers={'Content-Type': 'application/x-www-form-urlencoded'}
        )
        
        if response.status != 200:
            logger.error(f"Export API failed: {response.status}")
            return False
        
        try:
            data = await response.json()
            if data.get('response_code') == '00':
                logger.info("Export created")
                return True
            else:
                logger.error(f"Export failed: {data.get('response_message')}")
                return False
        except Exception as e:
            logger.error(f"Failed to parse export response: {e}")
            return False
    
    async def _get_latest_report_id(self, page: Page, date_str: str) -> Optional[int]:
        url = self.base_url + self.REPORT_LIST_PATH
        start_date, end_date = self._get_date_range(date_str)
        form_data = self._build_report_list_form_data(start_date, end_date)
        
        response = await page.request.post(
            url,
            form=form_data,
            headers={'Content-Type': 'application/x-www-form-urlencoded'}
        )
        
        if response.status != 200:
            logger.error(f"Report list API failed: {response.status}")
            return None
        
        try:
            data = await response.json()
            reports = data.get('data', [])
            
            if not reports:
                logger.error("No reports found")
                return None
            
            completed_reports = [r for r in reports if r.get('status_id') == 3]
            if not completed_reports:
                logger.error("No completed reports found")
                return None
            
            latest_report = max(completed_reports, key=lambda r: r.get('id', 0))
            report_id = latest_report.get('id')
            
            logger.info(f"Found report: id={report_id}")
            return report_id
            
        except Exception as e:
            logger.error(f"Failed to parse report list: {e}")
            return None
    
    async def _download_report(self, page: Page, download_dir: Path, report_id: int, date_str: str) -> Optional[Path]:
        url = self.base_url + self.DOWNLOAD_PATH
        
        response = await page.request.post(
            url,
            data={'reportID': report_id},
            headers={'Content-Type': 'application/json'}
        )
        
        if response.status != 200:
            logger.error(f"Download API failed: {response.status}")
            return None
        
        content_disposition = response.headers.get('content-disposition', '')
        
        filename = None
        if 'filename=' in content_disposition:
            filename = content_disposition.split('filename=')[1].strip('"\'')
        
        if not filename:
            filename = f"kira_transactions_{date_str}.xlsx"
        
        file_path = download_dir / filename
        
        content = await response.body()
        with open(file_path, 'wb') as f:
            f.write(content)
        
        logger.info(f"Downloaded: {filename}")
        return file_path
    
    def _get_date_range(self, date_str: str) -> tuple:
        dt = datetime.strptime(date_str, '%Y-%m-%d')
        start = dt.replace(hour=0, minute=0, second=0)
        end = dt.replace(hour=23, minute=59, second=59)
        
        start_str = start.strftime(f'%Y-%m-%dT%H:%M:%S{self.tz_offset}')
        end_str = end.strftime(f'%Y-%m-%dT%H:%M:%S{self.tz_offset}')
        
        return start_str, end_str
    
    def _build_export_form_data(self, start_date: str, end_date: str) -> dict:
        columns = [
            {'data': '', 'name': 'No.', 'searchable': 'false', 'orderable': 'false'},
            {'data': 'created_time', 'name': 'Created On', 'searchable': 'true', 'orderable': 'true'},
            {'data': 'payment_gateway_transaction_time', 'name': 'Paid On', 'searchable': 'true', 'orderable': 'true'},
            {'data': 'merchant_name', 'name': 'Merchant', 'searchable': 'true', 'orderable': 'true'},
            {'data': 'transaction_id', 'name': 'Transaction ID', 'searchable': 'true', 'orderable': 'true'},
            {'data': 'merchant_transaction_id', 'name': 'Merchant Order ID', 'searchable': 'true', 'orderable': 'true'},
            {'data': 'payment_gateway_transaction_id', 'name': 'Ext Transaction ID', 'searchable': 'true', 'orderable': 'false'},
            {'data': 'method_name', 'name': 'Payment Method', 'searchable': 'true', 'orderable': 'true'},
            {'data': 'currency_code', 'name': 'Currency', 'searchable': 'true', 'orderable': 'true'},
            {'data': 'transaction_amount', 'name': 'Transaction Amount', 'searchable': 'true', 'orderable': 'true'},
            {'data': 'mdr_percentage', 'name': 'MDR(%)', 'searchable': 'true', 'orderable': 'false'},
            {'data': 'mdr_percentage_amount', 'name': 'MDR', 'searchable': 'true', 'orderable': 'false'},
            {'data': 'mdr_amount', 'name': 'Fee', 'searchable': 'true', 'orderable': 'false'},
            {'data': 'actual_amount', 'name': 'Actual Amount', 'searchable': 'true', 'orderable': 'false'},
            {'data': 'status_name', 'name': 'Transaction Status', 'searchable': 'true', 'orderable': 'true'},
            {'data': 'customer_name', 'name': 'Customer Name', 'searchable': 'true', 'orderable': 'true'},
            {'data': 'customer_email', 'name': 'Customer Email', 'searchable': 'true', 'orderable': 'true'},
            {'data': 'customer_phone', 'name': 'Customer Phone', 'searchable': 'true', 'orderable': 'true'},
        ]
        
        form_data = {
            'merchantIDs[]': '',
            'currencyIDs[]': '',
            'paymentMethodIDs[]': '',
            'transactionStatusIDs[]': '3',
            'startDate': start_date,
            'endDate': end_date,
            'isExport': 'true',
            'exportType': 'excel',
            'exportGlobalSearch': '',
            'order[0][column]': '1',
            'order[0][dir]': 'desc',
        }
        
        for i, col in enumerate(columns):
            form_data[f'columns[{i}][isExportVisible]'] = 'true'
            form_data[f'columns[{i}][data]'] = col['data']
            form_data[f'columns[{i}][name]'] = col['name']
            form_data[f'columns[{i}][searchable]'] = col['searchable']
            form_data[f'columns[{i}][orderable]'] = col['orderable']
        
        return form_data
    
    def _build_report_list_form_data(self, start_date: str, end_date: str) -> dict:
        columns = [
            {'data': '', 'searchable': 'false', 'orderable': 'false'},
            {'data': 'id', 'searchable': 'true', 'orderable': 'true'},
            {'data': 'username', 'searchable': 'true', 'orderable': 'true'},
            {'data': 'merchant_name', 'searchable': 'true', 'orderable': 'false'},
            {'data': 'module_name', 'searchable': 'true', 'orderable': 'false'},
            {'data': 'report_name', 'searchable': 'true', 'orderable': 'true'},
            {'data': 'start_date', 'searchable': 'true', 'orderable': 'true'},
            {'data': 'end_date', 'searchable': 'true', 'orderable': 'true'},
            {'data': 'version', 'searchable': 'true', 'orderable': 'false'},
            {'data': 'status_name', 'searchable': 'true', 'orderable': 'false'},
            {'data': 'created_time', 'searchable': 'true', 'orderable': 'true'},
            {'data': '', 'searchable': 'false', 'orderable': 'false'},
        ]
        
        form_data = {
            'draw': '1',
            'start': '0',
            'length': '10',
            'search[value]': '',
            'search[regex]': 'false',
            'merchantIDs': '0',
            'gameTypeID': '0',
            'gameIDs': '0',
            'startDate': start_date,
            'endDate': end_date,
            'order[0][column]': '1',
            'order[0][dir]': 'desc',
        }
        
        for i, col in enumerate(columns):
            form_data[f'columns[{i}][data]'] = col['data']
            form_data[f'columns[{i}][name]'] = ''
            form_data[f'columns[{i}][searchable]'] = col['searchable']
            form_data[f'columns[{i}][orderable]'] = col['orderable']
            form_data[f'columns[{i}][search][value]'] = ''
            form_data[f'columns[{i}][search][regex]'] = 'false'
        
        return form_data
