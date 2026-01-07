import gspread
from gspread.auth import authorize
from google.oauth2.service_account import Credentials
from typing import List, Any

from src.core.logger import get_logger
from src.core.loader import get_service_account_path, get_spreadsheet_id, load_settings
from src.utils.retry import exponential_backoff

logger = get_logger(__name__)

SCOPES = [
    'https://www.googleapis.com/auth/spreadsheets',
    'https://www.googleapis.com/auth/drive'
]


class SheetsClient:
    
    def __init__(self):
        self.settings = load_settings()
        self.spreadsheet_id = get_spreadsheet_id()
        self.service_account_path = get_service_account_path()
        
        credentials = Credentials.from_service_account_file(
            str(self.service_account_path),
            scopes=SCOPES
        )
        
        self.client = authorize(credentials)
        self.spreadsheet = self.client.open_by_key(self.spreadsheet_id)
    
    @exponential_backoff()
    def write_data(self, sheet_name: str, data: List[List[Any]], start_cell: str = 'A1'):
        try:
            worksheet = self.spreadsheet.worksheet(sheet_name)
            worksheet.update(start_cell, data)
        except Exception as e:
            logger.error(f"Failed to write to {sheet_name}: {e}")
            raise

    @exponential_backoff()
    def read_data(self, sheet_name: str, range_spec: str = '') -> List[List[Any]]:
        try:
            worksheet = self.spreadsheet.worksheet(sheet_name)
            if range_spec:
                values = worksheet.get(range_spec)
            else:
                values = worksheet.get_all_values()
            return values
        except Exception as e:
            logger.error(f"Failed to read from {sheet_name}: {e}")
            raise
    
    @exponential_backoff()
    def set_dropdown_range(self, sheet_name: str, col: str, start_row: int, end_row: int, values: List[str]):
        try:
            worksheet = self.spreadsheet.worksheet(sheet_name)
            
            col_num = sum((ord(c.upper()) - ord('A') + 1) * (26 ** i) 
                         for i, c in enumerate(reversed(col)))
            
            sheet_id = worksheet.id
            
            request = {
                "setDataValidation": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": start_row - 1,
                        "endRowIndex": end_row,
                        "startColumnIndex": col_num - 1,
                        "endColumnIndex": col_num
                    },
                    "rule": {
                        "condition": {
                            "type": "ONE_OF_LIST",
                            "values": [{"userEnteredValue": v} for v in values]
                        },
                        "showCustomUi": True,
                        "strict": False
                    }
                }
            }
            
            self.spreadsheet.batch_update({"requests": [request]})
        except Exception as e:
            logger.error(f"Failed to set dropdown range in {sheet_name}!{col}: {e}")

    @exponential_backoff()
    def clear_data_validation(self, sheet_name: str, range_spec: str):
        try:
            worksheet = self.spreadsheet.worksheet(sheet_name)
            sheet_id = worksheet.id

            start_cell, end_cell = range_spec.split(':')

            start_col = ''.join(filter(str.isalpha, start_cell))
            start_row = int(''.join(filter(str.isdigit, start_cell)))
            end_col = ''.join(filter(str.isalpha, end_cell))
            end_row = int(''.join(filter(str.isdigit, end_cell)))

            start_col_num = sum((ord(c.upper()) - ord('A') + 1) * (26 ** i)
                               for i, c in enumerate(reversed(start_col)))
            end_col_num = sum((ord(c.upper()) - ord('A') + 1) * (26 ** i)
                             for i, c in enumerate(reversed(end_col)))

            request = {
                "setDataValidation": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": start_row - 1,
                        "endRowIndex": end_row,
                        "startColumnIndex": start_col_num - 1,
                        "endColumnIndex": end_col_num
                    },
                    "rule": None
                }
            }

            self.spreadsheet.batch_update({"requests": [request]})
        except Exception as e:
            logger.error(f"Failed to clear data validation in {sheet_name}!{range_spec}: {e}")
    
    @exponential_backoff()
    def set_row_background(self, sheet_name: str, row: int, start_col: int, end_col: int, 
                           red: float = 0.9, green: float = 0.9, blue: float = 0.9):
        try:
            worksheet = self.spreadsheet.worksheet(sheet_name)
            sheet_id = worksheet.id
            
            request = {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": row - 1,
                        "endRowIndex": row,
                        "startColumnIndex": start_col - 1,
                        "endColumnIndex": end_col
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": {
                                "red": red,
                                "green": green,
                                "blue": blue
                            }
                        }
                    },
                    "fields": "userEnteredFormat.backgroundColor"
                }
            }
            
            self.spreadsheet.batch_update({"requests": [request]})
        except Exception as e:
            logger.error(f"Failed to set row background in {sheet_name}: {e}")

    @exponential_backoff()
    def clear_row_backgrounds(self, sheet_name: str, start_row: int, end_row: int, start_col: int, end_col: int):
        try:
            worksheet = self.spreadsheet.worksheet(sheet_name)
            sheet_id = worksheet.id

            request = {
                "repeatCell": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": start_row - 1,
                        "endRowIndex": end_row,
                        "startColumnIndex": start_col - 1,
                        "endColumnIndex": end_col
                    },
                    "cell": {
                        "userEnteredFormat": {
                            "backgroundColor": {
                                "red": 1.0,
                                "green": 1.0,
                                "blue": 1.0
                            }
                        }
                    },
                    "fields": "userEnteredFormat.backgroundColor"
                }
            }

            self.spreadsheet.batch_update({"requests": [request]})
        except Exception as e:
            logger.error(f"Failed to clear row backgrounds in {sheet_name}: {e}")


