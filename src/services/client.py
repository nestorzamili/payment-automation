import gspread
from gspread.auth import authorize
from google.oauth2.service_account import Credentials
import pandas as pd
from typing import List, Any

from src.core.logger import get_logger
from src.core.loader import get_service_account_path, get_spreadsheet_id, load_settings

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
    
    def clear_sheet(self, sheet_name: str):
        try:
            worksheet = self.spreadsheet.worksheet(sheet_name)
            worksheet.clear()
            logger.info(f"Cleared sheet: {sheet_name}")
        except Exception as e:
            logger.error(f"Failed to clear sheet {sheet_name}: {e}")
            raise
    
    def write_data(self, sheet_name: str, data: List[List[Any]], start_cell: str = 'A1'):
        try:
            worksheet = self.spreadsheet.worksheet(sheet_name)
            worksheet.update(start_cell, data)
        except Exception as e:
            logger.error(f"Failed to write to {sheet_name}: {e}")
            raise
    
    def append_data(self, sheet_name: str, data: List[List[Any]]):
        try:
            worksheet = self.spreadsheet.worksheet(sheet_name)
            worksheet.append_rows(data)
            logger.info(f"Appended {len(data)} rows to {sheet_name}")
        except Exception as e:
            logger.error(f"Failed to append to {sheet_name}: {e}")
            raise
    
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
    
    def upload_dataframe(self, sheet_name: str, df: pd.DataFrame, include_header: bool = True, clear_first: bool = False):
        
        data = []
        if include_header:
            data.append(df.columns.tolist())
        
        for _, row in df.iterrows():
            data.append(['' if pd.isna(val) else val for val in row.values])
        
        if clear_first and len(df.columns) > 0:
            end_col = self._col_to_letter(len(df.columns))
            self.clear_columns(sheet_name, 'A', end_col)
        
        self.write_data(sheet_name, data)
        logger.info(f"Uploaded {len(df)} rows to {sheet_name}")
    
    def clear_columns(self, sheet_name: str, start_col: str, end_col: str):
        try:
            worksheet = self.spreadsheet.worksheet(sheet_name)
            range_to_clear = f"{start_col}:{end_col}"
            worksheet.batch_clear([range_to_clear])
        except Exception as e:
            logger.error(f"Failed to clear columns in {sheet_name}: {e}")
            raise
    
    def _col_to_letter(self, col: int) -> str:
        result = ""
        while col > 0:
            col, remainder = divmod(col - 1, 26)
            result = chr(65 + remainder) + result
        return result
    
    def set_dropdown(self, sheet_name: str, cell: str, values: List[str]):
        try:
            worksheet = self.spreadsheet.worksheet(sheet_name)
            
            col_letter = ''.join(filter(str.isalpha, cell))
            row_num = int(''.join(filter(str.isdigit, cell)))
            col_num = sum((ord(c.upper()) - ord('A') + 1) * (26 ** i) 
                         for i, c in enumerate(reversed(col_letter)))
            
            sheet_id = worksheet.id
            
            request = {
                "setDataValidation": {
                    "range": {
                        "sheetId": sheet_id,
                        "startRowIndex": row_num - 1,
                        "endRowIndex": row_num,
                        "startColumnIndex": col_num - 1,
                        "endColumnIndex": col_num
                    },
                    "rule": {
                        "condition": {
                            "type": "ONE_OF_LIST",
                            "values": [{"userEnteredValue": v} for v in values]
                        },
                        "showCustomUi": True,
                        "strict": True
                    }
                }
            }
            
            self.spreadsheet.batch_update({"requests": [request]})
            logger.info(f"Set dropdown in {sheet_name}!{cell} with {len(values)} values")
        except Exception as e:
            logger.error(f"Failed to set dropdown in {sheet_name}!{cell}: {e}")
    
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


