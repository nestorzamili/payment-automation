import pandas as pd
from typing import Dict, List, Set, Any, Optional

from src.sheets.client import SheetsClient
from src.core.logger import get_logger
from src.core.loader import load_settings

logger = get_logger(__name__)


class ParameterLoader:
    
    def __init__(self, sheets_client: SheetsClient):
        self.client = sheets_client
        self.settings = load_settings()
        self.sheet_name = self.settings['google_sheets']['sheets']['parameters']
        
        self._settlement_rules: Optional[Dict[str, str]] = None
        self._fees: Optional[pd.DataFrame] = None
        self._deposit_rules: Optional[pd.DataFrame] = None
        self._add_on_holidays: Optional[Set[str]] = None
        self._loaded = False
    
    def load_all_parameters(self) -> Dict[str, Any]:
        logger.info(f"Loading parameters from sheet: {self.sheet_name}")
        
        try:
            data = self.client.read_data(self.sheet_name)
            
            if not data:
                logger.warning("No parameter data found")
                return {}
            
            sections = self._parse_sections(data)
            
            if 'SETTLEMENT_RULES' in sections:
                self._settlement_rules = self._parse_settlement_rules(sections['SETTLEMENT_RULES'])
                logger.info(f"Loaded {len(self._settlement_rules)} settlement rules")
            
            if 'FEES' in sections:
                self._fees = self._parse_fees(sections['FEES'])
                logger.info(f"Loaded {len(self._fees)} fee configurations")
            
            if 'ADD_ON_HOLIDAYS' in sections:
                self._add_on_holidays = self._parse_add_on_holidays(sections['ADD_ON_HOLIDAYS'])
                logger.info(f"Loaded {len(self._add_on_holidays)} add-on holidays")
            
            if 'DEPOSIT_RULES' in sections:
                self._deposit_rules = self._parse_deposit_rules(sections['DEPOSIT_RULES'])
                logger.info(f"Loaded {len(self._deposit_rules)} deposit rules")
            
            self._loaded = True
            
            return {
                'settlement_rules': self._settlement_rules,
                'fees': self._fees,
                'deposit_rules': self._deposit_rules,
                'add_on_holidays': self._add_on_holidays
            }
            
        except Exception as e:
            logger.error(f"Failed to load parameters: {e}")
            raise
    
    def _parse_sections(self, data: List[List[str]]) -> Dict[str, List[List[str]]]:
        sections = {}
        current_section = None
        current_data = []
        
        for row in data:
            if not row or all(cell.strip() == '' for cell in row):
                if current_section and current_data:
                    sections[current_section] = current_data
                current_section = None
                current_data = []
                continue
            
            first_cell = row[0].strip() if row else ''
            if first_cell.startswith('SECTION:'):
                section_name = first_cell.replace('SECTION:', '').strip()
                current_section = section_name
                current_data = []
                continue
            
            if current_section:
                current_data.append(row)
        
        if current_section and current_data:
            sections[current_section] = current_data
        
        return sections
    
    def _parse_settlement_rules(self, data: List[List[str]]) -> Dict[str, str]:
        rules = {}
        if not data:
            return rules
        
        for row in data:
            if len(row) >= 2:
                channel = row[0].strip()
                rule = row[1].strip()
                if channel and rule:
                    rules[channel.lower()] = rule
        
        return rules
    
    def _parse_fees(self, data: List[List[str]]) -> pd.DataFrame:
        if not data:
            return pd.DataFrame()
        
        header = data[0]
        rows = data[1:]
        df = pd.DataFrame(rows, columns=header)
        
        if 'Year' in df.columns:
            df['Year'] = pd.to_numeric(df['Year'], errors='coerce')
        if 'Month' in df.columns:
            df['Month'] = pd.to_numeric(df['Month'], errors='coerce')
        if 'Fee_Value' in df.columns:
            df['Fee_Value'] = pd.to_numeric(df['Fee_Value'], errors='coerce')
        
        return df
    
    def _parse_add_on_holidays(self, data: List[List[str]]) -> Set[str]:
        holidays = set()
        
        if not data:
            return holidays
        
        for row in data[1:]:
            if row and row[0].strip():
                holidays.add(row[0].strip())
        
        return holidays
    
    def _ensure_loaded(self):
        if not self._loaded:
            self.load_all_parameters()
    
    def get_settlement_rule(self, channel: str) -> str:
        self._ensure_loaded()
        
        if not self._settlement_rules:
            logger.warning(f"No settlement rules loaded, using T+1 for {channel}")
            return 'T+1'
        
        channel_lower = channel.lower() if channel else ''
        
        if channel_lower in self._settlement_rules:
            return self._settlement_rules[channel_lower]
        
        logger.warning(f"No settlement rule for {channel}, using T+1")
        return 'T+1'
    
    def get_fee_config(self, year: int, month: int, pg: str, payment_type: str) -> Dict[str, Any]:
        self._ensure_loaded()
        
        default = {'fee_type': 'percent', 'fee_value': 0.0}
        
        if self._fees is None or self._fees.empty:
            logger.warning(f"No fees loaded, using default for {year}/{month}/{pg}/{payment_type}")
            return default
        
        pg_lower = pg.lower() if pg else ''
        payment_type_lower = payment_type.lower() if payment_type else ''
        
        matching = self._fees[
            (self._fees['Year'] == year) &
            (self._fees['Month'] == month) &
            (self._fees['PG'].str.lower() == pg_lower) &
            (self._fees['Payment_Type'].str.lower() == payment_type_lower)
        ]
        
        if matching.empty:
            logger.warning(f"No fee config for {year}/{month}/{pg}/{payment_type}, using default")
            return default
        
        row = matching.iloc[0]
        return {
            'fee_type': str(row['Fee_Type']).strip(),
            'fee_value': float(row['Fee_Value']) if pd.notna(row['Fee_Value']) else 0.0
        }
    
    def get_add_on_holidays(self) -> Set[str]:
        self._ensure_loaded()
        
        if self._add_on_holidays is None:
            return set()
        
        return self._add_on_holidays
    
    def _parse_deposit_rules(self, data: List[List[str]]) -> pd.DataFrame:
        if not data:
            return pd.DataFrame()
        
        header = data[0]
        rows = data[1:]
        df = pd.DataFrame(rows, columns=header)
        
        if 'Year' in df.columns:
            df['Year'] = pd.to_numeric(df['Year'], errors='coerce')
        if 'Month' in df.columns:
            df['Month'] = pd.to_numeric(df['Month'], errors='coerce')
        
        for col in ['FPX', 'ewallet', 'FPXC', 'Withdrawal']:
            if col in df.columns:
                df[col] = df[col].astype(str).str.replace('%', '').str.strip()
                df[col] = pd.to_numeric(df[col], errors='coerce')
        
        return df
    
    def get_deposit_fee(self, year: int, month: int, merchant: str, channel: str) -> float:
        self._ensure_loaded()
        
        if self._deposit_rules is None or self._deposit_rules.empty:
            logger.warning(f"No deposit rules loaded for {year}/{month}/{merchant}/{channel}")
            return 0.0
        
        matching = self._deposit_rules[
            (self._deposit_rules['Year'] == year) &
            (self._deposit_rules['Month'] == month) &
            (self._deposit_rules['Merchant'] == merchant)
        ]
        
        if matching.empty:
            logger.warning(f"No deposit rule for {year}/{month}/{merchant}/{channel}")
            return 0.0
        
        row = matching.iloc[0]
        channel_col = channel if channel in ['FPX', 'FPXC'] else 'ewallet'
        
        if channel_col in row:
            return float(row[channel_col]) if pd.notna(row[channel_col]) else 0.0
        
        return 0.0
