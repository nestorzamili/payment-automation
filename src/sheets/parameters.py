import pandas as pd
from typing import Dict, Any

from src.sheets.client import SheetsClient
from src.core.logger import get_logger
from src.core.loader import load_settings

logger = get_logger(__name__)


class ParameterLoader:
    
    def __init__(self, sheets_client: SheetsClient):
        self.client = sheets_client
        self.settings = load_settings()
        self.sheet_name = self.settings['google_sheets']['sheets']['parameters']
        self.parameters = None
    
    def load_parameters(self) -> pd.DataFrame:
        logger.info(f"Loading parameters from sheet: {self.sheet_name}")
        
        try:
            data = self.client.read_data(self.sheet_name)
            
            if not data:
                logger.warning("No parameter data found")
                return pd.DataFrame()
            
            header = data[0]
            rows = data[1:]
            df = pd.DataFrame(rows, columns=header)
            
            logger.info(f"Loaded {len(df)} parameter rows")
            self.parameters = df
            return df
            
        except Exception as e:
            logger.error(f"Failed to load parameters: {e}")
            raise
    
    def get_settlement_rule(self, merchant: str, channel: str) -> str:
        if self.parameters is None:
            self.load_parameters()
        
        matching = self.parameters[
            (self.parameters['Merchant'] == merchant) & 
            (self.parameters['Channel'] == channel)
        ]
        
        if not matching.empty:
            return matching.iloc[0]['Settlement Rule']
        
        logger.warning(f"No settlement rule found for {merchant}/{channel}, using T+1")
        return 'T+1'
    
    def get_fee_rate(self, merchant: str, channel: str, month: str) -> float:
        if self.parameters is None:
            self.load_parameters()
        
        matching = self.parameters[
            (self.parameters['Merchant'] == merchant) & 
            (self.parameters['Channel'] == channel) &
            (self.parameters['Month'] == month)
        ]
        
        if not matching.empty:
            fee = matching.iloc[0]['Fee Rate']
            return float(fee)
        
        logger.warning(f"No fee rate found for {merchant}/{channel}/{month}, using 0")
        return 0.0
    
    def get_withdrawal_rate(self, merchant: str, month: str) -> float:
        if self.parameters is None:
            self.load_parameters()
        
        matching = self.parameters[
            (self.parameters['Merchant'] == merchant) &
            (self.parameters['Month'] == month)
        ]
        
        if not matching.empty:
            rate = matching.iloc[0]['Withdrawal Rate']
            return float(rate)
        
        return 0.0
