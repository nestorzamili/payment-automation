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
        self._add_on_holidays: Optional[Set[str]] = None
        self._loaded = False
    
    def load_all_parameters(self) -> Dict[str, Any]:
        try:
            data = self.client.read_data(self.sheet_name)
            
            if not data:
                logger.warning("No parameter data found")
                return {}
            
            sections = self._parse_sections(data)
            
            if 'SETTLEMENT_RULES' in sections:
                self._settlement_rules = self._parse_settlement_rules(sections['SETTLEMENT_RULES'])
            
            if 'ADD_ON_HOLIDAYS' in sections:
                self._add_on_holidays = self._parse_add_on_holidays(sections['ADD_ON_HOLIDAYS'])
            
            self._loaded = True
            rules_count = len(self._settlement_rules) if self._settlement_rules is not None else 0
            logger.info(f"Loaded parameters: {rules_count} settlement rules")
            
            return {
                'settlement_rules': self._settlement_rules,
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
    
    def get_add_on_holidays(self) -> Set[str]:
        self._ensure_loaded()
        
        if self._add_on_holidays is None:
            return set()
        
        return self._add_on_holidays
