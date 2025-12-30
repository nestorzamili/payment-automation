from typing import Dict, Set, Any

from sqlalchemy import and_

from src.core.database import get_session
from src.core.models import Parameter
from src.core.logger import get_logger

logger = get_logger(__name__)


class ParameterService:
    _cache = None
    
    @classmethod
    def load_parameters(cls) -> tuple[Dict[str, str], Set[str]]:
        if cls._cache is None:
            cls._cache = cls._fetch_from_db()
        return cls._cache
    
    @classmethod
    def clear_cache(cls):
        cls._cache = None
    
    @staticmethod
    def _fetch_from_db() -> tuple[Dict[str, str], Set[str]]:
        session = get_session()
        try:
            params = session.query(Parameter).all()
            settlement_rules = {}
            add_on_holidays = set()
            for p in params:
                if p.type == 'SETTLEMENT_RULES':
                    settlement_rules[p.key.lower()] = p.value
                elif p.type == 'ADD_ON_HOLIDAYS':
                    add_on_holidays.add(p.key)
            return settlement_rules, add_on_holidays
        finally:
            session.close()
    
    @classmethod
    def sync_from_sheet(cls) -> int:
        from src.services.client import SheetsClient
        
        client = SheetsClient()
        session = get_session()
        
        try:
            data = client.read_data('Parameter')
            
            if not data:
                logger.info("No parameters found in sheet")
                return 0
            
            header_row_idx = None
            for idx, row in enumerate(data):
                if len(row) >= 3 and row[0] == 'ID' and row[1] == 'Type':
                    header_row_idx = idx
                    break
            
            if header_row_idx is None:
                logger.info("No valid header row found in Parameter sheet")
                return 0
            
            headers = data[header_row_idx]
            id_idx = 0
            type_idx = headers.index('Type') if 'Type' in headers else 1
            key_idx = headers.index('Key') if 'Key' in headers else 2
            value_idx = headers.index('Value') if 'Value' in headers else 3
            desc_idx = headers.index('Description') if 'Description' in headers else 4
            
            sheet_params = set()
            count = 0
            
            for row in data[header_row_idx + 1:]:
                if len(row) < 3:
                    continue
                
                param_type = str(row[type_idx]).strip() if len(row) > type_idx else ''
                param_key = str(row[key_idx]).strip().lower() if len(row) > key_idx else ''
                param_value = str(row[value_idx]).strip() if len(row) > value_idx else ''
                param_desc = str(row[desc_idx]).strip() if len(row) > desc_idx else ''
                
                if not param_type or not param_key or param_key == '-':
                    continue
                
                sheet_params.add((param_type, param_key))
                
                existing = session.query(Parameter).filter(
                    and_(
                        Parameter.type == param_type,
                        Parameter.key == param_key
                    )
                ).first()
                
                if existing:
                    existing.value = param_value
                    existing.description = param_desc
                else:
                    session.add(Parameter(
                        type=param_type,
                        key=param_key,
                        value=param_value,
                        description=param_desc
                    ))
                
                count += 1
            
            existing_all = session.query(Parameter).all()
            for p in existing_all:
                if (p.type, p.key) not in sheet_params:
                    session.delete(p)
                    logger.info(f"Deleted parameter: {p.type}/{p.key}")
            
            session.commit()
            cls.clear_cache()
            logger.info(f"Synced {count} parameters from sheet")
            return count
            
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to sync parameters: {e}")
            raise
        finally:
            session.close()
    
    def get_all_parameters(self) -> Dict[str, Any]:
        session = get_session()
        
        try:
            params = session.query(Parameter).all()
            
            settlement_rules = {}
            add_on_holidays = []
            
            for p in params:
                if p.type == 'SETTLEMENT_RULES':
                    settlement_rules[p.key] = p.value
                elif p.type == 'ADD_ON_HOLIDAYS':
                    add_on_holidays.append({
                        'date': p.key,
                        'description': p.description
                    })
            
            return {
                'settlement_rules': settlement_rules,
                'add_on_holidays': add_on_holidays
            }
            
        finally:
            session.close()
    
    def get_settlement_rules(self) -> Dict[str, str]:
        rules, _ = self.load_parameters()
        return rules
    
    def get_add_on_holidays(self) -> Set[str]:
        _, holidays = self.load_parameters()
        return holidays
