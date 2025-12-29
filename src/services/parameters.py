from typing import Dict, List, Set, Any

from sqlalchemy import and_

from src.core.database import get_session
from src.core.models import Parameter
from src.core.logger import get_logger

logger = get_logger(__name__)


class ParameterService:
    
    def __init__(self):
        pass
    
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
        session = get_session()
        
        try:
            params = session.query(Parameter).filter(
                Parameter.type == 'SETTLEMENT_RULES'
            ).all()
            
            return {p.key.lower(): p.value for p in params}
            
        finally:
            session.close()
    
    def get_add_on_holidays(self) -> Set[str]:
        session = get_session()
        
        try:
            params = session.query(Parameter).filter(
                Parameter.type == 'ADD_ON_HOLIDAYS'
            ).all()
            
            return {p.key for p in params}
            
        finally:
            session.close()
    
    def save_parameters(self, data: Dict[str, Any]) -> int:
        session = get_session()
        count = 0
        
        try:
            if 'settlement_rules' in data:
                for key, value in data['settlement_rules'].items():
                    existing = session.query(Parameter).filter(
                        and_(
                            Parameter.type == 'SETTLEMENT_RULES',
                            Parameter.key == key
                        )
                    ).first()
                    
                    if existing:
                        existing.value = value
                    else:
                        session.add(Parameter(
                            type='SETTLEMENT_RULES',
                            key=key,
                            value=value
                        ))
                    count += 1
            
            if 'add_on_holidays' in data:
                existing_holidays = session.query(Parameter).filter(
                    Parameter.type == 'ADD_ON_HOLIDAYS'
                ).all()
                existing_dates = {h.key for h in existing_holidays}
                
                new_dates = set()
                for holiday in data['add_on_holidays']:
                    date = holiday.get('date')
                    description = holiday.get('description', '')
                    
                    if not date:
                        continue
                    
                    new_dates.add(date)
                    
                    existing = session.query(Parameter).filter(
                        and_(
                            Parameter.type == 'ADD_ON_HOLIDAYS',
                            Parameter.key == date
                        )
                    ).first()
                    
                    if existing:
                        existing.description = description
                    else:
                        session.add(Parameter(
                            type='ADD_ON_HOLIDAYS',
                            key=date,
                            description=description
                        ))
                    count += 1
                
                for date in existing_dates - new_dates:
                    session.query(Parameter).filter(
                        and_(
                            Parameter.type == 'ADD_ON_HOLIDAYS',
                            Parameter.key == date
                        )
                    ).delete()
            
            session.commit()
            logger.info(f"Saved {count} parameters")
            return count
            
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to save parameters: {e}")
            raise
        finally:
            session.close()
