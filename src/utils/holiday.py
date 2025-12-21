import re
import requests
from datetime import datetime, timedelta
from typing import Set
from zoneinfo import ZoneInfo

from src.core.logger import get_logger

logger = get_logger(__name__)

KL_TZ = ZoneInfo('Asia/Kuala_Lumpur')
MALAYSIA_HOLIDAYS_URL = "https://calendar.google.com/calendar/ical/en.malaysia%23holiday@group.v.calendar.google.com/public/basic.ics"


def load_malaysia_holidays() -> Set[str]:
    logger.info("Loading Malaysia public holidays")
    
    try:
        response = requests.get(MALAYSIA_HOLIDAYS_URL, timeout=30)
        response.raise_for_status()
        
        lines = response.text.split('\n')
        holidays = set()
        current_date = None
        
        for line in lines:
            line = line.strip()
            
            if line.startswith('DTSTART'):
                match = re.search(r'(\d{4})(\d{2})(\d{2})', line)
                if match:
                    year, month, day = match.groups()
                    current_date = f"{year}-{month}-{day}"
            
            if line.startswith('SUMMARY:') and current_date:
                holidays.add(current_date)
                current_date = None
        
        logger.info(f"Loaded {len(holidays)} holidays")
        return holidays
        
    except Exception as e:
        logger.error(f"Error loading holidays: {e}")
        return set()


def is_weekend(date: datetime) -> bool:
    return date.weekday() in [5, 6]


def is_holiday(date_str: str, holiday_set: Set[str]) -> bool:
    return date_str in holiday_set


def format_date_string(date: datetime) -> str:
    return date.strftime('%Y-%m-%d')


def calculate_settlement_date(
    transaction_date_str: str, 
    settlement_rule: str, 
    holiday_set: Set[str],
    add_on_holidays: Set[str] = None
) -> str:
    if not transaction_date_str or not settlement_rule:
        return ''
    
    match = re.match(r'T\+(\d+)', settlement_rule, re.IGNORECASE)
    if not match:
        return ''
    
    days_to_add = int(match.group(1))
    
    try:
        date_parts = transaction_date_str.split('-')
        if len(date_parts) != 3:
            return ''
        
        year, month, day = map(int, date_parts)
        current_date = datetime(year, month, day, tzinfo=KL_TZ)
    except Exception:
        return ''
    
    all_holidays = set(holiday_set) if holiday_set else set()
    if add_on_holidays:
        all_holidays = all_holidays.union(add_on_holidays)
    
    business_days_added = 0
    while business_days_added < days_to_add:
        current_date += timedelta(days=1)
        current_date_str = format_date_string(current_date)
        
        if not is_weekend(current_date) and not is_holiday(current_date_str, all_holidays):
            business_days_added += 1
    
    settlement_date_str = format_date_string(current_date)
    while is_weekend(current_date) or is_holiday(settlement_date_str, all_holidays):
        current_date += timedelta(days=1)
        settlement_date_str = format_date_string(current_date)
    
    return settlement_date_str
