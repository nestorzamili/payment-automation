"""
Holiday processor - migrated from HolidayProcessor.gs
Loads Malaysia holidays and calculates settlement dates
"""
import requests
from datetime import datetime, timedelta
from typing import Set
from zoneinfo import ZoneInfo
import re

from src.core.logger import get_logger

logger = get_logger(__name__)

KL_TZ = ZoneInfo('Asia/Kuala_Lumpur')
MALAYSIA_HOLIDAYS_URL = "https://calendar.google.com/calendar/ical/en.malaysia%23holiday@group.v.calendar.google.com/public/basic.ics"


def load_malaysia_holidays() -> Set[str]:
    """
    Load Malaysia public holidays from Google Calendar iCal.
    
    Returns:
        Set of date strings in YYYY-MM-DD format
    """
    logger.info("Loading Malaysia public holidays")
    
    try:
        response = requests.get(MALAYSIA_HOLIDAYS_URL, timeout=30)
        response.raise_for_status()
        
        ical_content = response.text
        lines = ical_content.split('\n')
        
        holidays = set()
        current_date = None
        
        for line in lines:
            line = line.strip()
            
            # Parse DTSTART (date)
            if line.startswith('DTSTART'):
                match = re.search(r'(\d{4})(\d{2})(\d{2})', line)
                if match:
                    year, month, day = match.groups()
                    current_date = f"{year}-{month}-{day}"
            
            # Parse SUMMARY (holiday name)
            if line.startswith('SUMMARY:'):
                name = line.replace('SUMMARY:', '').strip()
                if current_date and name:
                    holidays.add(current_date)
                    logger.debug(f"Holiday: {current_date} - {name}")
                    current_date = None
        
        logger.info(f"Loaded {len(holidays)} Malaysia public holidays")
        return holidays
        
    except Exception as e:
        logger.error(f"Error loading Malaysia holidays: {e}")
        return set()


def is_weekend(date: datetime) -> bool:
    """Check if date is weekend (Saturday=5 or Sunday=6)."""
    return date.weekday() in [5, 6]


def is_holiday(date_str: str, holiday_set: Set[str]) -> bool:
    """Check if date is a public holiday."""
    return date_str in holiday_set


def format_date_string(date: datetime) -> str:
    """Format datetime to YYYY-MM-DD string."""
    return date.strftime('%Y-%m-%d')


def calculate_settlement_date(transaction_date_str: str, settlement_rule: str, holiday_set: Set[str]) -> str:
    """
    Calculate settlement date based on transaction date and settlement rule.
    
    Business logic:
    1. Parse settlement rule (e.g., "T+1", "T+2")
    2. Add business days (skip weekends and Malaysia public holidays)
    3. If settlement date falls on weekend/holiday, move to next business day
    
    Args:
        transaction_date_str: Transaction date in YYYY-MM-DD format
        settlement_rule: Settlement rule (e.g., "T+1", "T+2")
        holiday_set: Set of public holiday dates in YYYY-MM-DD format
        
    Returns:
        Settlement date in YYYY-MM-DD format
    """
    if not transaction_date_str or not settlement_rule:
        return ''
    
    # Parse settlement rule (T+N)
    match = re.match(r'T\+(\d+)', settlement_rule, re.IGNORECASE)
    if not match:
        logger.warning(f"Invalid settlement rule: {settlement_rule}")
        return ''
    
    days_to_add = int(match.group(1))
    
    # Parse transaction date
    try:
        date_parts = transaction_date_str.split('-')
        if len(date_parts) != 3:
            logger.warning(f"Invalid transaction date format: {transaction_date_str}")
            return ''
        
        year, month, day = map(int, date_parts)
        current_date = datetime(year, month, day, tzinfo=KL_TZ)
    except Exception as e:
        logger.warning(f"Failed to parse transaction date '{transaction_date_str}': {e}")
        return ''
    
    # Add business days
    business_days_added = 0
    while business_days_added < days_to_add:
        current_date += timedelta(days=1)
        current_date_str = format_date_string(current_date)
        
        # Skip weekends and holidays
        if not is_weekend(current_date) and not is_holiday(current_date_str, holiday_set):
            business_days_added += 1
    
    # If settlement date is weekend or holiday, move to next business day
    settlement_date_str = format_date_string(current_date)
    while is_weekend(current_date) or is_holiday(settlement_date_str, holiday_set):
        current_date += timedelta(days=1)
        settlement_date_str = format_date_string(current_date)
    
    logger.debug(f"Settlement: {transaction_date_str} + {settlement_rule} = {settlement_date_str}")
    return settlement_date_str
