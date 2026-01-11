import re


MONTHS = {
    'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
    'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
}


def categorize_channel(channel: str) -> str:
    if not channel:
        return 'EWALLET'
    ch_upper = channel.upper().strip()
    if ch_upper in ('FPX', 'FPXC') or 'FPX' in ch_upper:
        return 'FPX'
    return 'EWALLET'


def round_decimal(value: float) -> float:
    return round(value, 2) if value is not None else None


def to_float(value):
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        value = value.strip()
        if not value:
            return None
        if value.startswith("'"):
            value = value[1:]
        value = value.replace(',', '')
        try:
            return float(value)
        except ValueError:
            return None
    return None


def safe_get_value(row: list, idx: int):
    if idx < len(row):
        val = row[idx]
        if val is not None and str(val).strip() != '':
            return val
    return None


def parse_period(period_str: str) -> tuple:
    if not period_str:
        return None, None
    
    match = re.match(r'(\w+)\s+(\d{4})', str(period_str))
    if not match:
        return None, None
    
    month_name = match.group(1)
    year = int(match.group(2))
    month = MONTHS.get(month_name)
    
    return year, month


def calculate_fee(fee_type: str, fee_rate: float, amount: float, volume: int) -> float:
    if not fee_type or fee_rate is None:
        return 0
    
    if fee_type == 'percentage':
        return round(amount * (fee_rate / 100), 2)
    elif fee_type == 'per_volume':
        return round(volume * fee_rate, 2)
    elif fee_type == 'flat':
        return round(fee_rate, 2)
    
    return 0
