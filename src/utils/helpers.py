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
        try:
            return float(value)
        except ValueError:
            return None
    return None


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
