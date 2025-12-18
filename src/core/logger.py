import sys
from pathlib import Path
from loguru import logger
from datetime import datetime
from zoneinfo import ZoneInfo

PROJECT_ROOT = Path(__file__).parent.parent.parent
KL_TZ = ZoneInfo('Asia/Kuala_Lumpur')


def get_kl_timestamp():
    return datetime.now(KL_TZ).strftime("%Y-%m-%d %H:%M:%S")


def setup_logger():
    logger.remove()
    
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | <level>{message}</level>",
        level="INFO",
        colorize=True
    )
    
    log_dir = PROJECT_ROOT / 'logs'
    log_dir.mkdir(exist_ok=True)
    
    kl_date = datetime.now(KL_TZ).strftime("%Y-%m-%d")
    log_file = log_dir / f"{kl_date}.log"
    
    logger.add(
        log_file,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
        level="DEBUG",
        encoding="utf-8"
    )
    
    return logger


def get_logger(name: str):
    if not hasattr(get_logger, '_initialized'):
        setup_logger()
        get_logger._initialized = True
    
    return logger.bind(name=name)
