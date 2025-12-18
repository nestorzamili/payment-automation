import logging
import sys
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from loguru import logger

PROJECT_ROOT = Path(__file__).parent.parent.parent
KL_TZ = ZoneInfo('Asia/Kuala_Lumpur')


def get_kl_timestamp():
    return datetime.now(KL_TZ).strftime("%Y-%m-%d %H:%M:%S")


class InterceptHandler(logging.Handler):
    def emit(self, record):
        try:
            level = logger.level(record.levelname).name
        except ValueError:
            level = record.levelno

        frame, depth = sys._getframe(6), 6
        while frame and frame.f_code.co_filename == logging.__file__:
            frame = frame.f_back
            depth += 1

        logger.opt(depth=depth, exception=record.exc_info).log(level, record.getMessage())


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
    
    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)
    
    for name in ["werkzeug", "flask", "urllib3", "asyncio"]:
        logging.getLogger(name).handlers = [InterceptHandler()]
        logging.getLogger(name).propagate = False
    
    return logger


def get_logger(name: str):
    if not hasattr(get_logger, '_initialized'):
        setup_logger()
        get_logger._initialized = True
    
    return logger.bind(name=name)
