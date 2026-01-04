import logging
import sys
from datetime import datetime
from pathlib import Path

from loguru import logger

PROJECT_ROOT = Path(__file__).parent.parent.parent


def get_kl_timestamp():
    from src.core.loader import get_timezone
    return datetime.now(get_timezone()).strftime("%Y-%m-%d %H:%M:%S")


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
    if hasattr(setup_logger, '_initialized'):
        return logger
    setup_logger._initialized = True
    
    logger.remove()
    
    logger.add(
        sys.stderr,
        format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level}</level> | <level>{message}</level>",
        level="INFO",
        colorize=True
    )
    from src.core.loader import get_timezone, load_settings
    settings = load_settings()
    log_dir = PROJECT_ROOT / settings['logging']['directory']
    log_dir.mkdir(exist_ok=True)
    
    kl_date = datetime.now(get_timezone()).strftime("%Y-%m-%d")
    log_file = log_dir / f"{kl_date}.log"
    
    logger.add(
        log_file,
        format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}",
        level="INFO",
        encoding="utf-8"
    )
    
    logging.basicConfig(handlers=[InterceptHandler()], level=0, force=True)
    
    for name in ["werkzeug", "flask", "urllib3", "asyncio"]:
        logging.getLogger(name).handlers = [InterceptHandler()]
        logging.getLogger(name).propagate = False
    
    return logger


def get_logger(name: str):
    setup_logger()
    return logger.bind(name=name)


def clean_error_msg(error: Exception) -> str:
    """Clean Playwright error message by removing Call log and separator lines."""
    msg = str(error).split('Call log:')[0].strip()
    lines = msg.split('\n')
    cleaned_lines = [line for line in lines if not line.strip().replace('=', '').replace(' ', '') == '']
    return '\n'.join(cleaned_lines).strip()
