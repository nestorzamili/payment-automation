import uuid
from threading import Thread

from flask import Blueprint

from src.core.logger import get_logger
from src.services.parser import run_parse_job
from src.utils import jsend_success

bp = Blueprint('parse', __name__)
logger = get_logger(__name__)

_parse_running = False


@bp.route('/parse', methods=['POST'])
def parse_all():
    global _parse_running
    
    if _parse_running:
        return jsend_success({'status': 'running', 'message': 'Parse job already running'}, 200)
    
    _parse_running = True
    run_id = str(uuid.uuid4())
    
    def job_wrapper(rid):
        global _parse_running
        try:
            run_parse_job(rid)
        except Exception as e:
            logger.error(f"Parse job failed: {e}")
        finally:
            _parse_running = False
    
    thread = Thread(target=job_wrapper, args=(run_id,), daemon=True)
    thread.start()
    
    logger.info(f"Parse job started (run_id: {run_id})")
    return jsend_success({'run_id': run_id, 'message': 'Parse job started'}, 202)
