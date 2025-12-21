from flask import Blueprint

from src.core.logger import get_logger
from src.core.jobs import job_manager
from src.utils import jsend_success, jsend_fail

bp = Blueprint('jobs', __name__)
logger = get_logger(__name__)


@bp.route('/jobs/<int:job_id>', methods=['GET'])
def get_job_status(job_id: int):
    job = job_manager.get_job(job_id)
    if not job:
        return jsend_fail(f'Job not found: {job_id}', 404)
    return jsend_success(job)
