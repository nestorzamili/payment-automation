from flask import Blueprint

from src.core.logger import get_logger
from src.core.jobs import job_manager
from src.utils import jsend_success, jsend_fail

bp = Blueprint('jobs', __name__)
logger = get_logger(__name__)


@bp.route('/jobs/<run_id>', methods=['GET'])
def get_jobs_by_run(run_id: str):
    result = job_manager.get_jobs_by_run_id(run_id)
    if not result:
        return jsend_fail(f'No jobs found for run_id: {run_id}', 404)
    return jsend_success(result)

