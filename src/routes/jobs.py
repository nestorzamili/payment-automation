import asyncio

from flask import Blueprint

from main import PaymentReconciliationPipeline
from src.core.logger import get_logger
from src.utils import jsend_success, jsend_fail, job_manager

bp = Blueprint('jobs', __name__)
logger = get_logger(__name__)


def run_pipeline_job():
    pipeline = PaymentReconciliationPipeline()
    result = asyncio.run(pipeline.run())
    if result['status'] == 'success':
        return {
            'message': 'Pipeline completed',
            'duration': result['duration_seconds'],
            'stats': {
                'download': result['download_stats'],
                'process': result['process_stats'],
                'merge': result['merge_stats']
            }
        }
    else:
        raise Exception(result['error'])


@bp.route('/trigger', methods=['POST'])
def trigger_pipeline():
    existing = job_manager.get_running_job_by_type('trigger')
    if existing:
        return jsend_fail(f"Pipeline job already running (job: {existing['job_id']})", 409)
    
    job_id = job_manager.create_job('trigger')
    job_manager.run_in_background(job_id, run_pipeline_job)
    logger.info(f"Pipeline job queued: {job_id}")
    return jsend_success({'job_id': job_id, 'message': 'Pipeline job queued'}, 202)


@bp.route('/jobs/<int:job_id>', methods=['GET'])
def get_job_status(job_id: int):
    job = job_manager.get_job(job_id)
    if not job:
        return jsend_fail(f'Job not found: {job_id}', 404)
    return jsend_success(job)
