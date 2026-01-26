from flask import Blueprint

from src.core.logger import get_logger
from src.services.sync import (
    start_full_sync, 
    start_platform_sync,
    start_parse_only,
    is_sync_running, 
    get_current_run_id
)
from src.utils import jsend_success, jsend_fail

bp = Blueprint('sync', __name__, url_prefix='/api/sync')
logger = get_logger(__name__)


@bp.route('', methods=['POST'])
def sync_data():
    result = start_full_sync()
    
    if result['status'] == 'already_running':
        return jsend_fail(result['message'], 409)
    
    return jsend_success(result, 202)


@bp.route('/<platform>', methods=['POST'])
def sync_platform(platform: str):
    valid_platforms = ['kira', 'pg', 'fiuu', 'm1', 'axai']
    if platform not in valid_platforms:
        return jsend_fail(f'Invalid platform: {platform}. Valid: {valid_platforms}', 400)
    
    result = start_platform_sync(platform)
    
    if result['status'] == 'already_running':
        return jsend_fail(result['message'], 409)
    
    return jsend_success(result, 202)


@bp.route('/parse', methods=['POST'])
def parse_only():
    result = start_parse_only()
    
    if result['status'] == 'already_running':
        return jsend_fail(result['message'], 409)
    
    return jsend_success(result, 202)


@bp.route('/status', methods=['GET'])
def sync_status():
    return jsend_success({
        'running': is_sync_running(),
        'run_id': get_current_run_id()
    })
