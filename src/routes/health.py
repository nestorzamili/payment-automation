from datetime import datetime

from flask import Blueprint

from src.core.loader import get_timezone
from src.utils import jsend_success

bp = Blueprint('health', __name__)


@bp.route('/health', methods=['GET'])
def health_check():
    return jsend_success({
        'status': 'healthy',
        'timestamp': datetime.now(get_timezone()).strftime('%Y-%m-%d %H:%M:%S')
    })
