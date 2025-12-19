from datetime import datetime
from zoneinfo import ZoneInfo

from flask import Blueprint

from src.utils import jsend_success

bp = Blueprint('health', __name__)

KL_TZ = ZoneInfo('Asia/Kuala_Lumpur')


@bp.route('/health', methods=['GET'])
def health_check():
    return jsend_success({
        'status': 'healthy',
        'timestamp': datetime.now(KL_TZ).strftime('%Y-%m-%d %H:%M:%S')
    })
