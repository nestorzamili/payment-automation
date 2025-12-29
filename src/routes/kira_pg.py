from flask import Blueprint, request

from src.services.kira_pg import KiraPGService
from src.core.logger import get_logger
from src.utils.response import jsend_success, jsend_error

logger = get_logger(__name__)

bp = Blueprint('kira_pg', __name__)


@bp.route('/kira-pg', methods=['POST'])
def update_kira_pg():
    try:
        data = request.get_json() or {}
        manual_data = data.get('manual_data', [])
        year = data.get('year')
        month = data.get('month')
        
        service = KiraPGService()
        
        if manual_data:
            logger.info(f"Saving {len(manual_data)} Kira PG manual inputs")
            service.save_manual_data(manual_data)
        
        kira_pg_data = service.get_kira_pg_data(year=year, month=month)
        
        return jsend_success({
            'message': 'Kira PG updated successfully',
            'rows': len(kira_pg_data),
            'data': kira_pg_data
        })
            
    except Exception as e:
        logger.error(f"Error updating Kira PG: {e}")
        return jsend_error(str(e))
