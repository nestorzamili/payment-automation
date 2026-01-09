from flask import Blueprint, jsonify

from src.services.account import get_all_accounts, get_account_by_id, create_account, update_account
from src.core.logger import get_logger

logger = get_logger(__name__)

bp = Blueprint('account', __name__)


@bp.route('/api/accounts', methods=['GET'])
def list_accounts():
    try:
        accounts = get_all_accounts()
        return jsonify({
            'success': True,
            'data': [acc.to_dict(include_credentials=False) for acc in accounts]
        })
    except Exception as e:
        logger.error(f"Failed to list accounts: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@bp.route('/api/accounts/<int:account_id>', methods=['GET'])
def get_account(account_id):
    try:
        account = get_account_by_id(account_id)
        if not account:
            return jsonify({'success': False, 'error': 'Account not found'}), 404
        
        return jsonify({
            'success': True,
            'data': account.to_dict(include_credentials=True)
        })
    except Exception as e:
        logger.error(f"Failed to get account: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@bp.route('/api/accounts', methods=['POST'])
def add_account():
    from flask import request
    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'error': 'No data provided'}), 400
        
        required = ['label', 'platform']
        for field in required:
            if field not in data:
                return jsonify({'success': False, 'error': f'Missing field: {field}'}), 400
        
        account = create_account(data)
        return jsonify({
            'success': True,
            'data': account.to_dict(include_credentials=False)
        }), 201
    except Exception as e:
        logger.error(f"Failed to create account: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500


@bp.route('/api/accounts/<int:account_id>', methods=['PUT'])
def edit_account(account_id):
    from flask import request
    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'error': 'No data provided'}), 400
        
        account = update_account(account_id, data)
        if not account:
            return jsonify({'success': False, 'error': 'Account not found'}), 404
        
        return jsonify({
            'success': True,
            'data': account.to_dict(include_credentials=False)
        })
    except Exception as e:
        logger.error(f"Failed to update account: {e}")
        return jsonify({'success': False, 'error': str(e)}), 500
