from flask import jsonify


def jsend_success(data: dict):
    return jsonify({'status': 'success', 'data': data}), 200


def jsend_fail(data: dict, http_code: int = 400):
    return jsonify({'status': 'fail', 'data': data}), http_code


def jsend_error(message: str, http_code: int = 500, code: str = None, data: dict = None):
    response = {'status': 'error', 'message': message}
    if code:
        response['code'] = code
    if data:
        response['data'] = data
    return jsonify(response), http_code
