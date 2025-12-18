import asyncio
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from flask import Flask, jsonify, request

from main import PaymentReconciliationPipeline
from src.core import BrowserManager, get_logger, load_accounts, load_settings, setup_logger
from src.utils import jsend_success, jsend_fail, jsend_error

setup_logger()
logger = get_logger(__name__)

settings = load_settings()
flask_config = settings['flask']

app = Flask(__name__)

logging.getLogger('werkzeug').setLevel(logging.ERROR)

KL_TZ = ZoneInfo('Asia/Kuala_Lumpur')


def clean_error(e: Exception) -> str:
    return str(e).split('Call log:')[0].strip()


@app.after_request
def log_response(response):
    logger.info(f"{request.remote_addr} - {request.method} {request.path} {response.status_code}")
    return response


@app.route('/trigger', methods=['POST'])
def trigger_pipeline():
    try:
        pipeline = PaymentReconciliationPipeline()
        result = asyncio.run(pipeline.run())
        
        if result['status'] == 'success':
            return jsend_success({
                'message': 'Pipeline completed',
                'duration': result['duration_seconds'],
                'stats': {
                    'download': result['download_stats'],
                    'process': result['process_stats'],
                    'merge': result['merge_stats']
                }
            })
        else:
            return jsend_error(result['error'])
            
    except Exception as e:
        error_msg = clean_error(e)
        logger.error(f"Pipeline error: {error_msg}")
        return jsend_error(error_msg)


@app.route('/test/<label>', methods=['POST'])
def test_account(label: str):
    accounts = load_accounts()
    account = next((a for a in accounts if a['label'] == label), None)
    
    if not account:
        return jsend_fail({'message': f"Account not found: {label}"}, 404)
    
    from_date = request.args.get('from_date')
    to_date = request.args.get('to_date')
    
    if not from_date or not to_date:
        return jsend_fail({'message': 'from_date and to_date query params are required'}, 400)
    
    try:
        async def run_download():
            async with BrowserManager() as browser_manager:
                from src.scrapers import get_scraper_class
                scraper_class = get_scraper_class(account['platform'])
                scraper = scraper_class(account)
                downloaded_files = await scraper.download_data(browser_manager, from_date, to_date)
                return [str(f) for f in downloaded_files]
        
        files = asyncio.run(run_download())
        
        return jsend_success({
            'label': label,
            'platform': account['platform'],
            'from_date': from_date,
            'to_date': to_date,
            'files': files,
            'file_count': len(files)
        })
        
    except Exception as e:
        error_msg = clean_error(e)
        logger.error(f"Test failed: {label} - {error_msg}")
        return jsend_error(error_msg, 502)


@app.route('/accounts', methods=['GET'])
def list_accounts():
    accounts = load_accounts()
    return jsend_success({
        'accounts': [
            {'label': a['label'], 'platform': a['platform'], 'need_captcha': a.get('need_captcha', False)}
            for a in accounts
        ]
    })


@app.route('/health', methods=['GET'])
def health_check():
    return jsend_success({
        'status': 'healthy',
        'timestamp': datetime.now(KL_TZ).strftime('%Y-%m-%d %H:%M:%S')
    })


if __name__ == '__main__':
    logger.info("Starting server")
    logger.info(f"http://{flask_config['host']}:{flask_config['port']}")
    
    app.run(
        host=flask_config['host'],
        port=flask_config['port'],
        debug=flask_config['debug'],
        use_reloader=False
    )

