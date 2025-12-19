import asyncio
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

from flask import Flask, jsonify, request

from main import PaymentReconciliationPipeline
from src.core import BrowserManager, get_logger, load_accounts, load_settings, setup_logger
from src.utils import jsend_success, jsend_fail, jsend_error, job_manager

setup_logger()
logger = get_logger(__name__)

settings = load_settings()
flask_config = settings['flask']

app = Flask(__name__)
app.json.sort_keys = False

logging.getLogger('werkzeug').setLevel(logging.ERROR)

KL_TZ = ZoneInfo('Asia/Kuala_Lumpur')


def clean_error(e: Exception) -> str:
    return str(e).split('Call log:')[0].strip()


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


def run_test_job(account: dict, from_date: str, to_date: str):
    from src.core.loader import PROJECT_ROOT
    
    async def run_download():
        async with BrowserManager() as browser_manager:
            from src.scrapers import get_scraper_class
            scraper_class = get_scraper_class(account['platform'])
            scraper = scraper_class(account)
            downloaded_files = await scraper.download_data(browser_manager, from_date, to_date)
            return [str(f.relative_to(PROJECT_ROOT)) for f in downloaded_files]

    files = asyncio.run(run_download())
    return {
        'label': account['label'],
        'platform': account['platform'],
        'from_date': from_date,
        'to_date': to_date,
        'files': files,
        'file_count': len(files)
    }


PUBLIC_ENDPOINTS = ('health_check',)


@app.before_request
def check_api_key():
    if request.endpoint in PUBLIC_ENDPOINTS:
        return
    api_key = request.headers.get('X-API-Key')
    if not api_key or api_key != flask_config.get('api_key'):
        return jsend_fail('Invalid or missing API key', 401)


@app.after_request
def log_response(response):
    log_msg = f"{request.remote_addr} - {request.method} {request.path} {response.status_code}"
    
    if response.status_code >= 500:
        logger.error(log_msg)
    elif response.status_code >= 400:
        logger.warning(log_msg)
    else:
        logger.info(log_msg)
    
    return response


@app.route('/trigger', methods=['POST'])
def trigger_pipeline():
    existing = job_manager.get_running_job_by_type('trigger')
    if existing:
        return jsend_fail(f"Pipeline job already running (job: {existing['job_id']})", 409)
    
    job_id = job_manager.create_job('trigger')
    job_manager.run_in_background(job_id, run_pipeline_job)
    logger.info(f"Pipeline job queued: {job_id}")
    return jsend_success({'job_id': job_id, 'message': 'Pipeline job queued'}, 202)


@app.route('/test/<label>', methods=['POST'])
def test_account(label: str):
    accounts = load_accounts()
    account = next((a for a in accounts if a['label'] == label), None)
    
    if not account:
        return jsend_fail(f'Account not found: {label}', 404)
    
    from_date = request.args.get('from_date')
    to_date = request.args.get('to_date')
    
    if not from_date or not to_date:
        return jsend_fail('from_date and to_date query params are required', 400)
    
    job_type = f"test:{label}"
    existing = job_manager.get_running_job_by_type(job_type)
    if existing:
        return jsend_fail(f"Test job already running for {label} (job: {existing['job_id']})", 409)
    
    job_id = job_manager.create_job(
        job_type=job_type,
        account_label=label,
        from_date=from_date,
        to_date=to_date
    )
    job_manager.run_in_background(job_id, run_test_job, account, from_date, to_date)
    logger.info(f"Test job queued: {job_id} ({label})")
    return jsend_success({'job_id': job_id, 'label': label, 'message': 'Test job queued'}, 202)


@app.route('/jobs/<int:job_id>', methods=['GET'])
def get_job_status(job_id: int):
    job = job_manager.get_job(job_id)
    if not job:
        return jsend_fail(f'Job not found: {job_id}', 404)
    return jsend_success(job)


@app.route('/health', methods=['GET'])
def health_check():
    return jsend_success({
        'status': 'healthy',
        'timestamp': datetime.now(KL_TZ).strftime('%Y-%m-%d %H:%M:%S')
    })


@app.route('/parse/<label>', methods=['POST'])
def parse_m1_files(label: str):
    from pathlib import Path
    from src.core.loader import PROJECT_ROOT
    from src.processors.m1_parser import M1Parser
    
    accounts = load_accounts()
    account = next((a for a in accounts if a['label'] == label), None)
    
    if not account:
        return jsend_fail(f'Account not found: {label}', 404)
    
    if account['platform'] != 'm1':
        return jsend_fail(f'Account {label} is not an M1 account', 400)
    
    data_dir = PROJECT_ROOT / 'data' / label
    if not data_dir.exists():
        return jsend_fail(f'Data directory not found: {label}', 404)
    
    try:
        parser = M1Parser()
        result = parser.process_directory(data_dir, label)
        logger.info(f"Parsed {result['total_transactions']} transactions for {label}")
        return jsend_success(result)
    except Exception as e:
        logger.error(f"Error parsing files for {label}: {e}")
        return jsend_error(str(e), 500)


if __name__ == '__main__':
    logger.info("Starting server")
    logger.info(f"http://{flask_config['host']}:{flask_config['port']}")
    
    app.run(
        host=flask_config['host'],
        port=flask_config['port'],
        debug=flask_config['debug'],
        use_reloader=False
    )
