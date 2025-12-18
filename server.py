import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

from flask import Flask, jsonify, request

from main import PaymentReconciliationPipeline
from src.core import BrowserManager, get_logger, load_accounts, load_settings, setup_logger
from src.scrapers import get_scraper_class

setup_logger()
logger = get_logger(__name__)

settings = load_settings()
flask_config = settings['flask']

app = Flask(__name__)

KL_TZ = ZoneInfo('Asia/Kuala_Lumpur')


def clean_error(e: Exception) -> str:
    return str(e).split('Call log:')[0].strip()


@app.route('/trigger', methods=['POST'])
def trigger_pipeline():
    logger.info(f"Received trigger request at {datetime.now(KL_TZ).strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        pipeline = PaymentReconciliationPipeline()
        result = asyncio.run(pipeline.run())
        
        if result['status'] == 'success':
            response = {
                'status': 'success',
                'message': 'Pipeline completed successfully',
                'duration': result['duration_seconds'],
                'stats': {
                    'download': result['download_stats'],
                    'process': result['process_stats'],
                    'merge': result['merge_stats']
                }
            }
            logger.info("Pipeline completed successfully via Flask trigger")
            return jsonify(response), 200
        else:
            response = {
                'status': 'error',
                'message': f"Pipeline failed: {result['error']}",
                'duration': result['duration_seconds']
            }
            logger.error(f"Pipeline failed via Flask trigger: {result['error']}")
            return jsonify(response), 500
            
    except Exception as e:
        error_msg = clean_error(e)
        logger.error(f"Unexpected error in Flask trigger: {error_msg}")
        return jsonify({
            'status': 'error',
            'message': f"Unexpected error: {error_msg}"
        }), 500


@app.route('/test/<label>', methods=['POST'])
def test_account(label: str):
    logger.info(f"Test download for account: {label}")
    
    accounts = load_accounts()
    account = next((a for a in accounts if a['label'] == label), None)
    
    if not account:
        return jsonify({
            'status': 'error',
            'message': f"Account not found: {label}"
        }), 404
    
    try:
        async def run_download():
            async with BrowserManager() as browser_manager:
                scraper_class = get_scraper_class(account['platform'])
                scraper = scraper_class(account)
                downloaded_files = await scraper.download_data(browser_manager)
                return [str(f) for f in downloaded_files]
        
        files = asyncio.run(run_download())
        
        return jsonify({
            'status': 'success',
            'label': label,
            'platform': account['platform'],
            'files': files,
            'file_count': len(files)
        }), 200
        
    except Exception as e:
        error_msg = clean_error(e)
        logger.error(f"Test failed for {label}: {error_msg}")
        return jsonify({
            'status': 'error',
            'label': label,
            'message': error_msg
        }), 500


@app.route('/accounts', methods=['GET'])
def list_accounts():
    accounts = load_accounts()
    return jsonify({
        'accounts': [
            {
                'label': a['label'],
                'platform': a['platform'],
                'need_captcha': a.get('need_captcha', False)
            }
            for a in accounts
        ]
    }), 200


@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now(KL_TZ).strftime('%Y-%m-%d %H:%M:%S'),
        'timezone': 'Asia/Kuala_Lumpur'
    }), 200


if __name__ == '__main__':
    logger.info("Starting payment reconciliation automation server")
    logger.info(f"{flask_config['host']}:{flask_config['port']}")
    
    app.run(
        host=flask_config['host'],
        port=flask_config['port'],
        debug=flask_config['debug']
    )
