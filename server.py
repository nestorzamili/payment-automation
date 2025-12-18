from flask import Flask, request, jsonify
import asyncio
from datetime import datetime
from zoneinfo import ZoneInfo

from src.core import get_logger, setup_logger, load_settings
from main import PaymentReconciliationPipeline

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


@app.route('/health', methods=['GET'])
def health_check():
    return jsonify({
        'status': 'healthy',
        'timestamp': datetime.now(KL_TZ).strftime('%Y-%m-%d %H:%M:%S'),
        'timezone': 'Asia/Kuala_Lumpur'
    }), 200


if __name__ == '__main__':
    logger.info("Starting Flask Trigger Server")
    logger.info(f"Host: {flask_config['host']}, Port: {flask_config['port']}")
    
    app.run(
        host=flask_config['host'],
        port=flask_config['port'],
        debug=flask_config['debug']
    )
