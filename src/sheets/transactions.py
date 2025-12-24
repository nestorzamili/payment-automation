from typing import List, Dict, Any

from src.core.database import get_session
from src.core.models import KiraTransaction, PGTransaction
from src.core.logger import get_logger

logger = get_logger(__name__)


def get_all_joined_transactions() -> List[Dict[str, Any]]:
    session = get_session()
    
    try:
        results = session.query(
            KiraTransaction,
            PGTransaction
        ).join(
            PGTransaction,
            KiraTransaction.transaction_id == PGTransaction.transaction_id
        ).all()
        
        joined_data = []
        for kira, pg in results:
            joined_data.append({
                'transaction_id': kira.transaction_id,
                'kira_amount': kira.amount,
                'kira_mdr': kira.mdr,
                'kira_settlement_amount': kira.settlement_amount,
                'kira_merchant': kira.merchant,
                'kira_payment_method': kira.payment_method,
                'kira_date': kira.transaction_date[:10],
                'pg_amount': pg.amount,
                'pg_account_label': pg.account_label,
                'account_label': pg.account_label,
                'transaction_type': pg.transaction_type,
                'channel': pg.channel,
                'platform': pg.platform
            })
        
        logger.info(f"Loaded {len(joined_data)} joined transactions")
        return joined_data
        
    finally:
        session.close()
