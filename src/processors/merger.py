import pandas as pd
from typing import Tuple

from src.core.logger import get_logger

logger = get_logger(__name__)


def merge_data(kira_df: pd.DataFrame, pg_df: pd.DataFrame, bank_df: pd.DataFrame) -> Tuple[pd.DataFrame, dict]:
    logger.info("Merging KIRA, PG, and Bank data")
    
    merged = kira_df.merge(pg_df, on='transaction_id', how='outer', indicator='_merge_pg')
    merged = merged.merge(bank_df, on='transaction_id', how='outer', indicator='_merge_bank')
    
    merged['has_kira'] = merged['_merge_pg'].isin(['both', 'left_only'])
    merged['has_pg'] = (merged['_merge_pg'] == 'both') | (merged['_merge_bank'] == 'right_only')
    merged['has_bank'] = merged['_merge_bank'].isin(['both', 'right_only'])
    
    merged['transaction_amount'] = merged['transaction_amount'].fillna(0)
    merged['pg_amount'] = merged['pg_amount'].fillna(0)
    merged['bank_amount'] = merged['bank_amount'].fillna(0)
    
    def calculate_remarks(row):
        has_kira = row['has_kira']
        has_pg = row['has_pg']
        has_bank = row['has_bank']
        
        kira_amt = row['transaction_amount']
        pg_amt = row['pg_amount']
        bank_amt = row['bank_amount']
        
        if has_kira:
            pg_match = has_pg and kira_amt == pg_amt
            bank_match = has_bank and kira_amt == bank_amt
            
            if has_pg and has_bank:
                if pg_match and bank_match:
                    return 'Match'
                elif not pg_match and not bank_match:
                    return 'Not Match (PG & Bank)'
                elif not pg_match:
                    return 'Not Match (PG)'
                else:
                    return 'Not Match (Bank)'
            elif has_pg:
                return 'Match (PG only)' if pg_match else 'Not Match (PG)'
            elif has_bank:
                return 'Match (Bank only)' if bank_match else 'Not Match (Bank)'
            else:
                return 'No Data (PG & Bank)'
        else:
            if has_pg and has_bank:
                return 'No Kira Data'
            elif has_pg:
                return 'No Kira Data (PG only)'
            elif has_bank:
                return 'No Kira Data (Bank only)'
            else:
                return 'Unknown'
    
    merged['remarks'] = merged.apply(calculate_remarks, axis=1)
    
    result = pd.DataFrame()
    
    result['Created On'] = merged['created_on'].where(merged['has_kira'], 'No Data')
    result['Merchant'] = merged['merchant'].where(merged['has_kira'], 'No Data')
    result['Transaction ID'] = merged['transaction_id']
    result['Merchant Order ID'] = merged['merchant_order_id'].where(merged['has_kira'], 'No Data')
    result['Payment Method'] = merged['payment_method'].where(merged['has_kira'], 'No Data')
    result['Kira Amount'] = merged['transaction_amount'].where(merged['has_kira'], 'No Data')
    
    result['PG Merchant'] = merged['pg_merchant'].where(merged['has_pg'], 'No Data')
    result['PG Channel'] = merged['pg_channel'].where(merged['has_pg'], 'No Data')
    result['PG Transaction Date'] = merged['pg_transaction_date'].where(merged['has_pg'], 'No Data')
    result['Amount PG'] = merged['pg_amount'].where(merged['has_pg'], 'No Data')
    
    result['Bank Merchant'] = merged['bank_merchant'].where(merged['has_bank'], 'No Data')
    result['Bank Channel'] = merged['bank_channel'].where(merged['has_bank'], 'No Data')
    result['Bank Transaction Date'] = merged['bank_transaction_date'].where(merged['has_bank'], 'No Data')
    result['Amount RHB'] = merged['bank_amount'].where(merged['has_bank'], 'No Data')
    
    result['Remarks'] = merged['remarks']
    
    stats = {
        'total_records': len(result),
        'kira_records': merged['has_kira'].sum(),
        'pg_records': merged['has_pg'].sum(),
        'bank_records': merged['has_bank'].sum(),
        'matched': (result['Remarks'] == 'Match').sum(),
        'mismatch_pg': result['Remarks'].str.contains('Not Match.*PG', regex=True).sum(),
        'mismatch_bank': result['Remarks'].str.contains('Not Match.*Bank', regex=True).sum(),
    }
    
    logger.info(f"Merge completed: {stats['total_records']} records, {stats['matched']} matched")
    return result, stats
