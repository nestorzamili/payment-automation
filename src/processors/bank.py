import pandas as pd
from pathlib import Path
from typing import Any, Dict

from src.core.logger import get_logger
from src.processors.payment_gateway import normalize_pg_date

logger = get_logger(__name__)


def parse_bank_filename(filename: str) -> Dict[str, Any]:
    meta = {
        'merchant': '',
        'channel': 'ewallet'
    }
    
    if 'RHB' in filename.upper():
        parts = filename.split('RHB')[0].strip()
        meta['merchant'] = parts.strip('_').strip()
    else:
        parts = filename.replace('.xlsx', '').split('_')
        meta['merchant'] = parts[0] if parts else ''
    
    filename_upper = filename.upper()
    if 'FPX' in filename_upper:
        if 'B2B' in filename_upper or 'CORP' in filename_upper:
            meta['channel'] = 'FPXC'
        else:
            meta['channel'] = 'FPX'
    elif 'TNG' in filename_upper:
        meta['channel'] = 'TNG'
    elif 'BOOST' in filename_upper:
        meta['channel'] = 'BOOST'
    elif 'SHOPEE' in filename_upper:
        meta['channel'] = 'Shopee'
    
    return meta


def process_bank_file(file_path: Path) -> pd.DataFrame:
    logger.info(f"Processing Bank: {file_path.name}")
    
    try:
        filename = file_path.name
        meta = parse_bank_filename(filename)
        
        df = pd.read_excel(file_path)
        
        if df.empty:
            logger.warning(f"Empty file: {file_path.name}")
            return pd.DataFrame()
        
        df.columns = df.columns.str.strip()
        
        txn_id_col = None
        for col in df.columns:
            col_lower = col.lower()
            if col_lower in ['order id', 'orderid', 'merchantorderno', 'transactionid', 'order number', 'ordernumber', 'order_no', 'order no']:
                txn_id_col = col
                break
        
        if not txn_id_col:
            logger.warning(f"No transaction ID column: {file_path.name}")
            return pd.DataFrame()
        
        amount_col = None
        for col in df.columns:
            col_lower = col.lower()
            if col_lower in ['amount', 'transactionamount', 'payment amount', 'paymentamount', 'amount (rm)', 'amount(rm)']:
                amount_col = col
                break
        
        if not amount_col:
            logger.warning(f"No amount column: {file_path.name}")
            return pd.DataFrame()
        
        date_col = None
        for col in df.columns:
            col_lower = col.lower()
            if col_lower in ['payment time', 'createddate', 'date', 'transaction date', 'created date']:
                date_col = col
                break
        
        payment_mode_col = None
        for col in df.columns:
            col_lower = col.lower()
            if col_lower in ['payment mode', 'paymentmode', 'payment method', 'paymentmethod']:
                payment_mode_col = col
                break
        
        result_df = pd.DataFrame()
        result_df['transaction_id'] = df[txn_id_col].astype(str).str.strip()
        result_df['bank_merchant'] = meta['merchant']
        result_df['bank_channel'] = meta['channel']
        result_df['bank_amount'] = pd.to_numeric(df[amount_col], errors='coerce').fillna(0)
        
        if date_col:
            result_df['bank_transaction_date'] = df[date_col].apply(normalize_pg_date)
        else:
            result_df['bank_transaction_date'] = ''
        
        if payment_mode_col:
            def detect_channel(row):
                mode = str(row[payment_mode_col]).lower() if pd.notna(row[payment_mode_col]) else ''
                if 'fpx b2c' in mode or 'fpx casa' in mode:
                    return 'FPX'
                elif 'fpx b2b' in mode or 'fpxc' in mode:
                    return 'FPXC'
                elif 'tng' in mode or 'touch' in mode:
                    return 'TNG'
                elif 'boost' in mode:
                    return 'BOOST'
                elif 'shopee' in mode:
                    return 'Shopee'
                return meta['channel']
            
            result_df['bank_channel'] = df.apply(detect_channel, axis=1)
        
        result_df = result_df[result_df['transaction_id'] != '']
        result_df = result_df.drop_duplicates(subset=['transaction_id'], keep='first')
        
        logger.info(f"Bank processed: {len(result_df)} records from {meta['merchant']}")
        return result_df
        
    except Exception as e:
        logger.error(f"Error processing {file_path.name}: {e}")
        raise


def process_bank_folder(folder_path: Path) -> pd.DataFrame:
    logger.info(f"Processing Bank folder: {folder_path}")
    
    if not folder_path.exists():
        logger.warning(f"Folder not found: {folder_path}")
        return pd.DataFrame()
    
    xlsx_files = list(folder_path.glob('**/*.xlsx'))
    
    if not xlsx_files:
        logger.warning(f"No files found: {folder_path}")
        return pd.DataFrame()
    
    all_data = []
    for file_path in xlsx_files:
        try:
            df = process_bank_file(file_path)
            if not df.empty:
                all_data.append(df)
        except Exception as e:
            logger.error(f"Failed: {file_path.name} - {e}")
            continue
    
    if not all_data:
        return pd.DataFrame()
    
    merged_df = pd.concat(all_data, ignore_index=True)
    merged_df = merged_df.drop_duplicates(subset=['transaction_id'], keep='first')
    
    logger.info(f"Total Bank records: {len(merged_df)}")
    return merged_df
