import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from src.core.logger import get_logger
from src.processors.kira import normalize_payment_method

logger = get_logger(__name__)


def parse_pg_filename(filename: str) -> Dict[str, Any]:
    meta = {
        'merchant': '',
        'pg': '',
        'channel': 'ewallet',
        'is_rhb': False
    }
    
    if 'RHB' in filename.upper():
        meta['is_rhb'] = True
        meta['pg'] = 'RHB'
        parts = filename.split('RHB')[0].strip()
        meta['merchant'] = parts.strip('_').strip()
        
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
    else:
        parts = filename.replace('.xlsx', '').split('_')
        if len(parts) >= 3:
            meta['merchant'] = parts[0]
            meta['pg'] = parts[1]
            meta['channel'] = normalize_payment_method(parts[2])
        elif len(parts) >= 2:
            meta['merchant'] = parts[0]
            meta['pg'] = parts[1]
    
    return meta


def normalize_pg_date(date_value: Any) -> str:
    if pd.isna(date_value) or not date_value:
        return ''
    
    try:
        if isinstance(date_value, str):
            for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%Y/%m/%d', '%d-%m-%Y']:
                try:
                    dt = datetime.strptime(date_value, fmt)
                    return dt.strftime('%Y-%m-%d')
                except ValueError:
                    continue
        elif isinstance(date_value, (datetime, pd.Timestamp)):
            return date_value.strftime('%Y-%m-%d')
    except Exception:
        pass
    
    return ''


def process_pg_file(file_path: Path) -> pd.DataFrame:
    logger.info(f"Processing PG: {file_path.name}")
    
    try:
        filename = file_path.name
        meta = parse_pg_filename(filename)
        
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
        if meta['is_rhb']:
            for col in df.columns:
                if 'Amount (RM)' in col or 'amount(rm)' in col.lower():
                    amount_col = col
                    break
        
        if not amount_col:
            for col in df.columns:
                col_lower = col.lower()
                if col_lower in ['amount', 'transactionamount', 'payment amount', 'paymentamount', 'amount (rm)']:
                    amount_col = col
                    break
        
        if not amount_col:
            logger.warning(f"No amount column: {file_path.name}")
            return pd.DataFrame()
        
        date_col = None
        for col in df.columns:
            col_lower = col.lower()
            if col_lower in ['payment time', 'createddate', 'date', 'transaction date']:
                date_col = col
                break
        
        payment_mode_col = None
        for col in df.columns:
            col_lower = col.lower()
            if col_lower in ['payment mode', 'paymentmode']:
                payment_mode_col = col
                break
        
        result_df = pd.DataFrame()
        result_df['transaction_id'] = df[txn_id_col].astype(str).str.strip()
        result_df['pg_merchant'] = meta['merchant']
        result_df['pg_channel'] = meta['channel']
        result_df['pg_amount'] = pd.to_numeric(df[amount_col], errors='coerce').fillna(0)
        
        if date_col:
            result_df['pg_transaction_date'] = df[date_col].apply(normalize_pg_date)
        else:
            result_df['pg_transaction_date'] = ''
        
        if payment_mode_col:
            def detect_channel(row):
                mode = str(row[payment_mode_col]).lower() if payment_mode_col in df.columns else ''
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
            
            result_df['pg_channel'] = df.apply(detect_channel, axis=1)
        
        result_df = result_df[result_df['transaction_id'] != '']
        result_df = result_df.drop_duplicates(subset=['transaction_id'], keep='first')
        
        logger.info(f"PG processed: {len(result_df)} records from {meta['merchant']}")
        return result_df
        
    except Exception as e:
        logger.error(f"Error processing {file_path.name}: {e}")
        raise


def process_pg_folder(folder_path: Path) -> pd.DataFrame:
    logger.info(f"Processing PG folder: {folder_path}")
    
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
            df = process_pg_file(file_path)
            if not df.empty:
                all_data.append(df)
        except Exception as e:
            logger.error(f"Failed: {file_path.name} - {e}")
            continue
    
    if not all_data:
        return pd.DataFrame()
    
    merged_df = pd.concat(all_data, ignore_index=True)
    merged_df = merged_df.drop_duplicates(subset=['transaction_id'], keep='first')
    
    logger.info(f"Total PG records: {len(merged_df)}")
    return merged_df
