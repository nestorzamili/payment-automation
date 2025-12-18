"""
PG (Payment Gateway) file processor - migrated from PGProcessor.gs
"""
import pandas as pd
from pathlib import Path
from typing import Dict, Any, Tuple
import re
from datetime import datetime

from src.core.logger import get_logger
from src.processors.kira import normalize_payment_method

logger = get_logger(__name__)


def parse_pg_filename(filename: str) -> Dict[str, Any]:
    """
    Parse PG filename to extract metadata.
    
    Expected formats:
    - {Merchant}_{PG}_{channel}_{details}.xlsx
    - {Merchant} RHB_{details}.xlsx
    
    Args:
        filename: PG file name
        
    Returns:
        Dictionary with merchant, pg, channel, is_rhb
    """
    meta = {
        'merchant': '',
        'pg': '',
        'channel': 'ewallet',
        'is_rhb': False
    }
    
    # Check if RHB file
    if 'RHB' in filename.upper():
        meta['is_rhb'] = True
        meta['pg'] = 'RHB'
        
        # Extract merchant name before "RHB"
        parts = filename.split('RHB')[0].strip()
        meta['merchant'] = parts.strip('_').strip()
        
        # Try to detect channel from filename
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
        # Parse Ragnarok/M1pay format: Merchant_PG_channel_details.xlsx
        parts = filename.replace('.xlsx', '').split('_')
        
        if len(parts) >= 3:
            meta['merchant'] = parts[0]
            meta['pg'] = parts[1]
            channel_str = parts[2] if len(parts) > 2 else 'ewallet'
            meta['channel'] = normalize_payment_method(channel_str)
        elif len(parts) >= 2:
            meta['merchant'] = parts[0]
            meta['pg'] = parts[1]
    
    logger.debug(f"Parsed PG filename '{filename}': {meta}")
    return meta


def normalize_pg_date(date_value: Any) -> str:
    """
    Normalize PG date to YYYY-MM-DD format.
    
    Args:
        date_value: Date value (string, datetime, or timestamp)
        
    Returns:
        Date string in YYYY-MM-DD format
    """
    if pd.isna(date_value) or not date_value:
        return ''
    
    try:
        if isinstance(date_value, str):
            # Try parsing common formats
            for fmt in ['%Y-%m-%d', '%d/%m/%Y', '%m/%d/%Y', '%Y/%m/%d', '%d-%m-%Y']:
                try:
                    dt = datetime.strptime(date_value, fmt)
                    return dt.strftime('%Y-%m-%d')
                except ValueError:
                    continue
        elif isinstance(date_value, datetime):
            return date_value.strftime('%Y-%m-%d')
        elif isinstance(date_value, pd.Timestamp):
            return date_value.strftime('%Y-%m-%d')
    except Exception as e:
        logger.warning(f"Failed to normalize PG date '{date_value}': {e}")
    
    return ''


def process_pg_file(file_path: Path) -> pd.DataFrame:
    """
    Process a PG transaction file.
    
    Args:
        file_path: Path to the PG .xlsx file
        
    Returns:
        DataFrame with processed PG data
    """
    logger.info(f"Processing PG file: {file_path}")
    
    try:
        filename = file_path.name
        meta = parse_pg_filename(filename)
        
        # Read Excel file
        df = pd.read_excel(file_path)
        
        if df.empty:
            logger.warning(f"Empty PG file: {file_path}")
            return pd.DataFrame()
        
        # Normalize column names
        df.columns = df.columns.str.strip()
        
        # Find transaction ID column
        txn_id_col = None
        for col in df.columns:
            col_lower = col.lower()
            if col_lower in ['order id', 'orderid', 'merchantorderno', 'transactionid', 'order number', 'ordernumber', 'order_no', 'order no']:
                txn_id_col = col
                break
        
        if not txn_id_col:
            logger.warning(f"No transaction ID column found in PG file: {file_path}")
            return pd.DataFrame()
        
        # Find amount column
        amount_col = None
        if meta['is_rhb']:
            # RHB prefers "Amount (RM)"
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
            logger.warning(f"No amount column found in PG file: {file_path}")
            return pd.DataFrame()
        
        # Find date column
        date_col = None
        for col in df.columns:
            col_lower = col.lower()
            if col_lower in ['payment time', 'createddate', 'date', 'transaction date']:
                date_col = col
                break
        
        # Find payment mode column (for channel detection)
        payment_mode_col = None
        for col in df.columns:
            col_lower = col.lower()
            if col_lower in ['payment mode', 'paymentmode']:
                payment_mode_col = col
                break
        
        # Create result dataframe
        result_df = pd.DataFrame()
        result_df['transaction_id'] = df[txn_id_col].astype(str).str.strip()
        result_df['pg_merchant'] = meta['merchant']
        result_df['pg_channel'] = meta['channel']
        result_df['pg_amount'] = pd.to_numeric(df[amount_col], errors='coerce').fillna(0)
        
        # Process date if available
        if date_col:
            result_df['pg_transaction_date'] = df[date_col].apply(normalize_pg_date)
        else:
            result_df['pg_transaction_date'] = ''
        
        # Override channel from payment mode if available
        if payment_mode_col:
            def detect_channel_from_payment_mode(row):
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
            
            result_df['pg_channel'] = df.apply(detect_channel_from_payment_mode, axis=1)
        
        # Remove empty transaction IDs
        result_df = result_df[result_df['transaction_id'] != '']
        
        # Remove duplicates
        initial_count = len(result_df)
        result_df = result_df.drop_duplicates(subset=['transaction_id'], keep='first')
        duplicates_removed = initial_count - len(result_df)
        
        if duplicates_removed > 0:
            logger.warning(f"Removed {duplicates_removed} duplicate transaction IDs from PG file")
        
        logger.info(f"Processed PG file: {len(result_df)} records from {meta['merchant']}/{meta['pg']}")
        return result_df
        
    except Exception as e:
        logger.error(f"Error processing PG file {file_path}: {e}")
        raise


def process_pg_folder(folder_path: Path) -> pd.DataFrame:
    """
    Process all PG files in a folder.
    
    Args:
        folder_path: Path to folder containing PG .xlsx files
        
    Returns:
        DataFrame with all PG data merged
    """
    logger.info(f"Processing PG folder: {folder_path}")
    
    all_data = []
    
    if not folder_path.exists():
        logger.warning(f"PG folder does not exist: {folder_path}")
        return pd.DataFrame()
    
    # Find all .xlsx files
    xlsx_files = list(folder_path.glob('**/*.xlsx'))
    
    if not xlsx_files:
        logger.warning(f"No .xlsx files found in PG folder: {folder_path}")
        return pd.DataFrame()
    
    logger.info(f"Found {len(xlsx_files)} PG files")
    
    for file_path in xlsx_files:
        try:
            df = process_pg_file(file_path)
            if not df.empty:
                all_data.append(df)
        except Exception as e:
            logger.error(f"Failed to process PG file {file_path}: {e}")
            continue
    
    if not all_data:
        logger.warning("No data extracted from PG folder")
        return pd.DataFrame()
    
    # Merge all dataframes
    merged_df = pd.concat(all_data, ignore_index=True)
    
    # Remove duplicates across all files
    initial_count = len(merged_df)
    merged_df = merged_df.drop_duplicates(subset=['transaction_id'], keep='first')
    duplicates_removed = initial_count - len(merged_df)
    
    if duplicates_removed > 0:
        logger.warning(f"Removed {duplicates_removed} duplicate transaction IDs across all PG files")
    
    logger.info(f"Total PG records: {len(merged_df)}")
    return merged_df
