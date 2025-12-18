"""
Bank file processor - migrated from BankProcessor.gs
"""
import pandas as pd
from pathlib import Path
from typing import Dict, Any
import re
from datetime import datetime

from src.core.logger import get_logger
from src.processors.kira import normalize_payment_method
from src.processors.payment_gateway import normalize_pg_date

logger = get_logger(__name__)


def parse_bank_filename(filename: str) -> Dict[str, Any]:
    """
    Parse Bank filename to extract metadata.
    
    Expected format: {Merchant} RHB_Axaipay_All-Transactions-{date}.xlsx
    
    Args:
        filename: Bank file name
        
    Returns:
        Dictionary with merchant, channel
    """
    meta = {
        'merchant': '',
        'channel': 'ewallet'
    }
    
    # Extract merchant name before "RHB"
    if 'RHB' in filename.upper():
        parts = filename.split('RHB')[0].strip()
        meta['merchant'] = parts.strip('_').strip()
    else:
        # Fallback: use first part of filename
        parts = filename.replace('.xlsx', '').split('_')
        meta['merchant'] = parts[0] if parts else ''
    
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
    
    logger.debug(f"Parsed Bank filename '{filename}': {meta}")
    return meta


def process_bank_file(file_path: Path) -> pd.DataFrame:
    """
    Process a Bank transaction file.
    
    Args:
        file_path: Path to the Bank .xlsx file
        
    Returns:
        DataFrame with processed Bank data
    """
    logger.info(f"Processing Bank file: {file_path}")
    
    try:
        filename = file_path.name
        meta = parse_bank_filename(filename)
        
        # Read Excel file
        df = pd.read_excel(file_path)
        
        if df.empty:
            logger.warning(f"Empty Bank file: {file_path}")
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
            logger.warning(f"No transaction ID column found in Bank file: {file_path}")
            return pd.DataFrame()
        
        # Find amount column
        amount_col = None
        for col in df.columns:
            col_lower = col.lower()
            if col_lower in ['amount', 'transactionamount', 'payment amount', 'paymentamount', 'amount (rm)', 'amount(rm)']:
                amount_col = col
                break
        
        if not amount_col:
            logger.warning(f"No amount column found in Bank file: {file_path}")
            return pd.DataFrame()
        
        # Find date column
        date_col = None
        for col in df.columns:
            col_lower = col.lower()
            if col_lower in ['payment time', 'createddate', 'date', 'transaction date', 'created date']:
                date_col = col
                break
        
        # Find payment mode column (for channel detection)
        payment_mode_col = None
        for col in df.columns:
            col_lower = col.lower()
            if col_lower in ['payment mode', 'paymentmode', 'payment method', 'paymentmethod']:
                payment_mode_col = col
                break
        
        # Create result dataframe
        result_df = pd.DataFrame()
        result_df['transaction_id'] = df[txn_id_col].astype(str).str.strip()
        result_df['bank_merchant'] = meta['merchant']
        result_df['bank_channel'] = meta['channel']
        result_df['bank_amount'] = pd.to_numeric(df[amount_col], errors='coerce').fillna(0)
        
        # Process date if available
        if date_col:
            result_df['bank_transaction_date'] = df[date_col].apply(normalize_pg_date)
        else:
            result_df['bank_transaction_date'] = ''
        
        # Override channel from payment mode if available
        if payment_mode_col:
            def detect_channel_from_payment_mode(row):
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
            
            result_df['bank_channel'] = df.apply(detect_channel_from_payment_mode, axis=1)
        
        # Remove empty transaction IDs
        result_df = result_df[result_df['transaction_id'] != '']
        
        # Remove duplicates
        initial_count = len(result_df)
        result_df = result_df.drop_duplicates(subset=['transaction_id'], keep='first')
        duplicates_removed = initial_count - len(result_df)
        
        if duplicates_removed > 0:
            logger.warning(f"Removed {duplicates_removed} duplicate transaction IDs from Bank file")
        
        logger.info(f"Processed Bank file: {len(result_df)} records from {meta['merchant']}")
        return result_df
        
    except Exception as e:
        logger.error(f"Error processing Bank file {file_path}: {e}")
        raise


def process_bank_folder(folder_path: Path) -> pd.DataFrame:
    """
    Process all Bank files in a folder.
    
    Args:
        folder_path: Path to folder containing Bank .xlsx files
        
    Returns:
        DataFrame with all Bank data merged
    """
    logger.info(f"Processing Bank folder: {folder_path}")
    
    all_data = []
    
    if not folder_path.exists():
        logger.warning(f"Bank folder does not exist: {folder_path}")
        return pd.DataFrame()
    
    # Find all .xlsx files
    xlsx_files = list(folder_path.glob('**/*.xlsx'))
    
    if not xlsx_files:
        logger.warning(f"No .xlsx files found in Bank folder: {folder_path}")
        return pd.DataFrame()
    
    logger.info(f"Found {len(xlsx_files)} Bank files")
    
    for file_path in xlsx_files:
        try:
            df = process_bank_file(file_path)
            if not df.empty:
                all_data.append(df)
        except Exception as e:
            logger.error(f"Failed to process Bank file {file_path}: {e}")
            continue
    
    if not all_data:
        logger.warning("No data extracted from Bank folder")
        return pd.DataFrame()
    
    # Merge all dataframes
    merged_df = pd.concat(all_data, ignore_index=True)
    
    # Remove duplicates across all files
    initial_count = len(merged_df)
    merged_df = merged_df.drop_duplicates(subset=['transaction_id'], keep='first')
    duplicates_removed = initial_count - len(merged_df)
    
    if duplicates_removed > 0:
        logger.warning(f"Removed {duplicates_removed} duplicate transaction IDs across all Bank files")
    
    logger.info(f"Total Bank records: {len(merged_df)}")
    return merged_df
